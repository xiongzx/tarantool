/*
 * Copyright 2010-2018, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "swim.h"
#include "sio.h"
#include "uri.h"
#include "assoc.h"
#include "fiber.h"
#include "small/rlist.h"
#include "msgpuck.h"
#include "info.h"
#include <arpa/inet.h>

/**
 * Possible optimizations:
 *
 * Mandatory:
 * - split swim_member_addr_hash into port and ip.
 *
 * Optional:
 * - do not send self.
 * - cache encoded batch.
 * - refute immediately.
 * - indirect ping.
 * - increment own incarnation on each round.
 * - attach dst incarnation to ping.
 */

/**
 * SWIM - Scalable Weakly-consistent Infection-style Process Group
 * Membership Protocol. It consists of 2 components: events
 * dissemination and failure detection, and stores in memory a
 * table of known remote hosts - members. Also some SWIM
 * implementations have additional component: anti-entropy -
 * periodical broadcast of a random subset of members table.
 *
 * Each SWIM component is different from others in both message
 * structures and goals, they even could be sent in different
 * messages. But SWIM describes piggybacking of messages: a ping
 * message can piggyback a dissemination's one. SWIM has a main
 * operating cycle during which it randomly chooses members from a
 * member table and sends them events + ping. Answers are
 * processed out of the main cycle asynchronously.
 *
 * Random selection provides even network load about ~1 message to
 * each member regardless of the cluster size. Without randomness
 * a member would get a network load of N messages each protocol
 * step, since all other members will choose the same member on
 * each step where N is the cluster size.
 *
 * Also SWIM describes a kind of fairness: when selecting a next
 * member to ping, the protocol prefers LRU members. In code it
 * would too complicated, so Tarantool's implementation is
 * slightly different, easier.
 *
 * Tarantool splits protocol operation into rounds. At the
 * beginning of a round all members are randomly reordered and
 * linked into a list. At each round step a member is popped from
 * the list head, a message is sent to him, and he waits for the
 * next round. In such implementation all random selection of the
 * original SWIM is executed once per round. The round is
 * 'planned' actually. A list is used instead of an array since
 * new members can be added to its tail without realloc, and dead
 * members can be removed as easy as that.
 *
 * Also Tarantool implements third component - anti-entropy. Why
 * is it needed and even vital? Consider the example: two SWIM
 * nodes, both are alive. Nothing happens, so the events list is
 * empty, only pings are being sent periodically. Then a third
 * node appears. It knows about one of existing nodes. How should
 * it learn about another one? The cluster is stable, no new
 * events, so the only chance is to wait until another server
 * stops and event about it will be broadcasted. Anti-entropy is
 * an extra simple component, it just piggybacks random part of
 * members table with each regular ping. In the example above the
 * new node will learn about the third one via anti-entropy
 * messages of the second one.
 */

/**
 * Global hash of all known members of the cluster. Hash key is
 * bitwise combination of ip and port, value is a struct member,
 * describing a remote instance.
 *
 * Discovered members live here until they are unavailable - in
 * such a case they are removed from the hash. But a subset of
 * members are pinned - the ones added via SWIM configuration.
 * When a member is pinned, it can not be removed from the hash,
 * and the module will ping him constantly.
 */
static struct mh_i64ptr_t *members = NULL;

static inline uint64_t
sockaddr_in_hash(const struct sockaddr_in *a)
{
	return ((uint64_t) a->sin_addr.s_addr << 16) | a->sin_port;
}

static inline void
sockaddr_in_unhash(struct sockaddr_in *a, uint64_t hash)
{
	memset(a, 0, sizeof(*a));
	a->sin_family = AF_INET;
	a->sin_addr.s_addr = hash >> 16;
	a->sin_port = hash & 0xffff;
}

/**
 * Each SWIM component in a common case independently may want to
 * push some data into the network. Dissemination sends events,
 * failure detection sends pings, acks. Anti-entropy sends member
 * tables. The intention to send a data is called IO task and is
 * stored in a queue that is dispatched when output is possible.
 */
enum swim_io_task_type {
	/**
	 * Send merged components to a next member in the round.
	 */
	SWIM_IO_ROUND_STEP,
	/** A ping was received. This task schedules an ack. */
	SWIM_IO_ACK,
	/**
	 * This task schedules a separate ping, not attached to a
	 * regular round message. Used when the latter has failed.
	 */
	SWIM_IO_PING,
};

struct swim_io_task {
	enum swim_io_task_type type;
	struct rlist in_queue_output;
};

static inline void
swim_io_task_create(struct swim_io_task *task, enum swim_io_task_type type)
{
	task->type = type;
	rlist_create(&task->in_queue_output);
}

static inline void
swim_io_task_destroy(struct swim_io_task *task)
{
	rlist_del_entry(task, in_queue_output);
}

enum swim_member_status {
	/**
	 * The instance is ok, it responds to requests, sends its
	 * members table.
	 */
	MEMBER_ALIVE = 0,
	/**
	 * The member is considered to be dead. It will disappear
	 * from the membership, if it is not pinned.
	 */
	MEMBER_DEAD,
	swim_member_status_MAX,
};

static const char *swim_member_status_strs[] = {
	"alive",
	"dead",
};

/**
 * A cluster member description. This structure describes the
 * last known state of an instance, that is updated periodically
 * via UDP according to SWIM protocol.
 */
struct swim_member {
	/**
	 * Member status. Since the communication goes via UDP,
	 * actual status can be different, as well as different on
	 * other SWIM nodes. But SWIM guarantees that each member
	 * will learn a real status of an instance sometime.
	 */
	enum swim_member_status status;
	/**
	 * Address of the instance to which send UDP packets.
	 * Unique identifier of the member.
	 */
	struct sockaddr_in addr;
	/**
	 * Position in a queue of members in the current round.
	 */
	struct rlist in_queue_round;
	/**
	 *
	 *               Failure detection component
	 */
	/**
	 * True, if the member is configured explicitly and can
	 * not disappear from the membership.
	 */
	bool is_pinned;
	/** Growing number to refute old messages. */
	uint64_t incarnation;
	/**
	 * How many pings did not receive an ack in a row. After
	 * a threshold the instance is marked as dead. After more
	 * it is removed from the table (if not pinned).
	 */
	int failed_pings;
	/** When the last ping was sent. */
	double ping_time;
	/**
	 * Ready at hand ack task to send when this member has
	 * sent ping to us.
	 */
	struct swim_io_task ack_task;
	/**
	 * Ready at hand ping task to send when this member too
	 * long does not respond to an initial ping, piggybacked
	 * with members table.
	 */
	struct swim_io_task ping_task;
	/** Position in a queue of members waiting for an ack. */
	struct rlist in_queue_wait_ack;
	/**
	 *
	 *                 Dissemination component
	 *
	 * Dissemination component sends events. Event is a
	 * notification about member status update. So formally,
	 * this structure already has all the needed attributes.
	 * But also an event somehow should be sent to all members
	 * at least once according to SWIM, so it requires
	 * something like TTL, which decrements on each send. And
	 * a member can not be removed from the global table until
	 * it gets dead and its dissemination TTL is 0, so as to
	 * allow other members learn its dead status.
	 */
	int dissemination_ttl;
	/**
	 * Events are put into a queue sorted by event occurrence
	 * time.
	 */
	struct rlist in_queue_events;
};

/** This node. Used to do not send messages to self. */
static struct swim_member *self = NULL;

/**
 * Update status of the member if needed. Statuses are compared as
 * a compound key: {incarnation, status}. So @a new_status can
 * override an old one only if its incarnation is greater, or the
 * same, but its status is "bigger". Statuses are compared by
 * their identifier, so "alive" < "dead". This protects from the
 * case when a member is detected as dead on one instance, but
 * overriden by another instance with the same incarnation "alive"
 * message.
 *
 * @retval True if the status is changed.
 */
static inline bool
swim_member_update_status(struct swim_member *member,
			  enum swim_member_status new_status,
			  uint64_t incarnation)
{
	assert(member != self);
	if (member->incarnation == incarnation) {
		if (member->status < new_status) {
			member->status = new_status;
			return true;
		} else {
			return false;
		}
	} else if (member->incarnation < incarnation) {
		member->status = new_status;
		member->incarnation = incarnation;
		return true;
	} else {
		return false;
	}
}

/**
 * Main round messages can carry merged failure detection
 * messages and anti-entropy. With these keys the components can
 * be distinguished.
 */
enum swim_component_type {
	SWIM_ANTI_ENTROPY = 0,
	SWIM_FAILURE_DETECTION,
	SWIM_DISSEMINATION,
};

/** {{{                Failure detection component              */

/** Possible failure detection keys. */
enum swim_fd_key {
	/** Type of the failure detection message: ping or ack. */
	SWIM_FD_MSG_TYPE,
	/**
	 * Incarnation of the sender. To make the member alive if
	 * it was considered to be dead, but ping/ack with greater
	 * incarnation was received from it.
	 */
	SWIM_FD_INCARNATION,
};

/**
 * Failure detection message now has only two types: ping or ack.
 * Indirect ping/ack are todo.
 */
enum swim_fd_msg_type {
	SWIM_FD_MSG_PING,
	SWIM_FD_MSG_ACK,
	swim_fd_msg_type_MAX,
};

static const char *swim_fd_msg_type_strs[] = {
	"ping",
	"ack",
};

/** SWIM failure detection MsgPack header template. */
struct PACKED swim_fd_header_bin {
	/** mp_encode_uint(SWIM_FAILURE_DETECTION) */
	uint8_t k_header;
	/** mp_encode_map(2) */
	uint8_t m_header;

	/** mp_encode_uint(SWIM_FD_MSG_TYPE) */
	uint8_t k_type;
	/** mp_encode_uint(enum swim_fd_msg_type) */
	uint8_t v_type;

	/** mp_encode_uint(SWIM_FD_INCARNATION) */
	uint8_t k_incarnation;
	/** mp_encode_uint(64bit incarnation) */
	uint8_t m_incarnation;
	uint64_t v_incarnation;
};

static inline void
swim_fd_header_bin_create(struct swim_fd_header_bin *header,
			  enum swim_fd_msg_type type)
{
	header->k_header = SWIM_FAILURE_DETECTION;
	header->m_header = 0x82;

	header->k_type = SWIM_FD_MSG_TYPE;
	header->v_type = type;

	header->k_incarnation = SWIM_FD_INCARNATION;
	header->m_incarnation = 0xcf;
	header->v_incarnation = mp_bswap_u64(self->incarnation);
}

/**
 * Members waiting for an ACK. On too long absence of ACK a member
 * is considered to be dead and is removed. The list is sorted by
 * time in ascending order (tail is newer, head is older).
 */
static RLIST_HEAD(queue_wait_ack);
/** Generator of ack checking events. */
static struct ev_periodic wait_ack_tick;

static void
swim_member_schedule_ack_wait(struct swim_member *member)
{
	if (rlist_empty(&member->in_queue_wait_ack)) {
		member->ping_time = fiber_time();
		rlist_add_tail_entry(&queue_wait_ack, member,
				     in_queue_wait_ack);
	}
}

/** }}}               Failure detection component               */

/** {{{                  Anti-entropy component                 */

/**
 * Attributes of each record of a broadcasted member table. Just
 * the same as some of struct swim_member attributes.
 */
enum swim_member_key {
	SWIM_MEMBER_STATUS = 0,
	SWIM_MEMBER_ADDR_HASH,
	SWIM_MEMBER_INCARNATION,
	swim_member_key_MAX,
};

/** SWIM anti-entropy MsgPack header template. */
struct PACKED swim_anti_entropy_header_bin {
	/** mp_encode_uint(SWIM_ANTI_ENTROPY) */
	uint8_t k_anti_entropy;
	/** mp_encode_array() */
	uint8_t m_anti_entropy;
	uint32_t v_anti_entropy;
};

static inline void
swim_anti_entropy_header_bin_create(struct swim_anti_entropy_header_bin *header,
				    int batch_size)
{
	header->k_anti_entropy = SWIM_ANTI_ENTROPY;
	header->m_anti_entropy = 0xdd;
	header->v_anti_entropy = mp_bswap_u32(batch_size);
}

/** SWIM member MsgPack template. */
struct PACKED swim_member_bin {
	/** mp_encode_map(3) */
	uint8_t m_header;

	/** mp_encode_uint(SWIM_MEMBER_STATUS) */
	uint8_t k_status;
	/** mp_encode_uint(enum member_status) */
	uint8_t v_status;

	/** mp_encode_uint(SWIM_MEMBER_ADDR_HASH) */
	uint8_t k_addr;
	/** mp_encode_uint((ip << 16) | port) */
	uint8_t m_addr;
	uint64_t v_addr;

	/** mp_encode_uint(SWIM_MEMBER_INCARNATION) */
	uint8_t k_incarnation;
	/** mp_encode_uint(64bit incarnation) */
	uint8_t m_incarnation;
	uint64_t v_incarnation;
};

static inline void
swim_member_bin_reset(struct swim_member_bin *header,
		      struct swim_member *member)
{
	header->v_status = member->status;
	header->v_addr = mp_bswap_u64(sockaddr_in_hash(&member->addr));
	header->v_incarnation = mp_bswap_u64(member->incarnation);
}

static inline void
swim_member_bin_create(struct swim_member_bin *header)
{
	header->m_header = 0x83;
	header->k_status = SWIM_MEMBER_STATUS;
	header->k_addr = SWIM_MEMBER_ADDR_HASH;
	header->m_addr = 0xcf;
	header->k_incarnation = SWIM_MEMBER_INCARNATION;
	header->m_incarnation = 0xcf;
}

/**
 * Members to which a message should be sent next during this
 * round.
 */
static RLIST_HEAD(queue_round);
/** Generator of round step events. */
static struct ev_periodic round_tick;

/**
 * Single round step task. It is impossible to have multiple
 * round steps at the same time, so it is static and global.
 * Other tasks are mainly pings and acks, attached to member
 * objects.
 */
static struct swim_io_task round_step_task = {
	/* .type = */ SWIM_IO_ROUND_STEP,
	/* .in_queue_output = */ RLIST_LINK_INITIALIZER,
};

/** }}}                  Anti-entropy component                 */

/** {{{                 Dissemination component                 */

/** SWIM dissemination MsgPack template. */
struct PACKED swim_diss_header_bin {
	/** mp_encode_uint(SWIM_DISSEMINATION) */
	uint8_t k_header;
	/** mp_encode_array() */
	uint8_t m_header;
	uint32_t v_header;
};

static inline void
swim_diss_header_bin_create(struct swim_diss_header_bin *header, int batch_size)
{
	header->k_header = SWIM_DISSEMINATION;
	header->m_header = 0xdd;
	header->v_header = mp_bswap_u32(batch_size);
}

/** SWIM event MsgPack template. */
struct PACKED swim_event_bin {
	/** mp_encode_map(3) */
	uint8_t m_header;

	/** mp_encode_uint(SWIM_MEMBER_STATUS) */
	uint8_t k_status;
	/** mp_encode_uint(enum member_status) */
	uint8_t v_status;

	/** mp_encode_uint(SWIM_MEMBER_ADDR_HASH) */
	uint8_t k_addr;
	/** mp_encode_uint((ip << 16) | port) */
	uint8_t m_addr;
	uint64_t v_addr;

	/** mp_encode_uint(SWIM_MEMBER_INCARNATION) */
	uint8_t k_incarnation;
	/** mp_encode_uint(64bit incarnation) */
	uint8_t m_incarnation;
	uint64_t v_incarnation;
};

static inline void
swim_event_bin_create(struct swim_event_bin *header)
{
	header->m_header = 0x83;
	header->k_status = SWIM_MEMBER_STATUS;
	header->k_addr = SWIM_MEMBER_ADDR_HASH;
	header->m_addr = 0xcf;
	header->k_incarnation = SWIM_MEMBER_INCARNATION;
	header->m_incarnation = 0xcf;
}

static inline void
swim_event_bin_reset(struct swim_event_bin *event, struct swim_member *member)
{
	event->v_status = member->status;
	event->v_addr = mp_bswap_u64(sockaddr_in_hash(&member->addr));
	event->v_incarnation = mp_bswap_u64(member->incarnation);
}

/** Queue of events sorted by occurrence time. */
static RLIST_HEAD(queue_events);
static int event_count = 0;

static inline void
swim_schedule_event(struct swim_member *member)
{
	if (rlist_empty(&member->in_queue_events)) {
		rlist_add_tail_entry(&queue_events, member, in_queue_events);
		event_count++;
	}
	member->dissemination_ttl = mh_size(members);
}

/** }}}                 Dissemination component                 */

/**
 * SWIM message structure:
 * {
 *     SWIM_FAILURE_DETECTION: {
 *         SWIM_FD_MSG_TYPE: uint, enum swim_fd_msg_type,
 *         SWIM_FD_INCARNATION: uint
 *     },
 *
 *                 OR/AND
 *
 *     SWIM_DISSEMINATION: [
 *         {
 *             SWIM_MEMBER_STATUS: uint, enum member_status,
 *             SWIM_MEMBER_ADDR_HASH: uint, (ip << 16) | port,
 *             SWIM_MEMBER_INCARNATION: uint
 *         }
 *     ],
 *
 *                 OR/AND
 *
 *     SWIM_ANTI_ENTROPY: [
 *         {
 *             SWIM_MEMBER_STATUS: uint, enum member_status,
 *             SWIM_MEMBER_ADDR_HASH: uint, (ip << 16) | port,
 *             SWIM_MEMBER_INCARNATION: uint,
 *         },
 *         ...
 *     ],
 * }
 */

enum {
	/** How often to send membership messages and pings. */
	HEARTBEAT_RATE_DEFAULT = 1,
	/**
	 * Default MTU is 1500. MTU (when IPv4 is used) consists
	 * of IPv4 header, UDP header, Data. IPv4 has 20 bytes
	 * header, UDP - 8 bytes. So Data = 1500 - 20 - 8 = 1472.
	 */
	UDP_PACKET_SIZE = 1472,
	/**
	 * If a ping was sent, it is considered to be lost after
	 * this time without an ack.
	 */
	ACK_TIMEOUT = 1,
	/**
	 * If a member has not been responding to pings this
	 * number of times, it is considered to be dead.
	 */
	NO_ACKS_TO_DEAD = 3,
	/**
	 * If a not pinned member confirmed to be dead, it is
	 * removed from the membership after at least this number
	 * of failed pings.
	 */
	NO_ACKS_TO_GC = NO_ACKS_TO_DEAD + 2,
};

/**
 * Event dispatcher of incomming messages. Takes them from
 * network.
 */
static struct ev_io input;
/**
 * Event dispatcher of outcomming messages. Takes tasks from
 * queue_output.
 */
static struct ev_io output;

/**
 * An array of configured members. Used only to easy rollback a
 * failed reconfiguration.
 */
static struct swim_member **cfg = NULL;
/** Number of configured members. */
static int cfg_size = 0;

/**
 * An array of members shuffled on each round. Its head it sent
 * to each member during one round as an anti-entropy message.
 */
static struct swim_member **shuffled_members = NULL;
static int shuffled_members_size = 0;

/** Queue of io tasks ready to push now. */
static RLIST_HEAD(queue_output);

static inline void
swim_io_task_push(struct swim_io_task *task)
{
	rlist_add_tail_entry(&queue_output, task, in_queue_output);
	ev_io_start(loop(), &output);
}

/**
 * Register a new member with a specified status. Here it is
 * added to the hash, to the 'next' queue.
 */
static struct swim_member *
swim_member_new(const struct sockaddr_in *addr, enum swim_member_status status,
		uint64_t incarnation)
{
	struct swim_member *member =
		(struct swim_member *) malloc(sizeof(*member));
	if (member == NULL) {
		diag_set(OutOfMemory, sizeof(*member), "malloc", "member");
		return NULL;
	}
	member->status = status;
	member->addr = *addr;
	member->incarnation = incarnation;
	member->is_pinned = false;
	member->failed_pings = 0;
	member->ping_time = 0;
	struct mh_i64ptr_node_t node;
	node.key = sockaddr_in_hash(addr);
	node.val = member;
	mh_int_t rc = mh_i64ptr_put(members, &node, NULL, NULL);
	if (rc == mh_end(members)) {
		free(member);
		diag_set(OutOfMemory, sizeof(mh_int_t), "malloc", "node");
		return NULL;
	}
	swim_io_task_create(&member->ack_task, SWIM_IO_ACK);
	swim_io_task_create(&member->ping_task, SWIM_IO_PING);
	rlist_add_entry(&queue_round, member, in_queue_round);
	rlist_create(&member->in_queue_wait_ack);
	rlist_create(&member->in_queue_events);
	return member;
}

static inline struct swim_member *
swim_find_member(uint64_t hash)
{
	mh_int_t node = mh_i64ptr_find(members, hash, NULL);
	if (node == mh_end(members))
		return NULL;
	return (struct swim_member *) mh_i64ptr_node(members, node)->val;
}

/**
 * Remove the member from all queues, hashes, destroy it and free
 * the memory.
 */
static inline void
swim_member_delete(struct swim_member *member)
{
	uint64_t key = sockaddr_in_hash(&member->addr);
	mh_int_t rc = mh_i64ptr_find(members, key, NULL);
	assert(rc != mh_end(members));
	mh_i64ptr_del(members, rc, NULL);
	swim_io_task_destroy(&member->ack_task);
	swim_io_task_destroy(&member->ping_task);
	rlist_del_entry(member, in_queue_round);
	rlist_del_entry(member, in_queue_wait_ack);
	assert(rlist_empty(&member->in_queue_events));
	free(member);
}

/** At the end of each round members table is shuffled. */
static int
swim_shuffle_and_filter_members()
{
	struct swim_member *m, *tmp;
	rlist_foreach_entry_safe(m, &queue_wait_ack, in_queue_wait_ack, tmp) {
		if (m->failed_pings < NO_ACKS_TO_GC)
			break;
		if (!m->is_pinned && m->dissemination_ttl == 0)
			swim_member_delete(m);
	}
	int new_size = mh_size(members);
	/* Realloc is too big or too small. */
	if (shuffled_members_size < new_size ||
	    shuffled_members_size >= new_size * 2) {
		int size = sizeof(shuffled_members[0]) * new_size;
		struct swim_member **new =
			(struct swim_member **) realloc(shuffled_members, size);
		if (new == NULL) {
			diag_set(OutOfMemory, size, "realloc", "new");
			return -1;
		}
		shuffled_members = new;
		shuffled_members_size = new_size;
	}
	int i = 0;
	for (mh_int_t node = mh_first(members), end = mh_end(members);
	     node != end; node = mh_next(members, node), ++i) {
		shuffled_members[i] = (struct swim_member *)
			mh_i64ptr_node(members, node)->val;
		/*
		 * rand_max / (end - start + 1) + 1 - scaled range
		 * of random numbers to save distribution.
		 */
		int j = rand() / (RAND_MAX / (i + 1) + 1);
		SWAP(shuffled_members[i], shuffled_members[j]);
	}
	return 0;
}

/**
 * Shuffle, filter members. Build randomly ordered queue of
 * addressees. In other words, do all round preparation work.
 */
static int
swim_new_round()
{
	say_verbose("SWIM: start a new round");
	if (swim_shuffle_and_filter_members() != 0)
		return -1;
	rlist_create(&queue_round);
	for (int i = 0; i < shuffled_members_size; ++i) {
		if (shuffled_members[i] != self) {
			rlist_add_entry(&queue_round, shuffled_members[i],
					in_queue_round);
		}
	}
	return 0;
}

static inline int
calculate_bin_batch_size(int header_size, int member_size, int avail_size)
{
	if (avail_size <= header_size)
		return 0;
	return (avail_size - header_size) / member_size;
}

static int
swim_encode_round_msg(char *buffer, int size)
{
	char *start = buffer;
	if ((shuffled_members == NULL || rlist_empty(&queue_round)) &&
	    swim_new_round() != 0)
		return -1;
	/* +1 - for the root map header. */
	assert((uint)size > sizeof(struct swim_fd_header_bin) + 1);
	size -= sizeof(struct swim_fd_header_bin) + 1;

	int diss_batch_size = calculate_bin_batch_size(
		sizeof(struct swim_diss_header_bin),
		sizeof(struct swim_event_bin), size);
	size -= sizeof(struct swim_diss_header_bin) -
		diss_batch_size * sizeof(struct swim_event_bin);

	int ae_batch_size = calculate_bin_batch_size(
		sizeof(struct swim_anti_entropy_header_bin),
		sizeof(struct swim_member_bin), size);

	buffer = mp_encode_map(buffer, 1 + (diss_batch_size > 0) +
			       (ae_batch_size > 0));

	struct swim_fd_header_bin fd_header_bin;
	swim_fd_header_bin_create(&fd_header_bin, SWIM_FD_MSG_PING);
	memcpy(buffer, &fd_header_bin, sizeof(fd_header_bin));
	buffer += sizeof(fd_header_bin);

	if (diss_batch_size > 0) {
		struct swim_diss_header_bin diss_header_bin;
		swim_diss_header_bin_create(&diss_header_bin, diss_batch_size);
		memcpy(buffer, &diss_header_bin, sizeof(diss_header_bin));
		buffer += sizeof(diss_header_bin);

		int i = 0;
		struct swim_member *member, *tmp;
		struct swim_event_bin event_bin;
		swim_event_bin_create(&event_bin);
		rlist_foreach_entry_safe(member, &queue_events, in_queue_events,
					 tmp) {
			swim_event_bin_reset(&event_bin, member);
			memcpy(buffer, &event_bin, sizeof(event_bin));
			buffer += sizeof(event_bin);
			rlist_del_entry(member, in_queue_events);
			--member->dissemination_ttl;
			if (++i >= diss_batch_size)
				break;
		}
		event_count -= diss_batch_size;
	}

	if (ae_batch_size == 0)
		return buffer - start;
	struct swim_anti_entropy_header_bin ae_header_bin;
	swim_anti_entropy_header_bin_create(&ae_header_bin, ae_batch_size);
	memcpy(buffer, &ae_header_bin, sizeof(ae_header_bin));
	buffer += sizeof(ae_header_bin);

	struct swim_member_bin member_bin;
	swim_member_bin_create(&member_bin);
	for (int i = 0; i < ae_batch_size; ++i) {
		struct swim_member *member = shuffled_members[i];
		swim_member_bin_reset(&member_bin, member);
		memcpy(buffer, &member_bin, sizeof(member_bin));
		buffer += sizeof(member_bin);
	}
	return buffer - start;
}

/**
 * Do one round step. Send encoded components to a next member
 * from the queue.
 */
static void
swim_send_round_msg()
{
	char buffer[UDP_PACKET_SIZE];
	int size = swim_encode_round_msg(buffer, UDP_PACKET_SIZE);
	if (size < 0) {
		diag_log();
		return;
	}
	struct swim_member *m =
		rlist_first_entry(&queue_round, struct swim_member,
				  in_queue_round);
	say_verbose("SWIM: send to %s",
		    sio_strfaddr((struct sockaddr *) &m->addr,
				 sizeof(m->addr)));
	if (sio_sendto(output.fd, buffer, size, 0, (struct sockaddr *) &m->addr,
		       sizeof(m->addr)) == -1)
		diag_log();
	swim_member_schedule_ack_wait(m);
	rlist_del_entry(m, in_queue_round);
	ev_periodic_start(loop(), &round_tick);
}

/** Send a failure detection message. */
static void
swim_send_fd_message(struct swim_member *m, enum swim_fd_msg_type type)
{
	char buffer[UDP_PACKET_SIZE];
	char *pos = mp_encode_map(buffer, 1);
	struct swim_fd_header_bin header_bin;
	swim_fd_header_bin_create(&header_bin, type);
	memcpy(pos, &header_bin, sizeof(header_bin));
	pos += sizeof(header_bin);
	assert(pos - buffer <= (int)sizeof(buffer));
	say_verbose("SWIM: send %s to %s", swim_fd_msg_type_strs[type],
		    sio_strfaddr((struct sockaddr *) &m->addr,
				 sizeof(m->addr)));
	if (sio_sendto(output.fd, buffer, pos - buffer, 0,
		       (struct sockaddr *) &m->addr, sizeof(m->addr)) == -1)
		diag_log();
}

static inline void
swim_send_ack(struct swim_io_task *task)
{
	struct swim_member *m = container_of(task, struct swim_member,
					     ack_task);
	swim_send_fd_message(m, SWIM_FD_MSG_ACK);
}

static inline void
swim_send_ping(struct swim_io_task *task)
{
	struct swim_member *m = container_of(task, struct swim_member,
					     ping_task);
	swim_send_fd_message(m, SWIM_FD_MSG_PING);
	swim_member_schedule_ack_wait(m);
}

static void
swim_on_output(struct ev_loop *loop, struct ev_io *io, int events)
{
	assert((events & EV_WRITE) != 0);
	(void) events;
	if (rlist_empty(&queue_output)) {
		ev_io_stop(loop, io);
		return;
	}
	struct swim_io_task *task =
		rlist_shift_entry(&queue_output, struct swim_io_task,
				  in_queue_output);
	switch(task->type) {
	case SWIM_IO_ROUND_STEP:
		swim_send_round_msg();
		break;
	case SWIM_IO_PING:
		swim_send_ping(task);
		break;
	case SWIM_IO_ACK:
		swim_send_ack(task);
		break;
	default:
		unreachable();
	}
}

/** Once per specified timeout trigger a next broadcast step. */
static void
swim_trigger_round_step(struct ev_loop *loop, struct ev_periodic *p, int events)
{
	assert((events & EV_PERIODIC) != 0);
	(void) events;
	swim_io_task_push(&round_step_task);
	ev_periodic_stop(loop, p);
}

/**
 * Check for failed pings. A ping is failed if an ack was not
 * received during ACK_TIMEOUT. A failed ping is resent here.
 */
static void
swim_check_acks(struct ev_loop *loop, struct ev_periodic *p, int events)
{
	assert((events & EV_PERIODIC) != 0);
	(void) loop;
	(void) p;
	(void) events;
	struct swim_member *m, *tmp;
	double current_time = fiber_time();
	rlist_foreach_entry_safe(m, &queue_wait_ack, in_queue_wait_ack, tmp) {
		if (current_time - m->ping_time < ACK_TIMEOUT)
			break;
		if (++m->failed_pings >= NO_ACKS_TO_DEAD) {
			m->status = MEMBER_DEAD;
			swim_schedule_event(m);
		}
		swim_io_task_push(&m->ping_task);
		rlist_del_entry(m, in_queue_wait_ack);
	}
}

/**
 * SWIM member attributes from anti-entropy and dissemination
 * messages.
 */
struct swim_member_def {
	uint64_t addr_hash;
	uint64_t incarnation;
	enum swim_member_status status;
};

static inline void
swim_member_def_create(struct swim_member_def *def)
{
	def->addr_hash = UINT64_MAX;
	def->status = MEMBER_ALIVE;
	def->incarnation = 0;
}

static void
swim_process_member_update(struct swim_member_def *def)
{
	struct swim_member *member = swim_find_member(def->addr_hash);
	/*
	 * Trivial processing of a new member - just add it to the
	 * members table.
	 */
	if (member == NULL) {
		struct sockaddr_in addr;
		sockaddr_in_unhash(&addr, def->addr_hash);
		member = swim_member_new(&addr, def->status, def->incarnation);
		if (member == NULL)
			diag_log();
		swim_schedule_event(member);
		return;
	}
	if (member != self) {
		if (swim_member_update_status(member, def->status,
					      def->incarnation))
			swim_schedule_event(member);
		return;
	}
	/*
	 * It is possible that other instances know a bigger
	 * incarnation of this instance - such thing happens when
	 * the instance restarts and loses its local incarnation
	 * number. It will be restored by receiving dissemination
	 * messages about self.
	 */
	if (self->incarnation < def->incarnation)
		self->incarnation = def->incarnation;
	if (def->status != MEMBER_ALIVE) {
		/*
		 * In the cluster a gossip exists that this
		 * instance is not alive. Refute this information
		 * with a bigger incarnation.
		 */
		self->incarnation++;
	}
}

static int
swim_process_member_key(enum swim_member_key key, const char **pos,
			const char *end, const char *msg_pref,
			struct swim_member_def *def)
{
	switch(key) {
	case SWIM_MEMBER_STATUS:
		if (mp_typeof(**pos) != MP_UINT ||
		    mp_check_uint(*pos, end) > 0) {
			say_error("%s member status should be uint", msg_pref);
			return -1;
		}
		key = mp_decode_uint(pos);
		if (key >= swim_member_status_MAX) {
			say_error("%s unknown member status", msg_pref);
			return -1;
		}
		def->status = (enum swim_member_status) key;
		break;
	case SWIM_MEMBER_ADDR_HASH:
		if (mp_typeof(**pos) != MP_UINT ||
		    mp_check_uint(*pos, end) > 0) {
			say_error("%s member address should be uint", msg_pref);
			return -1;
		}
		def->addr_hash = mp_decode_uint(pos);
		if ((def->addr_hash >> 48) != 0) {
			say_error("%s invalid address",  msg_pref);
			return -1;
		}
		break;
	case SWIM_MEMBER_INCARNATION:
		if (mp_typeof(**pos) != MP_UINT ||
		    mp_check_uint(*pos, end) > 0) {
			say_error("%s member incarnation should be uint",
				  msg_pref);
			return -1;
		}
		def->incarnation = mp_decode_uint(pos);
		break;
	default:
		unreachable();
	}
	return 0;
}

/** Decode an anti-entropy message, update members table. */
static int
swim_process_anti_entropy(const char **pos, const char *end)
{
	const char *msg_pref = "Invalid SWIM anti-entropy message:";
	if (mp_typeof(**pos) != MP_ARRAY || mp_check_array(*pos, end) > 0) {
		say_error("%s message should be an array", msg_pref);
		return -1;
	}
	uint64_t size = mp_decode_array(pos);
	for (uint64_t i = 0; i < size; ++i) {
		if (mp_typeof(**pos) != MP_MAP ||
		    mp_check_map(*pos, end) > 0) {
			say_error("%s member should be map", msg_pref);
			return -1;
		}
		uint64_t map_size = mp_decode_map(pos);
		struct swim_member_def def;
		swim_member_def_create(&def);
		for (uint64_t j = 0; j < map_size; ++j) {
			if (mp_typeof(**pos) != MP_UINT ||
			    mp_check_uint(*pos, end) > 0) {
				say_error("%s member key should be uint",
					  msg_pref);
				return -1;
			}
			uint64_t key = mp_decode_uint(pos);
			if (key >= swim_member_key_MAX) {
				say_error("%s unknown member key", msg_pref);
				return -1;
			}
			if (swim_process_member_key(key, pos, end, msg_pref,
						    &def) != 0)
				return -1;
		}
		if (def.addr_hash == UINT64_MAX) {
			say_error("%s member address should be specified",
				  msg_pref);
			return -1;
		}
		swim_process_member_update(&def);
	}
	return 0;
}

/**
 * Decode a failure detection message. Schedule pings, process
 * acks.
 */
static int
swim_process_failure_detection(const char **pos, const char *end,
			       const struct sockaddr_in *src)
{
	const char *msg_pref = "Invalid SWIM failure detection message:";
	if (mp_typeof(**pos) != MP_MAP || mp_check_map(*pos, end) > 0) {
		say_error("%s root should be a map", msg_pref);
		return -1;
	}
	uint64_t size = mp_decode_map(pos);
	if (size != 2) {
		say_error("%s root map should have two keys - message type "\
			  "and incarnation", msg_pref);
		return -1;
	}
	enum swim_fd_msg_type type = swim_fd_msg_type_MAX;
	uint64_t incarnation = 0;
	for (int i = 0; i < (int) size; ++i) {
		if (mp_typeof(**pos) != MP_UINT ||
		    mp_check_uint(*pos, end) > 0) {
			say_error("%s a key should be uint", msg_pref);
			return -1;
		}
		uint64_t key = mp_decode_uint(pos);
		switch(key) {
		case SWIM_FD_MSG_TYPE:
			if (mp_typeof(**pos) != MP_UINT ||
			    mp_check_uint(*pos, end) > 0) {
				say_error("%s message type should be uint",
					  msg_pref);
				return -1;
			}
			key = mp_decode_uint(pos);
			if (key >= swim_fd_msg_type_MAX) {
				say_error("%s unknown message type", msg_pref);
				return -1;
			}
			type = key;
			break;
		case SWIM_FD_INCARNATION:
			if (mp_typeof(**pos) != MP_UINT ||
			    mp_check_uint(*pos, end) > 0) {
				say_error("%s incarnation should be uint",
					  msg_pref);
				return -1;
			}
			incarnation = mp_decode_uint(pos);
			break;
		default:
			say_error("%s unknown key", msg_pref);
			return -1;
		}
	}
	if (type == swim_fd_msg_type_MAX) {
		say_error("%s message type should be specified", msg_pref);
		return -1;
	}
	struct swim_member *sender = swim_find_member(sockaddr_in_hash(src));
	if (sender == NULL) {
		sender = swim_member_new(src, MEMBER_ALIVE, incarnation);
		if (sender == NULL) {
			diag_log();
			return 0;
		}
	} else {
		swim_member_update_status(sender, MEMBER_ALIVE, incarnation);
	}
	if (type == SWIM_FD_MSG_PING) {
		swim_io_task_push(&sender->ack_task);
	} else {
		assert(type == SWIM_FD_MSG_ACK);
		if (incarnation >= sender->incarnation) {
			sender->failed_pings = 0;
			rlist_del_entry(&sender->ping_task, in_queue_output);
			rlist_del_entry(sender, in_queue_wait_ack);
		}
	}
	return 0;
}

static int
swim_process_dissemination(const char **pos, const char *end)
{
	const char *msg_pref = "Invald SWIM dissemination message:";
	if (mp_typeof(**pos) != MP_ARRAY || mp_check_array(*pos, end) > 0) {
		say_error("%s message should be an array", msg_pref);
		return -1;
	}
	uint64_t size = mp_decode_array(pos);
	for (uint64_t i = 0; i < size; ++i) {
		if (mp_typeof(**pos) != MP_MAP ||
		    mp_check_map(*pos, end) > 0) {
			say_error("%s event should be map", msg_pref);
			return -1;
		}
		uint64_t map_size = mp_decode_map(pos);
		struct swim_member_def def;
		swim_member_def_create(&def);
		for (uint64_t j = 0; j < map_size; ++j) {
			if (mp_typeof(**pos) != MP_UINT ||
			    mp_check_uint(*pos, end) > 0) {
				say_error("%s event key should be uint",
					  msg_pref);
				return -1;
			}
			uint64_t key = mp_decode_uint(pos);
			if (key >= swim_member_key_MAX) {
				say_error("%s unknown event key", msg_pref);
				return -1;
			}
			if (swim_process_member_key(key, pos, end, msg_pref,
						    &def) != 0)
				return -1;
		}
		if (def.addr_hash == UINT64_MAX) {
			say_error("%s member address should be specified",
				  msg_pref);
			return -1;
		}
		swim_process_member_update(&def);
	}
	return 0;
}

/** Receive and process a new message. */
static void
swim_on_input(struct ev_loop *loop, struct ev_io *io, int events)
{
	assert((events & EV_READ) != 0);
	(void) events;
	(void) loop;
	const char *msg_pref = "Invalid SWIM message:";
	struct sockaddr_in addr;
	socklen_t len = sizeof(addr);
	char buffer[UDP_PACKET_SIZE];
	ssize_t size = sio_recvfrom(io->fd, buffer, sizeof(buffer), 0,
				    (struct sockaddr *) &addr, &len);
	if (size == -1) {
		diag_log();
		return;
	}
	say_verbose("SWIM: received from %s",
		    sio_strfaddr((struct sockaddr *) &addr, len));
	const char *pos = buffer;
	const char *end = pos + size;
	if (mp_typeof(*pos) != MP_MAP || mp_check_map(pos, end) > 0) {
		say_error("%s expected map header", msg_pref);
		return;
	}
	uint64_t map_size = mp_decode_map(&pos);
	for (uint64_t i = 0; i < map_size; ++i) {
		if (mp_typeof(*pos) != MP_UINT || mp_check_uint(pos, end) > 0) {
			say_error("%s header should contain uint keys",
				  msg_pref);
			return;
		}
		uint64_t key = mp_decode_uint(&pos);
		switch(key) {
		case SWIM_ANTI_ENTROPY:
			say_verbose("SWIM: process anti-entropy");
			if (swim_process_anti_entropy(&pos, end) != 0)
				return;
			break;
		case SWIM_FAILURE_DETECTION:
			say_verbose("SWIM: process failure detection");
			if (swim_process_failure_detection(&pos, end,
							   &addr) != 0)
				return;
			break;
		case SWIM_DISSEMINATION:
			say_verbose("SWIM: process dissemination");
			if (swim_process_dissemination(&pos, end) != 0)
				return;
			break;
		default:
			say_error("%s unknown component type component is "\
				  "supported", msg_pref);
			return;
		}
	}
}

/**
 * Convert a string URI like "ip:port" to sockaddr_in structure.
 */
static int
uri_to_addr(const char *str, struct sockaddr_in *addr)
{
	struct uri uri;
	if (uri_parse(&uri, str) != 0 || uri.service == NULL)
		goto invalid_uri;
	in_addr_t iaddr;
	if (uri.host_len == 0 || (uri.host_len == 9 &&
				  memcmp("localhost", uri.host, 9) == 0)) {
		iaddr = htonl(INADDR_ANY);
	} else {
		iaddr = inet_addr(tt_cstr(uri.host, uri.host_len));
		if (iaddr == (in_addr_t) -1)
			goto invalid_uri;
	}
	int port = htons(atoi(uri.service));
	memset(addr, 0, sizeof(*addr));
	addr->sin_family = AF_INET;
	addr->sin_addr.s_addr = iaddr;
	addr->sin_port = port;
	return 0;

invalid_uri:
	diag_set(SocketError, sio_socketname(-1), "invalid uri \"%s\"", str);
	return -1;
}

/**
 * Initialize the module. By default, the module is turned off and
 * does nothing. To start SWIM swim_cfg is used.
 */
static int
swim_init()
{
	members = mh_i64ptr_new();
	if (members == NULL) {
		diag_set(OutOfMemory, sizeof(*members), "malloc",
			 "members");
		return -1;
	}
	ev_init(&input, swim_on_input);
	ev_init(&output, swim_on_output);
	ev_init(&round_tick, swim_trigger_round_step);
	ev_init(&wait_ack_tick, swim_check_acks);
	ev_periodic_set(&round_tick, 0, HEARTBEAT_RATE_DEFAULT, NULL);
	ev_periodic_set(&wait_ack_tick, 0, ACK_TIMEOUT, NULL);
	return 0;
}

int
swim_cfg(const char **member_uris, int member_uri_count, const char *server_uri,
	 double heartbeat_rate)
{
	if (members == NULL && swim_init() != 0)
		return -1;
	struct sockaddr_in addr;
	struct swim_member **new_cfg;
	struct swim_member *new_self = self;
	enum swim_member_status new_status = swim_member_status_MAX;
	if (member_uri_count > 0) {
		int size = sizeof(new_cfg[0]) * member_uri_count;
		new_cfg =  (struct swim_member **) malloc(size);
		if (new_cfg == NULL) {
			diag_set(OutOfMemory, size, "malloc", "new_cfg");
			return -1;
		}
	}
	int new_cfg_size = 0;
	for (; new_cfg_size < member_uri_count; ++new_cfg_size) {
		if (uri_to_addr(member_uris[new_cfg_size], &addr) != 0)
			goto error;
		uint64_t hash = sockaddr_in_hash(&addr);
		struct swim_member *member = swim_find_member(hash);
		if (member == NULL) {
			member = swim_member_new(&addr, new_status, 0);
			if (member == NULL)
				goto error;
		}
		new_cfg[new_cfg_size] = member;
	}

	if (server_uri != NULL) {
		if (uri_to_addr(server_uri, &addr) != 0)
			goto error;
		socklen_t addrlen;
		struct sockaddr_in cur_addr;

		if (!ev_is_active(&input) ||
		    getsockname(input.fd, (struct sockaddr *) &cur_addr,
				&addrlen) != 0 ||
		    addr.sin_addr.s_addr != cur_addr.sin_addr.s_addr ||
		    addr.sin_port != cur_addr.sin_port) {
			int fd = sio_socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
			if (fd == -1)
				goto error;
			if (sio_bind(fd, (struct sockaddr *) &addr,
				     sizeof(addr)) != 0) {
				close(fd);
				goto error;
			}
			struct ev_loop *loop = loop();
			ev_io_set(&input, fd, EV_READ);
			ev_io_set(&output, fd, EV_WRITE);
			ev_io_start(loop, &input);
			ev_periodic_start(loop, &round_tick);
			ev_periodic_start(loop, &wait_ack_tick);

			uint64_t self_hash = sockaddr_in_hash(&addr);
			new_self = swim_find_member(self_hash);
			if (new_self == NULL) {
				new_self = swim_member_new(&addr, new_status,
							   0);
				if (new_self == NULL)
					goto error;
			}
		}
	}

	if (round_tick.interval != heartbeat_rate && heartbeat_rate > 0)
		ev_periodic_set(&round_tick, 0, heartbeat_rate, NULL);

	if (member_uri_count > 0) {
		for (int i = 0; i < cfg_size; ++i)
			cfg[i]->is_pinned = false;
		free(cfg);
		for (int i = 0; i < new_cfg_size; ++i) {
			new_cfg[i]->is_pinned = true;
			new_cfg[i]->status = MEMBER_ALIVE;
			swim_schedule_event(new_cfg[i]);
		}
		cfg = new_cfg;
		cfg_size = new_cfg_size;
	}
	if (new_self->status == new_status)
		new_self->status = MEMBER_ALIVE;
	self = new_self;
	return 0;

error:
	for (int i = 0; i < new_cfg_size; ++i) {
		if (new_cfg[i]->status == new_status) {
			swim_member_delete(new_cfg[i]);
			if (new_self == new_cfg[i])
				new_self = NULL;
		}
	}
	if (member_uri_count > 0)
		free(new_cfg);
	if (new_self != NULL && new_self->status == new_status)
		swim_member_delete(new_self);
	return -1;
}

void
swim_info(struct info_handler *info)
{
	info_begin(info);
	if (members == NULL)
		return;
	for (mh_int_t node = mh_first(members), end = mh_end(members);
	     node != end; node = mh_next(members, node)) {
		struct swim_member *member = (struct swim_member *)
			mh_i64ptr_node(members, node)->val;
		info_table_begin(info,
				 sio_strfaddr((struct sockaddr *) &member->addr,
					      sizeof(member->addr)));
		info_append_str(info, "status",
				swim_member_status_strs[member->status]);
		info_append_uint(info, "incarnation", member->incarnation);
		info_table_end(info);
	}
	info_end(info);
}

void
swim_stop()
{
	if (members == NULL)
		return;
	struct ev_loop *loop = loop();
	ev_io_stop(loop, &output);
	ev_io_stop(loop, &input);
	close(input.fd);
	ev_periodic_stop(loop, &round_tick);
	ev_periodic_stop(loop, &wait_ack_tick);
	mh_int_t node = mh_first(members);
	while (node != mh_end(members)) {
		struct swim_member *m = (struct swim_member *)
			mh_i64ptr_node(members, node)->val;
		mh_i64ptr_del(members, node, NULL);
		rlist_del_entry(m, in_queue_events);
		swim_member_delete(m);
		node = mh_first(members);
	}
	mh_i64ptr_delete(members);
	free(shuffled_members);
	free(cfg);

	members = NULL;
	cfg = NULL;
	cfg_size = 0;
	shuffled_members = NULL;
	shuffled_members_size = 0;
	event_count = 0;
	rlist_create(&queue_wait_ack);
	rlist_create(&queue_output);
	rlist_create(&queue_round);
	rlist_create(&queue_events);
}
