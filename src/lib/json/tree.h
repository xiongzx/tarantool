#ifndef TARANTOOL_JSON_TREE_H_INCLUDED
#define TARANTOOL_JSON_TREE_H_INCLUDED
/*
 * Copyright 2010-2018 Tarantool AUTHORS: please see AUTHORS file.
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
#include <stdbool.h>
#include <stdint.h>
#include "small/rlist.h"
#include "path.h"
#include <stdio.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C"
{
#endif

struct mh_json_tree_entry_t;

/**
 * JSON tree is a hierarchical data structure that organize JSON
 * nodes produced by parser. Key record point to source strings
 * memory.
 */
struct json_tree_entry {
	/** JSON path node produced by json_next_token. */
	struct json_path_node key;
	/**
	 * Rolling hash for node calculated with
	 * json_path_fragment_hash(key, parent).
	 */
	uint32_t rolling_hash;
	/** Array of child records. Match indexes. */
	struct json_tree_entry **children;
	/** Size of children array. */
	uint32_t child_count;
	/** Index of node in parent children array. */
	uint32_t sibling_idx;
	/** Pointer to parent node. */
	struct json_tree_entry *parent;
};

/** JSON tree object managing data relations. */
struct json_tree {
	/** JSON tree root node. */
	struct json_tree_entry root;
	/** Hashtable af all tree nodes. */
	struct mh_json_tree_entry_t *hash;
};

/** Create a JSON tree object to manage data relations. */
int
json_tree_create(struct json_tree *tree);

/**
 * Destroy JSON tree object. This routine doesn't  subtree so
 * should be called at the end of it's manual destruction.
 */
void
json_tree_destroy(struct json_tree *tree);

/** Compute the hash value of a JSON path component. */
uint32_t
json_path_fragment_hash(struct json_path_node *key, uint32_t seed);

/** Init a JSON tree node. */
void
json_tree_node_create(struct json_tree_entry *node,
		      struct json_path_node *path_node);

/** Destroy a JSON tree node. */
void
json_tree_node_destroy(struct json_tree_entry *node);

/**
 * Make child lookup in tree by path_node at position specified
 * with parent. Rolling hash should be calculated for path_node
 * and parent with json_path_fragment_hash.
 */
struct json_tree_entry *
json_tree_lookup_by_path_node(struct json_tree *tree,
			      struct json_tree_entry *parent,
			      struct json_path_node *path_node,
			      uint32_t rolling_hash);

/**
 * Append node to the given parent position in JSON tree. The
 * parent mustn't have a child with such content. Rolling
 * hash should be calculated for path_node and parent with
 * json_path_fragment_hash.
 */
int
json_tree_add(struct json_tree *tree, struct json_tree_entry *parent,
	      struct json_tree_entry *node, uint32_t rolling_hash);

/**
 * Delete a JSON tree node at the given parent position in JSON
 * tree. The parent node must have such child. Rolling hash should
 * be calculated for path_node and parent with
 * json_path_fragment_hash.
 */
void
json_tree_remove(struct json_tree *tree, struct json_tree_entry *parent,
		 struct json_tree_entry *node, uint32_t rolling_hash);

/** Make child lookup by path in JSON tree. */
struct json_tree_entry *
json_tree_lookup(struct json_tree *tree, struct json_tree_entry *parent,
		 const char *path, uint32_t path_len);

/** Make pre order traversal in JSON tree. */
struct json_tree_entry *
json_tree_preorder_next(struct json_tree_entry *parent,
			struct json_tree_entry *pos);

/** Make post order traversal in JSON tree. */
struct json_tree_entry *
json_tree_postorder_next(struct json_tree_entry *parent,
			 struct json_tree_entry *pos);

/** Return container entry by json_tree_entry item. */
#define json_tree_entry_container(item, type, member) ({		     \
	const typeof( ((type *)0)->member ) *__mptr = (item);		     \
	(type *)( (char *)__mptr - ((size_t) &((type *)0)->member) ); })

/**
 * Return container entry by json_tree_entry item or NULL if
 * item is NULL.
 */
#define json_tree_entry_container_safe(item, type, member) ({		     \
	(item) != NULL ? json_tree_entry_container((item), type, member) :   \
	NULL; })

/** Make lookup in tree by path and return entry. */
#define json_tree_lookup_entry(tree, parent, path, path_len, type, member)   \
({struct json_tree_entry *__tree_node =					     \
	json_tree_lookup((tree), (parent), path, path_len);		     \
 __tree_node != NULL ?							     \
 json_tree_entry_container(__tree_node, type, member) : NULL; })

/** Make lookup in tree by path_node and return entry. */
#define json_tree_lookup_entry_by_path_node(tree, parent, path_node,	     \
					    path_node_hash, type, member)    \
({struct json_tree_entry *__tree_node =					     \
	json_tree_lookup_by_path_node((tree), (parent), path_node,	     \
				      path_node_hash);			     \
 __tree_node != NULL ? json_tree_entry_container(__tree_node, type, member) :\
		       NULL; })

/** Make pre-order traversal in JSON tree. */
#define json_tree_foreach_preorder(item, root)				     \
for ((item) = json_tree_preorder_next((root), NULL); (item) != NULL;	     \
     (item) = json_tree_preorder_next((root), (item)))

/** Make post-order traversal in JSON tree. */
#define json_tree_foreach_postorder(item, root)				     \
for ((item) = json_tree_postorder_next((root), NULL); (item) != NULL;	     \
     (item) = json_tree_postorder_next((root), (item)))

/**
 * Make safe post-order traversal in JSON tree.
 * Safe for building destructors.
 */
#define json_tree_foreach_safe(item, root)				     \
for (struct json_tree_entry *__iter = json_tree_postorder_next((root), NULL);\
     (((item) = __iter) &&						     \
     (__iter = json_tree_postorder_next((root), (item))), (item) != NULL);)

/** Make post-order traversal in JSON tree and return entry. */
#define json_tree_foreach_entry_preorder(item, root, type, member)	     \
for (struct json_tree_entry *__iter =					     \
     json_tree_preorder_next((root), NULL);				     \
     __iter != NULL &&							     \
     ((item) = json_tree_entry_container(__iter, type, member));	     \
     __iter = json_tree_preorder_next((root), __iter))

/** Make pre-order traversal in JSON tree and return entry. */
#define json_tree_foreach_entry_postorder(item, root, type, member)	     \
for (struct json_tree_entry *__iter =					     \
     json_tree_postorder_next((root), NULL); __iter != NULL &&		     \
     ((item) = json_tree_entry_container(__iter, type, member));	     \
     __iter = json_tree_postorder_next((root), __iter))

/**
 * Make secure post-order traversal in JSON tree and return entry.
 */
#define json_tree_foreach_entry_safe(item, root, type, member)		     \
for (struct json_tree_entry *__tmp_iter, *__iter =			     \
     json_tree_postorder_next((root), NULL);				     \
     __iter != NULL &&							     \
     ((item) = json_tree_entry_container(__iter, type, member)) &&	     \
     (__iter != NULL && (__tmp_iter =					     \
			 json_tree_postorder_next((root), __iter))),	     \
     (__iter != NULL &&							     \
     ((item) = json_tree_entry_container(__iter, type, member)));	     \
     __iter = __tmp_iter)

#ifdef __cplusplus
}
#endif

#endif /* TARANTOOL_JSON_TREE_H_INCLUDED */
