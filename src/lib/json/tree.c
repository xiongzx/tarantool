
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

#include <assert.h>
#include <ctype.h>
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "small/rlist.h"
#include "trivia/util.h"
#include "tree.h"
#include "path.h"
#include "third_party/PMurHash.h"


/**
 * Hash table: json_path_node => json_tree_entry.
 */
struct mh_json_tree_entry {
	struct json_path_node key;
	uint32_t key_hash;
	struct json_tree_entry *parent;
	struct json_tree_entry *node;
};

/**
 * Compare hash records of two json tree nodes. Return 0 if equal.
 */
static inline int
mh_json_tree_entry_cmp(const struct mh_json_tree_entry *a,
		       const struct mh_json_tree_entry *b)
{
	if (a->key.type != b->key.type)
		return a->key.type - b->key.type;
	if (a->parent != b->parent)
		return a->parent - b->parent;
	if (a->key.type == JSON_PATH_STR) {
		if (a->key.len != b->key.len)
			return a->key.len - b->key.len;
		return memcmp(a->key.str, b->key.str, a->key.len);
	} else if (a->key.type == JSON_PATH_NUM) {
		return a->key_hash - b->key_hash;
	}
	unreachable();
}

#define MH_SOURCE 1
#define mh_name _json_tree_entry
#define mh_key_t struct mh_json_tree_entry *
#define mh_node_t struct mh_json_tree_entry
#define mh_arg_t void *
#define mh_hash(a, arg) ((a)->key_hash)
#define mh_hash_key(a, arg) ((a)->key_hash)
#define mh_cmp(a, b, arg) (mh_json_tree_entry_cmp((a), (b)))
#define mh_cmp_key(a, b, arg) mh_cmp(a, b, arg)
#include "salad/mhash.h"

static const uint32_t json_path_fragment_hash_seed = 13U;

uint32_t
json_path_fragment_hash(struct json_path_node *key, uint32_t seed)
{
	uint32_t h = seed;
	uint32_t carry = 0;
	const void *data;
	uint32_t data_size;
	if (key->type == JSON_PATH_STR) {
		data = key->str;
		data_size = key->len;
	} else if (key->type == JSON_PATH_NUM) {
		data = &key->num;
		data_size = sizeof(key->num);
	} else {
		unreachable();
	}
	PMurHash32_Process(&h, &carry, data, data_size);
	return PMurHash32_Result(h, carry, data_size);
}

int
json_tree_create(struct json_tree *tree)
{
	memset(tree, 0, sizeof(struct json_tree));
	tree->root.rolling_hash = json_path_fragment_hash_seed;
	tree->root.key.type = JSON_PATH_END;
	tree->hash = mh_json_tree_entry_new();
	if (unlikely(tree->hash == NULL))
		return -1;
	return 0;
}

void
json_tree_destroy(struct json_tree *tree)
{
	assert(tree->hash != NULL);
	json_tree_node_destroy(&tree->root);
	mh_json_tree_entry_delete(tree->hash);
}

void
json_tree_node_create(struct json_tree_entry *node,
		      struct json_path_node *path_node)
{
	memset(node, 0, sizeof(struct json_tree_entry));
	node->key = *path_node;
}

void
json_tree_node_destroy(struct json_tree_entry *node)
{
	free(node->children);
}

struct json_tree_entry *
json_tree_lookup_by_path_node(struct json_tree *tree,
			      struct json_tree_entry *parent,
			      struct json_path_node *path_node,
			      uint32_t rolling_hash)
{
	assert(parent != NULL);
	assert(rolling_hash == json_path_fragment_hash(path_node,
						       parent->rolling_hash));
	struct mh_json_tree_entry info;
	info.key = *path_node;
	info.key_hash = rolling_hash;
	info.parent = parent;
	mh_int_t id = mh_json_tree_entry_find(tree->hash, &info, NULL);
	if (unlikely(id == mh_end(tree->hash)))
		return NULL;
	struct mh_json_tree_entry *ht_node =
		mh_json_tree_entry_node(tree->hash, id);
	assert(ht_node == NULL || ht_node->node != NULL);
	struct json_tree_entry *ret = ht_node != NULL ? ht_node->node : NULL;
	assert(ret == NULL || ret->parent == parent);
	return ret;
}


int
json_tree_add(struct json_tree *tree, struct json_tree_entry *parent,
	      struct json_tree_entry *node, uint32_t rolling_hash)
{
	assert(parent != NULL);
	assert(node->parent == NULL);
	assert(rolling_hash ==
	       json_path_fragment_hash(&node->key, parent->rolling_hash));
	assert(json_tree_lookup_by_path_node(tree, parent, &node->key,
					     rolling_hash) == NULL);
	uint32_t insert_idx = (node->key.type == JSON_PATH_NUM) ?
			      (uint32_t)node->key.num - 1 :
			      parent->child_count;
	if (insert_idx >= parent->child_count) {
		uint32_t new_size = insert_idx + 1;
		struct json_tree_entry **children =
			realloc(parent->children, new_size*sizeof(void *));
		if (unlikely(children == NULL))
			return -1;
		memset(children + parent->child_count, 0,
		       (new_size - parent->child_count)*sizeof(void *));
		parent->children = children;
		parent->child_count = new_size;
	}
	assert(parent->children[insert_idx] == NULL);
	parent->children[insert_idx] = node;
	node->sibling_idx = insert_idx;
	node->rolling_hash = rolling_hash;

	struct mh_json_tree_entry ht_node;
	ht_node.key = node->key;
	ht_node.key_hash = rolling_hash;
	ht_node.node = node;
	ht_node.parent = parent;
	mh_int_t rc = mh_json_tree_entry_put(tree->hash, &ht_node, NULL, NULL);
	if (unlikely(rc == mh_end(tree->hash))) {
		parent->children[insert_idx] = NULL;
		return -1;
	}
	node->parent = parent;
	assert(json_tree_lookup_by_path_node(tree, parent, &node->key,
					     rolling_hash) == node);
	return 0;
}

void
json_tree_remove(struct json_tree *tree, struct json_tree_entry *parent,
		 struct json_tree_entry *node, uint32_t rolling_hash)
{
	assert(parent != NULL);
	assert(node->parent == parent);
	assert(json_tree_lookup_by_path_node(tree, parent, &node->key,
					     rolling_hash) == node);
	struct json_tree_entry **child_slot = NULL;
	if (node->key.type == JSON_PATH_NUM) {
		child_slot = &parent->children[node->key.num - 1];
	} else {
		uint32_t idx = 0;
		while (idx < parent->child_count &&
		       parent->children[idx] != node)
			child_slot = &parent->children[idx++];
	}
	assert(child_slot != NULL && *child_slot == node);
	*child_slot = NULL;

	struct mh_json_tree_entry info;
	info.key = node->key;
	info.key_hash = rolling_hash;
	info.parent = parent;
	mh_int_t id = mh_json_tree_entry_find(tree->hash, &info, NULL);
	assert(id != mh_end(tree->hash));
	mh_json_tree_entry_del(tree->hash, id, NULL);
	assert(json_tree_lookup_by_path_node(tree, parent, &node->key,
					     rolling_hash) == NULL);
}

struct json_tree_entry *
json_tree_lookup(struct json_tree *tree, struct json_tree_entry *parent,
		 const char *path, uint32_t path_len)
{
	int rc;
	struct json_path_parser parser;
	struct json_path_node path_node;
	struct json_tree_entry *ret = parent;
	uint32_t rolling_hash = ret->rolling_hash;
	json_path_parser_create(&parser, path, path_len);
	while ((rc = json_path_next(&parser, &path_node)) == 0 &&
	       path_node.type != JSON_PATH_END && ret != NULL) {
		rolling_hash =
			json_path_fragment_hash(&path_node, rolling_hash);
		ret = json_tree_lookup_by_path_node(tree, ret, &path_node,
						    rolling_hash);
	}
	if (rc != 0 || path_node.type != JSON_PATH_END)
		return NULL;
	return ret;
}

static struct json_tree_entry *
json_tree_child_next(struct json_tree_entry *parent, struct json_tree_entry *pos)
{
	assert(pos == NULL || pos->parent == parent);
	struct json_tree_entry **arr = parent->children;
	uint32_t arr_size = parent->child_count;
	if (arr == NULL)
		return NULL;
	uint32_t idx = pos != NULL ? pos->sibling_idx + 1 : 0;
	while (idx < arr_size && arr[idx] == NULL)
		idx++;
	if (idx >= arr_size)
		return NULL;
	return arr[idx];
}

static struct json_tree_entry *
json_tree_leftmost(struct json_tree_entry *pos)
{
	struct json_tree_entry *last;
	do {
		last = pos;
		pos = json_tree_child_next(pos, NULL);
	} while (pos != NULL);
	return last;
}

struct json_tree_entry *
json_tree_preorder_next(struct json_tree_entry *parent,
			struct json_tree_entry *pos)
{
	if (pos == NULL)
		pos = parent;
	struct json_tree_entry *next = json_tree_child_next(pos, NULL);
	if (next != NULL)
		return next;
	while (pos != parent) {
		next = json_tree_child_next(pos->parent, pos);
		if (next != NULL)
			return next;
		pos = pos->parent;
	}
	return NULL;
}

struct json_tree_entry *
json_tree_postorder_next(struct json_tree_entry *parent,
			 struct json_tree_entry *pos)
{
	struct json_tree_entry *next;
	if (pos == NULL) {
		next = json_tree_leftmost(parent);
		return next != parent ? next : NULL;
	}
	if (pos == parent)
		return NULL;
	next = json_tree_child_next(pos->parent, pos);
	if (next != NULL) {
		next = json_tree_leftmost(next);
		return next != parent ? next : NULL;
	}
	return pos->parent != parent ? pos->parent : NULL;
}
