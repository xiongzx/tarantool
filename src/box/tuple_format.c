/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
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
#include "fiber.h"
#include "json/path.h"
#include "tuple_format.h"
#include "coll_id_cache.h"

/** Global table of tuple formats */
struct tuple_format **tuple_formats;
static intptr_t recycled_format_ids = FORMAT_ID_NIL;

static uint32_t formats_size = 0, formats_capacity = 0;

static struct tuple_field *
tuple_field_create(struct json_path_node *node)
{
	struct tuple_field *ret = calloc(1, sizeof(struct tuple_field));
	if (ret == NULL) {
		diag_set(OutOfMemory, sizeof(struct tuple_field), "malloc",
			 "ret");
		return NULL;
	}
	ret->type = FIELD_TYPE_ANY;
	ret->offset_slot = TUPLE_OFFSET_SLOT_NIL;
	ret->coll_id = COLL_NONE;
	ret->nullable_action = ON_CONFLICT_ACTION_NONE;
	json_tree_node_create(&ret->tree_entry, node);
	return ret;
}

static void
tuple_field_destroy(struct tuple_field *field)
{
	json_tree_node_destroy(&field->tree_entry);
	free(field);
}

/** Build a JSON tree path for specified path. */
static struct tuple_field *
tuple_field_tree_add_path(struct tuple_format *format, const char *path,
			  uint32_t path_len, uint32_t fieldno)
{
	int rc = 0;
	struct json_tree *tree = &format->field_tree;
	struct tuple_field *field = tuple_format_field(format, fieldno);
	enum field_type iterm_node_type = FIELD_TYPE_ANY;

	struct json_path_parser parser;
	struct json_path_node path_node;
	bool is_last_new = false;
	json_path_parser_create(&parser, path, path_len);
	while ((rc = json_path_next(&parser, &path_node)) == 0 &&
	       path_node.type != JSON_PATH_END) {
		iterm_node_type = path_node.type == JSON_PATH_STR ?
				  FIELD_TYPE_MAP : FIELD_TYPE_ARRAY;
		if (field->type != FIELD_TYPE_ANY &&
		    field->type != iterm_node_type)
			goto error_type_mistmatch;
		uint32_t rolling_hash =
			json_path_fragment_hash(&path_node,
						field->tree_entry.rolling_hash);
		struct tuple_field *next_field =
			json_tree_lookup_entry_by_path_node(tree,
							    &field->tree_entry,
							    &path_node,
							    rolling_hash,
							    struct tuple_field,
							    tree_entry);
		if (next_field == NULL) {
			next_field = tuple_field_create(&path_node);
			rc = json_tree_add(tree, &field->tree_entry,
					   &next_field->tree_entry,
					   rolling_hash);
			if (rc != 0) {
				diag_set(OutOfMemory,
					 sizeof(struct json_tree_entry),
					 "json_tree_add", "hashtable");
				return NULL;
			}
			is_last_new = true;
		} else {
			is_last_new = false;
		}
		field->type = iterm_node_type;
		field = next_field;
	}
	/*
	 * Key parts path is already checked and normalized,
	 * so we don't need to handle parse error.
	 */
	assert(rc == 0 && path_node.type == JSON_PATH_END);
	assert(field != NULL);
	if (is_last_new) {
		uint32_t depth = 1;
		for (struct json_tree_entry *iter = field->tree_entry.parent;
		     iter != &format->field_tree.root;
		     iter = iter->parent, ++depth) {
			struct tuple_field *record =
				json_tree_entry_container(iter,
							  struct tuple_field,
							  tree_entry);
			record->subtree_depth =
				MAX(record->subtree_depth, depth);
		}
	}
	return field;

error_type_mistmatch: ;
	const char *name = tt_sprintf("[%d]%.*s", fieldno, path_len, path);
	diag_set(ClientError, ER_INDEX_PART_TYPE_MISMATCH, name,
		 field_type_strs[field->type],
		 field_type_strs[iterm_node_type]);
	return NULL;
}

static int
tuple_format_use_key_part(struct tuple_format *format,
			  const struct field_def *fields, uint32_t field_count,
			  const struct key_part *part, bool is_sequential,
			  int *current_slot, char **path_data)
{
	assert(part->fieldno < tuple_format_field_count(format));
	struct tuple_field *field = tuple_format_field(format, part->fieldno);
	if (unlikely(part->path != NULL)) {
		assert(!is_sequential);
		/**
		 * Copy JSON path data to reserved area at the
		 * end of format allocation.
		 */
		memcpy(*path_data, part->path, part->path_len);
		(*path_data)[part->path_len] = '\0';
		struct tuple_field *root = field;
		field = tuple_field_tree_add_path(format, *path_data,
						  part->path_len,
						  part->fieldno);
		if (field == NULL)
			return -1;
		format->subtree_depth =
			MAX(format->subtree_depth, root->subtree_depth + 1);
		field->is_key_part = true;
		*path_data += part->path_len + 1;
	}
	/*
		* If a field is not present in the space format,
		* inherit nullable action of the first key part
		* referencing it.
		*/
	if (part->fieldno >= field_count && !field->is_key_part)
		field->nullable_action = part->nullable_action;
	/*
	 * Field and part nullable actions may differ only
	 * if one of them is DEFAULT, in which case we use
	 * the non-default action *except* the case when
	 * the other one is NONE, in which case we assume
	 * DEFAULT. The latter is needed so that in case
	 * index definition and space format have different
	 * is_nullable flag, we will use the strictest option,
	 * i.e. DEFAULT.
	 */
	if (field->nullable_action == ON_CONFLICT_ACTION_DEFAULT) {
		if (part->nullable_action != ON_CONFLICT_ACTION_NONE)
			field->nullable_action = part->nullable_action;
	} else if (part->nullable_action == ON_CONFLICT_ACTION_DEFAULT) {
		if (field->nullable_action == ON_CONFLICT_ACTION_NONE)
			field->nullable_action = part->nullable_action;
	} else if (field->nullable_action != part->nullable_action) {
		diag_set(ClientError, ER_ACTION_MISMATCH,
				part->fieldno + TUPLE_INDEX_BASE,
				on_conflict_action_strs[field->nullable_action],
				on_conflict_action_strs[part->nullable_action]);
		return -1;
	}

	/**
	 * Check that there are no conflicts between index part
	 * types and space fields. If a part type is compatible
	 * with field's one, then the part type is more strict
	 * and the part type must be used in tuple_format.
	 */
	if (field_type1_contains_type2(field->type,
					part->type)) {
		field->type = part->type;
	} else if (!field_type1_contains_type2(part->type,
					       field->type)) {
		const char *name;
		int fieldno = part->fieldno + TUPLE_INDEX_BASE;
		if (unlikely(part->path != NULL)) {
			name = tt_sprintf("[%d]%.*s", fieldno, part->path_len,
					  part->path);
		} else if (part->fieldno >= field_count) {
			name = tt_sprintf("%d", fieldno);
		} else {
			const struct field_def *def =
				&fields[part->fieldno];
			name = tt_sprintf("'%s'", def->name);
		}
		int errcode;
		if (!field->is_key_part)
			errcode = ER_FORMAT_MISMATCH_INDEX_PART;
		else
			errcode = ER_INDEX_PART_TYPE_MISMATCH;
		diag_set(ClientError, errcode, name,
			 field_type_strs[field->type],
			 field_type_strs[part->type]);
		return -1;
	}
	field->is_key_part = true;
	/*
	 * In the tuple, store only offsets necessary to access
	 * fields of non-sequential keys. First field is always
	 * simply accessible, so we don't store an offset for it.
	 */
	if (field->offset_slot == TUPLE_OFFSET_SLOT_NIL &&
	    is_sequential == false &&
	    (part->fieldno > 0 || part->path != NULL))
		field->offset_slot = (*current_slot = *current_slot - 1);
	return 0;
}

/**
 * Extract all available type info from keys and field
 * definitions.
 */
static int
tuple_format_create(struct tuple_format *format, struct key_def * const *keys,
		    uint16_t key_count, const struct field_def *fields,
		    uint32_t field_count)
{
	format->min_field_count =
		tuple_format_min_field_count(keys, key_count, fields,
					     field_count);
	if (tuple_format_field_count(format) == 0) {
		format->field_map_size = 0;
		return 0;
	}
	/* Initialize defined fields */
	for (uint32_t i = 0; i < field_count; ++i) {
		struct tuple_field *field = tuple_format_field(format, i);
		field->type = fields[i].type;
		field->nullable_action = fields[i].nullable_action;
		struct coll *coll = NULL;
		uint32_t cid = fields[i].coll_id;
		if (cid != COLL_NONE) {
			struct coll_id *coll_id = coll_by_id(cid);
			if (coll_id == NULL) {
				diag_set(ClientError,ER_WRONG_COLLATION_OPTIONS,
					 i + 1, "collation was not found by ID");
				return -1;
			}
			coll = coll_id->coll;
		}
		field->coll = coll;
		field->coll_id = cid;
	}

	int current_slot = 0;
	char *paths_data = (char *)format + sizeof(struct tuple_format);
	/* extract field type info */
	for (uint16_t key_no = 0; key_no < key_count; ++key_no) {
		const struct key_def *key_def = keys[key_no];
		bool is_sequential = key_def_is_sequential(key_def);
		const struct key_part *part = key_def->parts;
		const struct key_part *parts_end = part + key_def->part_count;

		for (; part < parts_end; part++) {
			if (tuple_format_use_key_part(format, fields,
						      field_count, part,
						      is_sequential,
						      &current_slot,
						      &paths_data) != 0)
				return -1;
		}
	}

	assert(tuple_format_field(format, 0)->offset_slot ==
		TUPLE_OFFSET_SLOT_NIL);
	size_t field_map_size = -current_slot * sizeof(uint32_t);
	if (field_map_size > UINT16_MAX) {
		/** tuple->data_offset is 16 bits */
		diag_set(ClientError, ER_INDEX_FIELD_COUNT_LIMIT,
			 -current_slot);
		return -1;
	}
	format->field_map_size = field_map_size;
	return 0;
}

static int
tuple_format_register(struct tuple_format *format)
{
	if (recycled_format_ids != FORMAT_ID_NIL) {

		format->id = (uint16_t) recycled_format_ids;
		recycled_format_ids = (intptr_t) tuple_formats[recycled_format_ids];
	} else {
		if (formats_size == formats_capacity) {
			uint32_t new_capacity = formats_capacity ?
						formats_capacity * 2 : 16;
			struct tuple_format **formats;
			formats = (struct tuple_format **)
				realloc(tuple_formats, new_capacity *
						       sizeof(tuple_formats[0]));
			if (formats == NULL) {
				diag_set(OutOfMemory,
					 sizeof(struct tuple_format), "malloc",
					 "tuple_formats");
				return -1;
			}

			formats_capacity = new_capacity;
			tuple_formats = formats;
		}
		if (formats_size == FORMAT_ID_MAX + 1) {
			diag_set(ClientError, ER_TUPLE_FORMAT_LIMIT,
				 (unsigned) formats_capacity);
			return -1;
		}
		format->id = formats_size++;
	}
	tuple_formats[format->id] = format;
	return 0;
}

static void
tuple_format_deregister(struct tuple_format *format)
{
	if (format->id == FORMAT_ID_NIL)
		return;
	tuple_formats[format->id] = (struct tuple_format *) recycled_format_ids;
	recycled_format_ids = format->id;
	format->id = FORMAT_ID_NIL;
}

static struct tuple_format *
tuple_format_alloc(struct key_def * const *keys, uint16_t key_count,
		   uint32_t space_field_count, struct tuple_dictionary *dict)
{
	/* Size of area to store paths. */
	uint32_t paths_size = 0;
	uint32_t index_field_count = 0;
	/* find max max field no */
	for (uint16_t key_no = 0; key_no < key_count; ++key_no) {
		const struct key_def *key_def = keys[key_no];
		const struct key_part *part = key_def->parts;
		const struct key_part *pend = part + key_def->part_count;
		for (; part < pend; part++) {
			index_field_count = MAX(index_field_count,
						part->fieldno + 1);
			if (part->path != NULL)
				paths_size += part->path_len + 1;
		}
	}
	uint32_t field_count = MAX(space_field_count, index_field_count);

	uint32_t allocation_size = sizeof(struct tuple_format) + paths_size;
	struct tuple_format *format = malloc(allocation_size);
	if (format == NULL) {
		diag_set(OutOfMemory, allocation_size, "malloc",
			 "tuple format");
		return NULL;
	}
	if (json_tree_create(&format->field_tree) != 0) {
		free(format);
		return NULL;
	}
	format->subtree_depth = 1;
	struct json_path_node path_node;
	memset(&path_node, 0, sizeof(path_node));
	path_node.type = JSON_PATH_NUM;
	for (int32_t i = field_count - 1; i >= 0; i--) {
		path_node.num = i + TUPLE_INDEX_BASE;
		struct tuple_field *field = tuple_field_create(&path_node);
		if (field == NULL)
			goto error;
		uint32_t rolling_hash = format->field_tree.root.rolling_hash;
		rolling_hash =
			json_path_fragment_hash(&path_node, rolling_hash);
		if (json_tree_add(&format->field_tree, &format->field_tree.root,
				  &field->tree_entry, rolling_hash) != 0) {
			tuple_field_destroy(field);
			goto error;
		}
	}
	if (dict == NULL) {
		assert(space_field_count == 0);
		format->dict = tuple_dictionary_new(NULL, 0);
		if (format->dict == NULL)
			goto error;
	} else {
		format->dict = dict;
		tuple_dictionary_ref(dict);
	}
	format->allocation_size = allocation_size;
	format->refs = 0;
	format->id = FORMAT_ID_NIL;
	format->index_field_count = index_field_count;
	format->exact_field_count = 0;
	format->min_field_count = 0;
	return format;
error:;
	struct tuple_field *field;
	json_tree_foreach_entry_safe(field, &format->field_tree.root,
				     struct tuple_field, tree_entry)
		tuple_field_destroy(field);
	json_tree_destroy(&format->field_tree);
	free(format);
	return NULL;
}

/** Free tuple format resources, doesn't unregister. */
static inline void
tuple_format_destroy(struct tuple_format *format)
{
	struct tuple_field *field;
	json_tree_foreach_entry_safe(field, &format->field_tree.root,
				     struct tuple_field, tree_entry)
		tuple_field_destroy(field);
	json_tree_destroy(&format->field_tree);
	tuple_dictionary_unref(format->dict);
}

void
tuple_format_delete(struct tuple_format *format)
{
	tuple_format_deregister(format);
	tuple_format_destroy(format);
	free(format);
}

struct tuple_format *
tuple_format_new(struct tuple_format_vtab *vtab, struct key_def * const *keys,
		 uint16_t key_count, const struct field_def *space_fields,
		 uint32_t space_field_count, struct tuple_dictionary *dict)
{
	struct tuple_format *format =
		tuple_format_alloc(keys, key_count, space_field_count, dict);
	if (format == NULL)
		return NULL;
	format->vtab = *vtab;
	format->engine = NULL;
	format->is_temporary = false;
	if (tuple_format_register(format) < 0) {
		tuple_format_destroy(format);
		free(format);
		return NULL;
	}
	if (tuple_format_create(format, keys, key_count, space_fields,
				space_field_count) < 0) {
		tuple_format_delete(format);
		return NULL;
	}
	return format;
}

bool
tuple_format1_can_store_format2_tuples(struct tuple_format *format1,
				       struct tuple_format *format2)
{
	if (format1->exact_field_count != format2->exact_field_count)
		return false;
	uint32_t format1_field_count = tuple_format_field_count(format1);
	uint32_t format2_field_count = tuple_format_field_count(format2);
	for (uint32_t i = 0; i < format1_field_count; ++i) {
		const struct tuple_field *field1 =
			tuple_format_field(format1, i);
		/*
		 * The field has a data type in format1, but has
		 * no data type in format2.
		 */
		if (i >= format2_field_count) {
			/*
			 * The field can get a name added
			 * for it, and this doesn't require a data
			 * check.
			 * If the field is defined as not
			 * nullable, however, we need a data
			 * check, since old data may contain
			 * NULLs or miss the subject field.
			 */
			if (field1->type == FIELD_TYPE_ANY &&
			    tuple_field_is_nullable(field1))
				continue;
			else
				return false;
		}
		const struct tuple_field *field2 =
			tuple_format_field(format2, i);
		if (! field_type1_contains_type2(field1->type, field2->type))
			return false;
		/*
		 * Do not allow transition from nullable to non-nullable:
		 * it would require a check of all data in the space.
		 */
		if (tuple_field_is_nullable(field2) &&
		    !tuple_field_is_nullable(field1))
			return false;
	}
	return true;
}

/** Find a field in format by offset slot. */
static struct tuple_field *
tuple_field_by_offset_slot(const struct tuple_format *format,
			   int32_t offset_slot)
{
	struct tuple_field *field;
	struct json_tree_entry *root =
		(struct json_tree_entry *)&format->field_tree.root;
	json_tree_foreach_entry_preorder(field, root, struct tuple_field,
					 tree_entry) {
		if (field->offset_slot == offset_slot)
			return field;
	}
	return NULL;
}

/**
 * Verify field_map and raise error on some indexed field has
 * not been initialized. Routine rely on field_map has been
 * initialized with UINT32_MAX marker before field_map
 * initialization.
 */
static int
tuple_field_map_validate(const struct tuple_format *format, uint32_t *field_map)
{
	struct json_tree_entry *tree_node =
		(struct json_tree_entry *)&format->field_tree.root;
	/* Lookup for absent not-nullable fields. */
	int32_t field_map_items =
		(int32_t)(format->field_map_size/sizeof(field_map[0]));
	for (int32_t i = -1; i >= -field_map_items; i--) {
		if (field_map[i] != UINT32_MAX)
			continue;

		struct tuple_field *field =
			tuple_field_by_offset_slot(format, i);
		assert(field != NULL);
		/* Lookup for field number in tree. */
		struct json_tree_entry *parent = &field->tree_entry;
		while (parent->parent != &format->field_tree.root)
			parent = parent->parent;
		assert(parent->key.type == JSON_PATH_NUM);
		uint32_t fieldno = parent->key.num;

		tree_node = &field->tree_entry;
		const char *err_msg;
		if (field->tree_entry.key.type == JSON_PATH_STR) {
			err_msg = tt_sprintf("invalid field %d document "
					     "content: map doesn't contain a "
					     "key '%.*s' defined in index",
					     fieldno, tree_node->key.len,
					     tree_node->key.str);
		} else if (field->tree_entry.key.type == JSON_PATH_NUM) {
			err_msg = tt_sprintf("invalid field %d document "
					     "content: array size %d is less "
					     "than size %d defined in index",
					     fieldno, tree_node->key.num,
					     tree_node->parent->child_count);
		}
		diag_set(ClientError, ER_DATA_STRUCTURE_MISMATCH, err_msg);
		return -1;
	}
	return 0;
}

struct parse_ctx {
	enum json_path_type child_type;
	uint32_t items;
	uint32_t curr;
};

/** @sa declaration for details. */
int
tuple_init_field_map(const struct tuple_format *format, uint32_t *field_map,
		     const char *tuple, bool validate)
{
	if (tuple_format_field_count(format) == 0)
		return 0; /* Nothing to initialize */

	const char *pos = tuple;

	/* Check to see if the tuple has a sufficient number of fields. */
	uint32_t field_count = mp_decode_array(&pos);
	if (validate && format->exact_field_count > 0 &&
	    format->exact_field_count != field_count) {
		diag_set(ClientError, ER_EXACT_FIELD_COUNT,
			 (unsigned) field_count,
			 (unsigned) format->exact_field_count);
		return -1;
	}
	if (validate && field_count < format->min_field_count) {
		diag_set(ClientError, ER_MIN_FIELD_COUNT,
			 (unsigned) field_count,
			 (unsigned) format->min_field_count);
		return -1;
	}
	uint32_t defined_field_count = MIN(field_count, validate ?
					   tuple_format_field_count(format) :
					   format->index_field_count);
	/*
	 * Fill field_map with marker for toutine
	 * tuple_field_map_validate to detect absent fields.
	 */
	memset((char *)field_map - format->field_map_size,
		validate ? UINT32_MAX : 0, format->field_map_size);

	struct region *region = &fiber()->gc;
	uint32_t mp_stack_items = format->subtree_depth + 1;
	uint32_t mp_stack_size = mp_stack_items * sizeof(struct parse_ctx);
	struct parse_ctx *mp_stack = region_alloc(region, mp_stack_size);
	if (unlikely(mp_stack == NULL)) {
		diag_set(OutOfMemory, mp_stack_size, "region_alloc",
			 "mp_stack");
		return -1;
	}
	mp_stack[0] = (struct parse_ctx){
		.child_type = JSON_PATH_NUM,
		.items = defined_field_count,
		.curr = 0,
	};
	uint32_t mp_stack_idx = 0;
	struct json_tree *tree = (struct json_tree *)&format->field_tree;
	struct json_tree_entry *parent = &tree->root;
	while (mp_stack[0].curr <= mp_stack[0].items) {
		/* Prepare key for tree lookup. */
		struct json_path_node node;
		node.type = mp_stack[mp_stack_idx].child_type;
		++mp_stack[mp_stack_idx].curr;
		if (node.type == JSON_PATH_NUM) {
			node.num = mp_stack[mp_stack_idx].curr;
		} else if (node.type == JSON_PATH_STR) {
			if (mp_typeof(*pos) != MP_STR) {
				/*
				 * We do not support non-string
				 * keys in maps.
				 */
				mp_next(&pos);
				mp_next(&pos);
				continue;
			}
			node.str = mp_decode_str(&pos, (uint32_t *)&node.len);
		} else {
			unreachable();
		}
		uint32_t rolling_hash =
			json_path_fragment_hash(&node, parent->rolling_hash);
		struct tuple_field *field =
			json_tree_lookup_entry_by_path_node(tree, parent, &node,
							    rolling_hash,
							    struct tuple_field,
							    tree_entry);
		enum mp_type type = mp_typeof(*pos);
		if (field != NULL) {
			bool is_nullable = tuple_field_is_nullable(field);
			if (validate &&
			    key_mp_type_validate(field->type, type,
						 ER_FIELD_TYPE,
						 mp_stack[0].curr,
						 is_nullable) != 0)
				return -1;
			if (field->offset_slot != TUPLE_OFFSET_SLOT_NIL) {
				field_map[field->offset_slot] =
					(uint32_t)(pos - tuple);
			}
		}
		/* Prepare stack info for next iteration. */
		if (field != NULL && type == MP_ARRAY &&
		    mp_stack_idx + 1 < format->subtree_depth) {
			uint32_t size = mp_decode_array(&pos);
			if (unlikely(size == 0))
				continue;
			parent = &field->tree_entry;
			mp_stack[++mp_stack_idx] = (struct parse_ctx){
				.child_type = JSON_PATH_NUM,
				.items = size,
				.curr = 0,
			};
		} else if (field != NULL && type == MP_MAP &&
			   mp_stack_idx + 1 < format->subtree_depth) {
			uint32_t size = mp_decode_map(&pos);
			if (unlikely(size == 0))
				continue;
			parent = &field->tree_entry;
			mp_stack[++mp_stack_idx] = (struct parse_ctx){
				.child_type = JSON_PATH_STR,
				.items = size,
				.curr = 0,
			};
		} else {
			mp_next(&pos);
			while (mp_stack[mp_stack_idx].curr >=
			       mp_stack[mp_stack_idx].items) {
				assert(parent != NULL);
				parent = parent->parent;
				if (mp_stack_idx-- == 0)
					goto end;
			}
		}
	};
end:;
	/* Nullify absent nullable fields in field_map. */
	struct tuple_field *field;
	struct json_tree_entry *tree_node =
		(struct json_tree_entry *)&format->field_tree.root;
	/*
	 * Field map has already been initialized with zeros when
	 * no validation is required.
	 */
	if (!validate)
		return 0;
	json_tree_foreach_entry_preorder(field, tree_node, struct tuple_field,
					 tree_entry) {
		if (field->offset_slot != TUPLE_OFFSET_SLOT_NIL &&
			tuple_field_is_nullable(field) &&
			field_map[field->offset_slot] == UINT32_MAX)
			field_map[field->offset_slot] = 0;
	}
	return tuple_field_map_validate(format, field_map);
}

uint32_t
tuple_format_min_field_count(struct key_def * const *keys, uint16_t key_count,
			     const struct field_def *space_fields,
			     uint32_t space_field_count)
{
	uint32_t min_field_count = 0;
	for (uint32_t i = 0; i < space_field_count; ++i) {
		if (! space_fields[i].is_nullable)
			min_field_count = i + 1;
	}
	for (uint32_t i = 0; i < key_count; ++i) {
		const struct key_def *kd = keys[i];
		for (uint32_t j = 0; j < kd->part_count; ++j) {
			const struct key_part *kp = &kd->parts[j];
			if (!key_part_is_nullable(kp) &&
			    kp->fieldno + 1 > min_field_count)
				min_field_count = kp->fieldno + 1;
		}
	}
	return min_field_count;
}

/** Destroy tuple format subsystem and free resourses */
void
tuple_format_free()
{
	/* Clear recycled ids. */
	while (recycled_format_ids != FORMAT_ID_NIL) {
		uint16_t id = (uint16_t) recycled_format_ids;
		recycled_format_ids = (intptr_t) tuple_formats[id];
		tuple_formats[id] = NULL;
	}
	for (struct tuple_format **format = tuple_formats;
	     format < tuple_formats + formats_size; format++) {
		/* Do not unregister. Only free resources. */
		if (*format != NULL) {
			tuple_format_destroy(*format);
			free(*format);
		}
	}
	free(tuple_formats);
}

void
box_tuple_format_ref(box_tuple_format_t *format)
{
	tuple_format_ref(format);
}

void
box_tuple_format_unref(box_tuple_format_t *format)
{
	tuple_format_unref(format);
}

/**
 * Propagate @a field to MessagePack(field)[index].
 * @param[in][out] field Field to propagate.
 * @param index 1-based index to propagate to.
 *
 * @retval  0 Success, the index was found.
 * @retval -1 Not found.
 */
static inline int
tuple_field_go_to_index(const char **field, uint64_t index)
{
	enum mp_type type = mp_typeof(**field);
	if (type == MP_ARRAY) {
		if (index == 0)
			return -1;
		/* Make index 0-based. */
		index -= TUPLE_INDEX_BASE;
		uint32_t count = mp_decode_array(field);
		if (index >= count)
			return -1;
		for (; index > 0; --index)
			mp_next(field);
		return 0;
	} else if (type == MP_MAP) {
		uint64_t count = mp_decode_map(field);
		for (; count > 0; --count) {
			type = mp_typeof(**field);
			if (type == MP_UINT) {
				uint64_t value = mp_decode_uint(field);
				if (value == index)
					return 0;
			} else if (type == MP_INT) {
				int64_t value = mp_decode_int(field);
				if (value >= 0 && (uint64_t)value == index)
					return 0;
			} else {
				/* Skip key. */
				mp_next(field);
			}
			/* Skip value. */
			mp_next(field);
		}
	}
	return -1;
}

/**
 * Propagate @a field to MessagePack(field)[key].
 * @param[in][out] field Field to propagate.
 * @param key Key to propagate to.
 * @param len Length of @a key.
 *
 * @retval  0 Success, the index was found.
 * @retval -1 Not found.
 */
static inline int
tuple_field_go_to_key(const char **field, const char *key, int len)
{
	enum mp_type type = mp_typeof(**field);
	if (type != MP_MAP)
		return -1;
	uint64_t count = mp_decode_map(field);
	for (; count > 0; --count) {
		type = mp_typeof(**field);
		if (type == MP_STR) {
			uint32_t value_len;
			const char *value = mp_decode_str(field, &value_len);
			if (value_len == (uint)len &&
			    memcmp(value, key, len) == 0)
				return 0;
		} else {
			/* Skip key. */
			mp_next(field);
		}
		/* Skip value. */
		mp_next(field);
	}
	return -1;
}

int
tuple_field_go_to_path(const char **data, const char *path, uint32_t path_len)
{
	int rc;
	struct json_path_parser parser;
	struct json_path_node node;
	json_path_parser_create(&parser, path, path_len);
	while ((rc = json_path_next(&parser, &node)) == 0) {
		switch (node.type) {
		case JSON_PATH_NUM:
			rc = tuple_field_go_to_index(data, node.num);
			break;
		case JSON_PATH_STR:
			rc = tuple_field_go_to_key(data, node.str, node.len);
			break;
		default:
			assert(node.type == JSON_PATH_END);
			return 0;
		}
		if (rc != 0) {
			*data = NULL;
			return 0;
		}
	}
	return rc;
}

int
tuple_field_raw_by_path(struct tuple_format *format, const char *tuple,
                        const uint32_t *field_map, const char *path,
                        uint32_t path_len, uint32_t path_hash,
                        const char **field)
{
	assert(path_len > 0);
	uint32_t fieldno;
	/*
	 * It is possible, that a field has a name as
	 * well-formatted JSON. For example 'a.b.c.d' or '[1]' can
	 * be field name. To save compatibility at first try to
	 * use the path as a field name.
	 */
	if (tuple_fieldno_by_name(format->dict, path, path_len, path_hash,
				  &fieldno) == 0) {
		*field = tuple_field_raw(format, tuple, field_map, fieldno);
		return 0;
	}
	struct json_path_parser parser;
	struct json_path_node node;
	json_path_parser_create(&parser, path, path_len);
	int rc = json_path_next(&parser, &node);
	if (rc != 0)
		goto error;
	switch(node.type) {
	case JSON_PATH_NUM: {
		int index = node.num;
		if (index == 0) {
			*field = NULL;
			return 0;
		}
		index -= TUPLE_INDEX_BASE;
		*field = tuple_field_raw(format, tuple, field_map, index);
		if (*field == NULL)
			return 0;
		break;
	}
	case JSON_PATH_STR: {
		/* First part of a path is a field name. */
		uint32_t name_hash;
		if (path_len == (uint32_t) node.len) {
			name_hash = path_hash;
		} else {
			/*
			 * If a string is "field....", then its
			 * precalculated juajit hash can not be
			 * used. A tuple dictionary hashes only
			 * name, not path.
			 */
			name_hash = field_name_hash(node.str, node.len);
		}
		*field = tuple_field_raw_by_name(format, tuple, field_map,
						 node.str, node.len, name_hash);
		if (*field == NULL)
			return 0;
		break;
	}
	default:
		assert(node.type == JSON_PATH_END);
		*field = NULL;
		return 0;
	}
	rc = tuple_field_go_to_path(field, path + parser.offset,
				    path_len - parser.offset);
	if (rc == 0)
		return 0;
	/* Setup absolute error position. */
	rc += parser.offset;

error:
	assert(rc > 0);
	diag_set(ClientError, ER_ILLEGAL_PARAMS,
		 tt_sprintf("error in path on position %d", rc));
	return -1;
}

const char *
tuple_field_by_part_raw(struct tuple_format *format, const char *data,
			const uint32_t *field_map, struct key_part *part)
{
	if (likely(part->path == NULL))
		return tuple_field_raw(format, data, field_map, part->fieldno);

	uint32_t field_count = tuple_format_field_count(format);
	struct tuple_field *root_field =
		likely(part->fieldno < field_count) ?
		tuple_format_field(format, part->fieldno) : NULL;
	struct tuple_field *field =
		unlikely(root_field == NULL) ? NULL:
		tuple_format_field_by_path(format, root_field, part->path,
					   part->path_len);
	if (unlikely(field == NULL)) {
		/*
		 * Legacy tuple having no field map for JSON
		 * index require full path parse.
		 */
		const char *field_raw =
			tuple_field_raw(format, data, field_map, part->fieldno);
		if (unlikely(field_raw == NULL))
			return NULL;
		if (tuple_field_go_to_path(&field_raw, part->path,
					   part->path_len) != 0)
			return NULL;
		return field_raw;
	}
	int32_t offset_slot = field->offset_slot;
	assert(offset_slot < 0);
	assert(-offset_slot * sizeof(uint32_t) <= format->field_map_size);
	if (unlikely(field_map[offset_slot] == 0))
		return NULL;
	return data + field_map[offset_slot];
}
