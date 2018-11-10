#ifndef TARANTOOL_SQL_EXECUTE_H_INCLUDED
#define TARANTOOL_SQL_EXECUTE_H_INCLUDED
/*
 * Copyright 2010-2017, Tarantool AUTHORS, please see AUTHORS file.
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

#include <stdint.h>
#include <stdbool.h>
#include "port.h"
#include "mpstream.h"

#if defined(__cplusplus)
extern "C" {
#endif

/** Keys of IPROTO_SQL_INFO map. */
enum sql_info_key {
	SQL_INFO_ROW_COUNT = 0,
	SQL_INFO_AUTOINCREMENT_IDS = 1,
	sql_info_key_MAX,
};

extern const char *sql_info_key_strs[];

struct obuf;
struct region;
struct sql_bind;
struct xrow_header;

/** EXECUTE request. */
struct sql_request {
	uint64_t sync;
	/** SQL statement text. */
	const char *sql_text;
	/** Length of the SQL statement text. */
	uint32_t sql_text_len;
	/** Array of parameters. */
	struct sql_bind *bind;
	/** Length of the @bind. */
	uint32_t bind_count;
};

/** Response on EXECUTE request. */
struct sql_response {
	/** Request sync. */
	uint64_t sync;
	/** Port with response data if any. */
	struct port port;
	/** Prepared SQL statement with metadata. */
	void *prep_stmt;
};

/**
 * Dump a built response into @an out buffer. The response is
 * destroyed.
 * Response structure:
 * +----------------------------------------------+
 * | IPROTO_OK, sync, schema_version   ...        | iproto_header
 * +----------------------------------------------+---------------
 * | Body - a map with one or two keys.           |
 * |                                              |
 * | IPROTO_BODY: {                               |
 * |     IPROTO_METADATA: [                       |
 * |         {IPROTO_FIELD_NAME: column name1},   |
 * |         {IPROTO_FIELD_NAME: column name2},   | iproto_body
 * |         ...                                  |
 * |     ],                                       |
 * |                                              |
 * |     IPROTO_DATA: [                           |
 * |         tuple, tuple, tuple, ...             |
 * |     ]                                        |
 * | }                                            |
 * +-------------------- OR ----------------------+
 * | IPROTO_BODY: {                               |
 * |     IPROTO_SQL_INFO: {                       |
 * |         SQL_INFO_ROW_COUNT: number           |
 * |     }                                        |
 * | }                                            |
 * +----------------------------------------------+
 * @param response EXECUTE response.
 * @param[out] number of keys in map.
 * @param out Output buffer.
 *
 * @retval  0 Success.
 * @retval -1 Memory error.
 */
int
sql_response_dump(struct sql_response *response, int *keys,
		  struct mpstream *stream);

/**
 * Parse MessagePack array of SQL parameters and store a result
 * into the @request->bind, bind_count.
 * @param request Request to save decoded parameters.
 * @param data MessagePack array of parameters. Each parameter
 *        either must have scalar type, or must be a map with the
 *        following format: {name: value}. Name - string name of
 *        the named parameter, value - scalar value of the
 *        parameter. Named and positioned parameters can be mixed.
 *        For more details
 *        @sa https://www.sqlite.org/lang_expr.html#varparam.
 * @param region Allocator.
 *
 * @retval  0 Success.
 * @retval -1 Client or memory error.
 */
int
sql_bind_list_decode(struct sql_request *request, const char *data,
		     struct region *region);

/**
 * Prepare and execute an SQL statement.
 * @param request IProto request.
 * @param[out] response Response to store result.
 * @param region Runtime allocator for temporary objects
 *        (columns, tuples ...).
 *
 * @retval  0 Success.
 * @retval -1 Client or memory error.
 */
int
sql_prepare_and_execute(const struct sql_request *request,
			struct sql_response *response, struct region *region);

#if defined(__cplusplus)
} /* extern "C" { */
#endif

#endif /* TARANTOOL_SQL_EXECUTE_H_INCLUDED */
