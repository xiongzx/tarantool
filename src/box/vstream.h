#ifndef TARANTOOL_VSTREAM_H_INCLUDED
#define TARANTOOL_VSTREAM_H_INCLUDED
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
 * THIS SOFTWARE IS PROVIDED BY AUTHORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "diag.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

struct vstream;
struct mpstream;
struct lua_State;
struct port;

enum vstream_constants {
	VSTREAM_SQL_DATA = 0,
	VSTREAM_SQL_METADATA,
	VSTREAM_SQL_INFO,
	VSTREAM_SQL_FIELD_NAME,
	VSTREAM_SQL_FIELD_TYPE,
};

struct vstream_vtab {
	void (*encode_array)(struct vstream *stream, uint32_t size);
	void (*encode_map)(struct vstream *stream, uint32_t size);
	void (*encode_uint)(struct vstream *stream, uint64_t num);
	void (*encode_int)(struct vstream *stream, int64_t num);
	void (*encode_float)(struct vstream *stream, float num);
	void (*encode_double)(struct vstream *stream, double num);
	void (*encode_strn)(struct vstream *stream, const char *str,
			    uint32_t len);
	void (*encode_nil)(struct vstream *stream);
	void (*encode_bool)(struct vstream *stream, bool val);
	uint64_t (*encode_enum)(struct vstream *stream,
			        enum vstream_constants constant);
	int (*encode_port)(struct vstream *stream, struct port *port);
	int (*encode_reply)(struct vstream *stream, uint32_t size,
			    enum vstream_constants constant);
	void (*encode_array_commit)(struct vstream *stream);
	void (*encode_reply_commit)(struct vstream *stream);
	void (*encode_map_commit)(struct vstream *stream);
	void (*encode_map_element_commit)(struct vstream *stream);
};

struct vstream {
	/** Virtual function table. */
	const struct vstream_vtab *vtab;
	/** TODO: Write comment. */
	union {
		struct mpstream *mpstream;
		struct lua_State *L;
	};
};

void
mp_vstream_init(struct vstream *vstream, struct mpstream *mpstream);

static inline void
vstream_encode_array(struct vstream *stream, uint32_t size)
{
	return stream->vtab->encode_array(stream, size);
}

static inline void
vstream_encode_map(struct vstream *stream, uint32_t size)
{
	return stream->vtab->encode_map(stream, size);
}

static inline void
vstream_encode_uint(struct vstream *stream, uint64_t num)
{
	return stream->vtab->encode_uint(stream, num);
}

static inline void
vstream_encode_int(struct vstream *stream, int64_t num)
{
	return stream->vtab->encode_int(stream, num);
}

static inline void
vstream_encode_float(struct vstream *stream, float num)
{
	return stream->vtab->encode_float(stream, num);
}

static inline void
vstream_encode_double(struct vstream *stream, double num)
{
	return stream->vtab->encode_double(stream, num);
}

static inline void
vstream_encode_strn(struct vstream *stream, const char *str, uint32_t len)
{
	return stream->vtab->encode_strn(stream, str, len);
}

static inline void
vstream_encode_str(struct vstream *stream, const char *str)
{
	return stream->vtab->encode_strn(stream, str, strlen(str));
}

static inline void
vstream_encode_nil(struct vstream *stream)
{
	return stream->vtab->encode_nil(stream);
}

static inline void
vstream_encode_bool(struct vstream *stream, bool val)
{
	return stream->vtab->encode_bool(stream, val);
}

static inline uint64_t
vstream_encode_enum(struct vstream *stream, enum vstream_constants constant)
{
	return stream->vtab->encode_enum(stream, constant);
}

static inline int
vstream_encode_port(struct vstream *stream, struct port *port)
{
	return stream->vtab->encode_port(stream, port);
}

static inline int
vstream_encode_reply(struct vstream *stream, uint32_t size,
		     enum vstream_constants constant)
{
	return stream->vtab->encode_reply(stream, size, constant);
}

static inline void
vstream_encode_array_commit(struct vstream *stream)
{
	return stream->vtab->encode_array_commit(stream);
}

static inline void
vstream_encode_reply_commit(struct vstream *stream)
{
	return stream->vtab->encode_reply_commit(stream);
}

static inline void
vstream_encode_map_commit(struct vstream *stream)
{
	return stream->vtab->encode_map_commit(stream);
}

static inline void
vstream_encode_map_element_commit(struct vstream *stream)
{
	return stream->vtab->encode_map_element_commit(stream);
}

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* TARANTOOL_VSTREAM_H_INCLUDED */
