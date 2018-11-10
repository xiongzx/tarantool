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

#include "vstream.h"
#include "mpstream.h"
#include "iproto_constants.h"
#include "port.h"
#include "xrow.h"

void
mp_vstream_encode_array(struct vstream *stream, uint32_t size)
{
	mpstream_encode_array(stream->mpstream, size);
}

void
mp_vstream_encode_map(struct vstream *stream, uint32_t size)
{
	mpstream_encode_map(stream->mpstream, size);
}

void
mp_vstream_encode_uint(struct vstream *stream, uint64_t num)
{
	mpstream_encode_uint(stream->mpstream, num);
}

void
mp_vstream_encode_int(struct vstream *stream, int64_t num)
{
	mpstream_encode_int(stream->mpstream, num);
}

void
mp_vstream_encode_float(struct vstream *stream, float num)
{
	mpstream_encode_float(stream->mpstream, num);
}

void
mp_vstream_encode_double(struct vstream *stream, double num)
{
	mpstream_encode_double(stream->mpstream, num);
}

void
mp_vstream_encode_strn(struct vstream *stream, const char *str, uint32_t len)
{
	mpstream_encode_strn(stream->mpstream, str, len);
}

void
mp_vstream_encode_nil(struct vstream *stream)
{
	mpstream_encode_nil(stream->mpstream);
}

void
mp_vstream_encode_bool(struct vstream *stream, bool val)
{
	mpstream_encode_bool(stream->mpstream, val);
}

int
mp_vstream_encode_port(struct vstream *stream, struct port *port)
{
	mpstream_flush(stream->mpstream);
	/*
	 * Just like SELECT, SQL uses output format compatible
	 * with Tarantool 1.6
	 */
	if (port_dump_msgpack_16(port, stream->mpstream->ctx) < 0) {
		/* Failed port dump destroyes the port. */
		return -1;
	}
	mpstream_reset(stream->mpstream);
	return 0;
}

int
mp_vstream_encode_reply(struct vstream *stream, uint32_t size,
			enum vstream_constants constant)
{
	uint8_t key;
	uint8_t type;
	switch(constant) {
		case VSTREAM_SQL_DATA:
			key = IPROTO_DATA;
			type = 0xdd;
			break;
		case VSTREAM_SQL_METADATA:
			key = IPROTO_METADATA;
			type = 0xdd;
			break;
		case VSTREAM_SQL_INFO:
			key = IPROTO_SQL_INFO;
			type = 0xdf;
			break;
		default:
			// TODO: Error;
			assert(0);
	}

	char *pos = mpstream_reserve(stream->mpstream, IPROTO_KEY_HEADER_LEN);
	if (pos == NULL) {
		diag_set(OutOfMemory, IPROTO_KEY_HEADER_LEN,
			 "mpstream_reserve", "pos");
		return -1;
	}
	pos = mp_store_u8(pos, key);
	pos = mp_store_u8(pos, type);
	pos = mp_store_u32(pos, size);
	mpstream_advance(stream->mpstream, IPROTO_KEY_HEADER_LEN);
	return 0;
}

uint64_t mp_vstream_encode_enum(struct vstream *stream,
			        enum vstream_constants constant)
{
	(void)stream;
	switch(constant) {
		case VSTREAM_SQL_DATA:
			return IPROTO_DATA;
		case VSTREAM_SQL_METADATA:
			return IPROTO_METADATA;
		case VSTREAM_SQL_INFO:
			return IPROTO_SQL_INFO;
		case VSTREAM_SQL_FIELD_NAME:
			return IPROTO_FIELD_NAME;
		case VSTREAM_SQL_FIELD_TYPE:
			return IPROTO_FIELD_TYPE;
		default:
			// TODO: Error;
			assert(0);
	}
}

void
mp_vstream_encode_array_commit(struct vstream *stream)
{
	(void)stream;
}

void
mp_vstream_encode_reply_commit(struct vstream *stream)
{
	(void)stream;
}

void
mp_vstream_encode_map_commit(struct vstream *stream)
{
	(void)stream;
}

void
mp_vstream_encode_map_element_commit(struct vstream *stream)
{
	(void)stream;
}

const struct vstream_vtab mp_vstream_vtab = {
	/** encode_array = */ mp_vstream_encode_array,
	/** encode_map = */ mp_vstream_encode_map,
	/** encode_uint = */ mp_vstream_encode_uint,
	/** encode_int = */ mp_vstream_encode_int,
	/** encode_float = */ mp_vstream_encode_float,
	/** encode_double = */ mp_vstream_encode_double,
	/** encode_strn = */ mp_vstream_encode_strn,
	/** encode_nil = */ mp_vstream_encode_nil,
	/** encode_bool = */ mp_vstream_encode_bool,
	/** encode_enum = */ mp_vstream_encode_enum,
	/** encode_port = */ mp_vstream_encode_port,
	/** encode_reply = */ mp_vstream_encode_reply,
	/** encode_array_commit = */ mp_vstream_encode_array_commit,
	/** encode_reply_commit = */ mp_vstream_encode_reply_commit,
	/** encode_map_commit = */ mp_vstream_encode_map_commit,
	/** encode_map_element_commit = */ mp_vstream_encode_map_element_commit,
};

void
mp_vstream_init(struct vstream *vstream, struct mpstream *mpstream)
{
	vstream->vtab = &mp_vstream_vtab;
	vstream->mpstream = mpstream;
}
