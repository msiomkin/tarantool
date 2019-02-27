#ifndef TARANTOOL_WAL_MEM_H_INCLUDED
#define TARANTOOL_WAL_MEM_H_INCLUDED
/*
 * Copyright 2010-2019, Tarantool AUTHORS, please see AUTHORS file.
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

#include "small/ibuf.h"
#include "small/obuf.h"
#include "xrow.h"
#include "vclock.h"

enum {
	/* Count of internal swapping storages. */
	WAL_MEM_BUF_COUNT = 4,
};

/* Internal data storage. */
struct wal_mem_buf {

	struct ibuf rows;
	/* Data storage for encoded rows, both header and bodies. */
	struct obuf data;
};

/*
 * Wal memory container consists from data storages organized in
 * a ring queue and the first and the last buffer queue indexes.
 * Indexes are not wrapped around queue length in order to track
 * outdated accesses.
 */
struct wal_mem {
	/* The first used buffer index. */
	uint32_t first_buf_index;
	/* The last (current) used buffer index. */
	uint32_t last_buf_index;
	/* Data storages array. */
	struct wal_mem_buf buf[WAL_MEM_BUF_COUNT];
};

/* Wal memory cursor to track a position in a wal memory. */
struct wal_mem_cursor {
	/* Current buffer index. */
	uint32_t buf_index;
	/* Index in buffer data store. */
	int iov_index;
	/* Offset in buffer data store. */
	size_t iov_len;
};

/* Create a wal momory structure. */
void
wal_mem_create(struct wal_mem *wal_mem);

/* Destroy wal memory structure. */
void
wal_mem_destroy(struct wal_mem *wal_mem);

/*
 * Append xrow pointer array to wal memory. Whole array will be
 * written to one wal memory buffer. Each row will be placed
 * continuously.
 * Return
 * 0 for Ok
 * -1 in case of error
 */
int
wal_mem_write(struct wal_mem *wal_mem, struct xrow_header **begin,
	      struct xrow_header **end);

/* Create wal memory cursor from wal memory current position. */
void
wal_mem_cursor_create(struct wal_mem_cursor *wal_mem_cursor,
		      struct wal_mem *wal_mem);

/*
 * Move cursor forward to the end of current buffer and build iovec which
 * points on read data. If current buffer hasn't got data to read then cursor
 * swithes to the next one if it possible.
 * Return
 * count of build iovec items,
 * 0 there are no more data to read,
 * -1 in case of error.
 */
int
wal_mem_cursor_forward(struct wal_mem_cursor *mem_cursor,
		       struct wal_mem *wal_mem, struct iovec *iov);

#endif /* TARANTOOL_WAL_MEM_H_INCLUDED */
