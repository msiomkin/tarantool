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

#include "wal_mem.h"

#include "fiber.h"
#include "errinj.h"

enum {
	/* Initial size for rows storage. */
	WAL_MEM_BUF_INITIAL_ROW_COUNT = 4096,
	/* Initial size for data storage. */
	WAL_MEM_BUF_INITIAL_DATA_SIZE = 65536,
	/* How many rows we will place in one buffer. */
	WAL_MEM_BUF_ROWS_LIMIT = 8192,
	/* How many data we will place in one buffer. */
	WAL_MEM_BUF_DATA_LIMIT = 1 << 19,
};

void
wal_mem_create(struct wal_mem *wal_mem)
{
	wal_mem->last_buf_index = 0;
	wal_mem->first_buf_index = 0;
	int i;
	for (i = 0; i < WAL_MEM_BUF_COUNT; ++i) {
		ibuf_create(&wal_mem->buf[i].rows, &cord()->slabc,
			    WAL_MEM_BUF_INITIAL_ROW_COUNT *
			    sizeof(struct xrow_header));
		obuf_create(&wal_mem->buf[i].data, &cord()->slabc,
			    WAL_MEM_BUF_INITIAL_DATA_SIZE);
	}
}

void
wal_mem_destroy(struct wal_mem *wal_mem)
{
	int i;
	for (i = 0; i < WAL_MEM_BUF_COUNT; ++i) {
		ibuf_destroy(&wal_mem->buf[i].rows);
		obuf_destroy(&wal_mem->buf[i].data);
	}
}

/*
 * Switch to the next buffer if needed and discard outdated data.
 */
static void
wal_mem_rotate(struct wal_mem *wal_mem)
{
	struct wal_mem_buf *mem_buf = wal_mem->buf +
				      wal_mem->last_buf_index % WAL_MEM_BUF_COUNT;
	if (ibuf_used(&mem_buf->rows) < WAL_MEM_BUF_ROWS_LIMIT *
					sizeof(struct xrow_header) &&
	    obuf_size(&mem_buf->data) < WAL_MEM_BUF_DATA_LIMIT)
		return;
	/* Switch to the next buffer (a target to append new data). */
	++wal_mem->last_buf_index;
	if (wal_mem->last_buf_index - wal_mem->first_buf_index <
	    WAL_MEM_BUF_COUNT) {
		/* The buffer is unused, nothing to do. */
		return;
	}
	/* Discard data and adjust first buffer index. */
	mem_buf = wal_mem->buf + wal_mem->first_buf_index % WAL_MEM_BUF_COUNT;
	ibuf_reset(&mem_buf->rows);
	obuf_reset(&mem_buf->data);
	++wal_mem->first_buf_index;
}

int
wal_mem_write(struct wal_mem *wal_mem, struct xrow_header **begin,
	      struct xrow_header **end)
{
	wal_mem_rotate(wal_mem);
	struct wal_mem_buf *mem_buf = wal_mem->buf +
				      wal_mem->last_buf_index % WAL_MEM_BUF_COUNT;
	struct iovec iov[XROW_IOVMAX];

	/* Get rollback values. */
	size_t old_rows_size = ibuf_used(&mem_buf->rows);
	struct obuf_svp data_svp = obuf_create_svp(&mem_buf->data);

	/* Allocate space for rows. */
	struct xrow_header *mem_row =
		(struct xrow_header *)ibuf_alloc(&mem_buf->rows,
						 (end - begin) *
						 sizeof(struct xrow_header));
	if (mem_row == NULL) {
		diag_set(OutOfMemory, (end - begin) * sizeof(struct xrow_header),
			 "region", "wal memory rows");
		goto error;
	}
	/* Append rows. */
	struct xrow_header **row;
	for (row = begin; row < end; ++row, ++mem_row) {
		struct errinj *inj = errinj(ERRINJ_WAL_BREAK_LSN, ERRINJ_INT);
		if (inj != NULL && inj->iparam == (*row)->lsn) {
			(*row)->lsn = inj->iparam - 1;
			say_warn("injected broken lsn: %lld",
				 (long long) (*row)->lsn);
		}

		char *data = obuf_reserve(&mem_buf->data, xrow_approx_len(*row));
		if (data == NULL) {
			diag_set(OutOfMemory, xrow_approx_len(*row),
				 "region", "wal memory data");
			goto error;
		}
		/*
		 * It allocates space only for header on gc and should't
		 * be too expensive.
		 */
		int iov_cnt = xrow_header_encode(*row, 0, iov, 0);
		if (iov_cnt < 0)
			goto error;
		data = obuf_alloc(&mem_buf->data, iov[0].iov_len);
		memcpy(data, iov[0].iov_base, iov[0].iov_len);
		/* Write bodies and patch its location. */
		*mem_row = **row;
		assert(mem_row->bodycnt == iov_cnt - 1);
		int i;
		for (i = 1; i < iov_cnt; ++i) {
			/* Append xrow bodies and patch xrow pointers. */
			data = obuf_alloc(&mem_buf->data, iov[i].iov_len);
			memcpy(data, iov[i].iov_base, iov[i].iov_len);
			mem_row->body[i - 1].iov_base = data;
		}
	}
	return 0;

error:
	/* Restore buffer state. */
	mem_buf->rows.wpos = mem_buf->rows.rpos + old_rows_size;
	obuf_rollback_to_svp(&mem_buf->data, &data_svp);
	return -1;
}

void
wal_mem_cursor_create(struct wal_mem_cursor *wal_mem_cursor,
		      struct wal_mem *wal_mem)
{
	wal_mem_cursor->buf_index = wal_mem->last_buf_index;
	struct wal_mem_buf *mem_buf;
	mem_buf = wal_mem->buf + (wal_mem->last_buf_index % WAL_MEM_BUF_COUNT);
	wal_mem_cursor->iov_index = mem_buf->data.pos;
	wal_mem_cursor->iov_len = mem_buf->data.iov[mem_buf->data.pos].iov_len;
}

int
wal_mem_cursor_forward(struct wal_mem_cursor *mem_cursor,
		       struct wal_mem *wal_mem, struct iovec *iov)
{
	if (mem_cursor->buf_index - wal_mem->first_buf_index >=
	    WAL_MEM_BUF_COUNT) {
		/* Cursor is outdated. */
		return -1;
	}
	struct iovec *out;
	struct wal_mem_buf *mem_buf;
	mem_buf = wal_mem->buf + (mem_cursor->buf_index % WAL_MEM_BUF_COUNT);
	if (mem_cursor->iov_index == mem_buf->data.pos &&
	    mem_cursor->iov_len == mem_buf->data.iov[mem_buf->data.pos].iov_len &&
	    mem_cursor->buf_index != wal_mem->last_buf_index) {
		/*
		 * Current buffer has no data and we can switch to the
		 * next one
		 */
		 ++mem_cursor->buf_index;
		 mem_cursor->iov_index = 0;
		 mem_cursor->iov_len = 0;
		 mem_buf = wal_mem->buf +
			   (mem_cursor->buf_index % WAL_MEM_BUF_COUNT);
	}
	out = iov;
	mem_buf = wal_mem->buf + (mem_cursor->buf_index % WAL_MEM_BUF_COUNT);
	/* Read current iov item. */
	out->iov_base = mem_buf->data.iov[mem_cursor->iov_index].iov_base +
			mem_cursor->iov_len;
	out->iov_len = mem_buf->data.iov[mem_cursor->iov_index].iov_len -
		       mem_cursor->iov_len;
	mem_cursor->iov_len += out->iov_len;
	/* Read next iov items until buffer end. */
	while (mem_cursor->iov_index < mem_buf->data.pos) {
		++mem_cursor->iov_index;
		out++;
		out->iov_base = mem_buf->data.iov[mem_cursor->iov_index].iov_base;
		out->iov_len = mem_buf->data.iov[mem_cursor->iov_index].iov_len;
		mem_cursor->iov_len = out->iov_len;
	}
	int res = out - iov + (out->iov_len > 0? 1: 0);
	return res;
}
