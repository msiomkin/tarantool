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
#include <assert.h>
#include "ck_constraint.h"
#include "errcode.h"
#include "small/rlist.h"
#include "sql.h"
#include "sql/sqliteInt.h"

/**
 * Resolve space_def references for check constraint via AST
 * tree traversal.
 * @param expr Check constraint expression AST to resolve column
 *             references.
 * @param ck_constraint_name Check constraint name to raise error.
 * @param space_def Space definition to use.
 * @retval 0 on success.
 * @retval -1 on error.
 */
static int
ck_constraint_resolve_column_reference(struct Expr *expr,
				       const char *ck_constraint_name,
				       const struct space_def *space_def)
{
	struct Parse parser;
	sql_parser_create(&parser, sql_get());
	parser.parse_only = true;

	struct Table dummy_table;
	memset(&dummy_table, 0, sizeof(dummy_table));
	dummy_table.def = (struct space_def *)space_def;

	/* Fake SrcList for parser->pNewTable */
	struct SrcList sSrc;
	/* Name context for parser->pNewTable */
	struct NameContext sNC;

	memset(&sNC, 0, sizeof(sNC));
	memset(&sSrc, 0, sizeof(sSrc));
	sSrc.nSrc = 1;
	sSrc.a[0].zName = (char *)space_def->name;
	sSrc.a[0].pTab = &dummy_table;
	sSrc.a[0].iCursor = -1;
	sNC.pParse = &parser;
	sNC.pSrcList = &sSrc;
	sNC.ncFlags = NC_IsCheck;
	sqlite3ResolveExprNames(&sNC, expr);

	int rc = 0;
	if (parser.rc != SQLITE_OK) {
		/* Tarantool error may be already set with diag. */
		if (parser.rc != SQL_TARANTOOL_ERROR) {
			diag_set(ClientError, ER_CREATE_CK_CONSTRAINT,
				 ck_constraint_name, parser.zErrMsg);
		}
		rc = -1;
	}
	if (sNC.ncFlags & NC_HasTypeofFunction) {
		diag_set(ClientError, ER_CREATE_CK_CONSTRAINT,
			 ck_constraint_name,
			 "TYPEOF is forbidden in check constraint");
		rc = -1;
	}
	sql_parser_destroy(&parser);
	return rc;
}

uint32_t
ck_constraint_def_sizeof(uint32_t name_len, uint32_t expr_str_len,
			 uint32_t *name_offset, uint32_t *expr_str_offset)
{
	*name_offset = sizeof(struct ck_constraint_def);
	*expr_str_offset = *name_offset + (name_len != 0 ? name_len + 1 : 0);
	return *expr_str_offset + (expr_str_len != 0 ? expr_str_len + 1 : 0);
}

void
ck_constraint_def_create(struct ck_constraint_def *ck_constraint_def,
			 const char *name, uint32_t name_len,
			 const char *expr_str, uint32_t expr_str_len)
{
	uint32_t name_offset, expr_str_offset;
	(void)ck_constraint_def_sizeof(name_len, expr_str_len, &name_offset,
				       &expr_str_offset);
	ck_constraint_def->name = (char *)ck_constraint_def + name_offset;
	sprintf(ck_constraint_def->name, "%.*s", name_len, name);
	ck_constraint_def->expr_str =
		(char *)ck_constraint_def + expr_str_offset;
	sprintf(ck_constraint_def->expr_str, "%.*s", expr_str_len, expr_str);
	rlist_create(&ck_constraint_def->link);
}

struct ck_constraint *
ck_constraint_new(const struct ck_constraint_def *ck_constraint_def,
		  const struct space_def *space_def)
{
	uint32_t ck_constraint_name_len = strlen(ck_constraint_def->name);
	uint32_t expr_str_len = strlen(ck_constraint_def->expr_str);
	uint32_t name_offset, expr_str_offset;
	uint32_t ck_constraint_def_sz =
		ck_constraint_def_sizeof(ck_constraint_name_len, expr_str_len,
					 &name_offset, &expr_str_offset);
	uint32_t ck_constraint_sz = sizeof(struct ck_constraint) +
				    ck_constraint_def_sz;
	struct ck_constraint *ck_constraint = calloc(1, ck_constraint_sz);
	if (ck_constraint == NULL) {
		diag_set(OutOfMemory, ck_constraint_sz, "malloc",
			 "ck_constraint");
		return NULL;
	}
	rlist_create(&ck_constraint->link);
	ck_constraint->space_id = space_def->id;
	ck_constraint->def =
		(struct ck_constraint_def *)((char *)ck_constraint +
					     sizeof(struct ck_constraint));
	ck_constraint_def_create(ck_constraint->def, ck_constraint_def->name,
				 ck_constraint_name_len,
				 ck_constraint_def->expr_str, expr_str_len);
	struct Expr *expr =
		sql_expr_compile(sql_get(), ck_constraint_def->expr_str,
				 expr_str_len);
	if (expr == NULL)
		goto error;
	if (ck_constraint_resolve_column_reference(expr, ck_constraint_def->name,
						   space_def) != 0)
		goto error;
	ck_constraint->expr = expr;

	return ck_constraint;
error:
	ck_constraint_delete(ck_constraint);
	return NULL;
}

void
ck_constraint_delete(struct ck_constraint *ck_constraint)
{
	sql_expr_delete(sql_get(), ck_constraint->expr, false);
	TRASH(ck_constraint);
	free(ck_constraint);
}

struct ck_constraint *
space_ck_constraint_by_name(struct space *space, const char *name,
			    uint32_t name_len)
{
	struct ck_constraint *ck_constraint = NULL;
	rlist_foreach_entry(ck_constraint, &space->ck_constraint, link) {
		if (strlen(ck_constraint->def->name) == name_len &&
		    memcmp(ck_constraint->def->name, name, name_len) == 0)
			return ck_constraint;
	}
	return NULL;
}
