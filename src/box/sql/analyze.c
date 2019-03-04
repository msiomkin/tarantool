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

/*
 * This file contains code associated with the ANALYZE command.
 *
 * The ANALYZE command gather statistics about the content of tables
 * and indices.  These statistics are made available to the query planner
 * to help it make better decisions about how to perform queries.
 *
 * The following system tables are or have been supported:
 *
 *    CREATE TABLE _sql_stat1(tbl, idx, stat);
 *    CREATE TABLE _sql_stat4(tbl, idx, nEq, nLt, nDLt, sample);
 *
 * For most applications, _sql_stat1 provides all the statistics required
 * for the query planner to make good choices.
 *
 * Format of _sql_stat1:
 *
 * There is normally one row per index, with the index identified by the
 * name in the idx column.  The tbl column is the name of the table to
 * which the index belongs.  In each such row, the stat column will be
 * a string consisting of a list of integers.  The first integer in this
 * list is the number of rows in the index.  (This is the same as the
 * number of rows in the table.)  The second integer is the
 * average number of rows in the index that have the same value in
 * the first column of the index.  The third integer is the average
 * number of rows in the index that have the same value for the first two
 * columns.  The N-th integer (for N>1) is the average number of rows in
 * the index which have the same value for the first N-1 columns.  For
 * a K-column index, there will be K+1 integers in the stat column.  If
 * the index is unique, then the last integer will be 1.
 *
 * The list of integers in the stat column can optionally be followed
 * by the keyword "unordered".  The "unordered" keyword, if it is present,
 * must be separated from the last integer by a single space.  If the
 * "unordered" keyword is present, then the query planner assumes that
 * the index is unordered and will not use the index for a range query.
 *
 * If the _sql_stat1.idx column is NULL, then the _sql_stat1.stat
 * column contains a single integer which is the (estimated) number of
 * rows in the table identified by _sql_stat1.tbl.
 *
 * Format for _sql_stat4:
 *
 * The _sql_stat4 table contains histogram data
 * to aid the query planner in choosing good indices based on the values
 * that indexed columns are compared against in the WHERE clauses of
 * queries.
 *
 * The _sql_stat4 table contains multiple entries for each index.
 * The idx column names the index and the tbl column is the table of the
 * index.  If the idx and tbl columns are the same, then the sample is
 * of the INTEGER PRIMARY KEY.  The sample column is a blob which is the
 * binary encoding of a key from the index.  The nEq column is a
 * list of integers.  The first integer is the approximate number
 * of entries in the index whose left-most column exactly matches
 * the left-most column of the sample.  The second integer in nEq
 * is the approximate number of entries in the index where the
 * first two columns match the first two columns of the sample.
 * And so forth.  nLt is another list of integers that show the approximate
 * number of entries that are strictly less than the sample.  The first
 * integer in nLt contains the number of entries in the index where the
 * left-most column is less than the left-most column of the sample.
 * The K-th integer in the nLt entry is the number of index entries
 * where the first K columns are less than the first K columns of the
 * sample.  The nDLt column is like nLt except that it contains the
 * number of distinct entries in the index that are less than the
 * sample.
 *
 * There can be an arbitrary number of _sql_stat4 entries per index.
 * The ANALYZE command will typically generate _sql_stat4 tables
 * that contain between 10 and 40 samples which are distributed across
 * the key space, though not uniformly, and which include samples with
 * large nEq values.
 *
 */

#include "box/box.h"
#include "box/index.h"
#include "box/key_def.h"
#include "box/schema.h"
#include "third_party/qsort_arg.h"

#include "sqlInt.h"
#include "tarantoolInt.h"
#include "vdbeInt.h"

/**
 * This routine generates code that opens the sql_stat1/4 tables.
 * If the sql_statN tables do not previously exist, they are
 * created.
 *
 * @param parse Parsing context.
 * @param table_name Delete records of this table if specified.
 */
static void
vdbe_emit_stat_space_open(struct Parse *parse, uint32_t space_id)
{
	struct Vdbe *vdbe = sqlGetVdbe(parse);
	assert(vdbe != NULL);
	assert(sqlVdbeDb(vdbe) == parse->db);
	if (space_id != BOX_ID_NIL)
		vdbe_emit_stat_space_clear(parse, space_id, BOX_ID_NIL);
	else
		sqlVdbeAddOp1(vdbe, OP_Clear, BOX_SQL_STAT_ID);
}

/*
 * Recommended number of samples for _sql_stat4
 */
#ifndef SQL_STAT4_SAMPLES
#define SQL_STAT4_SAMPLES 24
#endif

/*
 * Three SQL functions - stat_init(), stat_push(), and stat_get() -
 * share an instance of the following structure to hold their state
 * information.
 */
typedef struct Stat4Accum Stat4Accum;
typedef struct Stat4Sample Stat4Sample;
struct Stat4Sample {
	tRowcnt *anEq;		/* _sql_stat4.nEq */
	tRowcnt *anDLt;		/* _sql_stat4.nDLt */
	tRowcnt *anLt;		/* _sql_stat4.nLt */
	u8 *aKey;		/* Table key */
	u32 nKey;		/* Sizeof aKey[] */
	u8 isPSample;		/* True if a periodic sample */
	int iCol;		/* If !isPSample, the reason for inclusion */
	u32 iHash;		/* Tiebreaker hash */
};
struct Stat4Accum {
	tRowcnt nRow;		/* Number of rows in the entire table */
	tRowcnt nPSample;	/* How often to do a periodic sample */
	int nCol;		/* Number of columns in index + pk */
	int nKeyCol;		/* Number of index columns w/o the pk */
	int mxSample;		/* Maximum number of samples to accumulate */
	Stat4Sample current;	/* Current row as a Stat4Sample */
	u32 iPrn;		/* Pseudo-random number used for sampling */
	Stat4Sample *aBest;	/* Array of nCol best samples */
	int iMin;		/* Index in a[] of entry with minimum score */
	int nSample;		/* Current number of samples */
	int iGet;		/* Index of current sample accessed by stat_get() */
	Stat4Sample *a;		/* Array of mxSample Stat4Sample objects */
	sql *db;		/* Database connection, for malloc() */
};

/* Reclaim memory used by a Stat4Sample
 */
static void
sampleClear(sql * db, Stat4Sample * p)
{
	assert(db != 0);
	if (p->nKey) {
		sqlDbFree(db, p->aKey);
		p->nKey = 0;
	}
}

/* Initialize the BLOB value of a sample key.
 */
static void
sampleSetKey(sql * db, Stat4Sample * p, int n, const u8 * pData)
{
	assert(db != 0);
	if (p->nKey)
		sqlDbFree(db, p->aKey);
	p->aKey = sqlDbMallocRawNN(db, n);
	if (p->aKey) {
		p->nKey = n;
		memcpy(p->aKey, pData, n);
	} else {
		p->nKey = 0;
	}
}

/*
 * Copy the contents of object (*pFrom) into (*pTo).
 */
static void
sampleCopy(Stat4Accum * p, Stat4Sample * pTo, Stat4Sample * pFrom)
{
	pTo->isPSample = pFrom->isPSample;
	pTo->iCol = pFrom->iCol;
	pTo->iHash = pFrom->iHash;
	memcpy(pTo->anEq, pFrom->anEq, sizeof(tRowcnt) * (p->nCol+1));
	memcpy(pTo->anLt, pFrom->anLt, sizeof(tRowcnt) * (p->nCol+1));
	memcpy(pTo->anDLt, pFrom->anDLt, sizeof(tRowcnt) * (p->nCol+1));
	sampleSetKey(p->db, pTo, pFrom->nKey, pFrom->aKey);
}

/*
 * Reclaim all memory of a Stat4Accum structure.
 */
static void
stat4Destructor(void *pOld)
{
	Stat4Accum *p = (Stat4Accum *) pOld;
	int i;
	for (i = 0; i < p->nCol; i++)
		sampleClear(p->db, p->aBest + i);
	for (i = 0; i < p->mxSample; i++)
		sampleClear(p->db, p->a + i);
	sampleClear(p->db, &p->current);
	sqlDbFree(p->db, p);
}

/*
 * Implementation of the stat_init(N,K,C) SQL function. The three parameters
 * are:
 *     N:    The number of columns in the index including the pk (note 1)
 *     K:    The number of columns in the index excluding the pk.
 *     C:    The number of rows in the index (note 2)
 *
 * Note 1:  In the special case of the covering index, N is the number of
 * PRIMARY KEY columns, not the total number of columns in the table.
 *
 * Note 2:  C is only used for STAT4.
 *
 * N=K+P where P is the number of columns in the
 * PRIMARY KEY of the table.  The covering index as N==K as a special case.
 *
 * This routine allocates the Stat4Accum object in heap memory. The return
 * value is a pointer to the Stat4Accum object.  The datatype of the
 * return value is BLOB, but it is really just a pointer to the Stat4Accum
 * object.
 */
static void
statInit(sql_context * context, int argc, sql_value ** argv)
{
	Stat4Accum *p;
	int nCol;		/* Number of columns in index being sampled */
	int nKeyCol;		/* Number of key columns */
	int nColUp;		/* nCol rounded up for alignment */
	int n;			/* Bytes of space to allocate */
	sql *db;		/* Database connection */
	int mxSample = SQL_STAT4_SAMPLES;

	/* Decode the three function arguments */
	UNUSED_PARAMETER(argc);
	nCol = sql_value_int(argv[0]);
	assert(nCol > 0);
	/* Tarantool: we use an additional artificial column for the reason
	 * that Tarantool's indexes don't contain PK columns after key columns.
	 * Hence, in order to correctly gather statistics when dealing with 
	 * identical rows, we have to use this artificial column.
	 */
	nColUp = sizeof(tRowcnt) < 8 ? (nCol + 2) & ~1 : nCol + 1;
	nKeyCol = sql_value_int(argv[1]);
	assert(nKeyCol <= nCol);
	assert(nKeyCol > 0);

	/* Allocate the space required for the Stat4Accum object */
	n = sizeof(*p)
	    + sizeof(tRowcnt) * nColUp	/* Stat4Accum.anEq */
	    + sizeof(tRowcnt) * nColUp	/* Stat4Accum.anDLt */
	    + sizeof(tRowcnt) * nColUp	/* Stat4Accum.anLt */
	    + sizeof(Stat4Sample) * (nCol + 1 + mxSample)	/* Stat4Accum.aBest[], a[] */
	    + sizeof(tRowcnt) * 3 * nColUp * (nCol + 1 + mxSample);
	db = sql_context_db_handle(context);
	p = sqlDbMallocZero(db, n);
	if (p == 0) {
		sql_result_error_nomem(context);
		return;
	}

	p->db = db;
	p->nRow = 0;
	p->nCol = nCol;
	p->nKeyCol = nKeyCol;
	p->current.anDLt = (tRowcnt *) & p[1];
	p->current.anEq = &p->current.anDLt[nColUp];

	{
		u8 *pSpace;	/* Allocated space not yet assigned */
		int i;		/* Used to iterate through p->aSample[] */

		p->iGet = -1;
		p->mxSample = mxSample;
		p->nPSample =
		    (tRowcnt) (sql_value_int64(argv[2]) /
			       (mxSample / 3 + 1) + 1);
		p->current.anLt = &p->current.anEq[nColUp];
		p->iPrn =
		    0x689e962d * (u32) nCol ^ 0xd0944565 *
		    (u32) sql_value_int(argv[2]);

		/* Set up the Stat4Accum.a[] and aBest[] arrays */
		p->a = (struct Stat4Sample *)&p->current.anLt[nColUp];
		p->aBest = &p->a[mxSample];
		pSpace = (u8 *) (&p->a[mxSample + nCol + 1]);
		for (i = 0; i < (mxSample + nCol + 1); i++) {
			p->a[i].anEq = (tRowcnt *) pSpace;
			pSpace += (sizeof(tRowcnt) * nColUp);
			p->a[i].anLt = (tRowcnt *) pSpace;
			pSpace += (sizeof(tRowcnt) * nColUp);
			p->a[i].anDLt = (tRowcnt *) pSpace;
			pSpace += (sizeof(tRowcnt) * nColUp);
		}
		assert((pSpace - (u8 *) p) == n);

		for (i = 0; i < nCol + 1; i++) {
			p->aBest[i].iCol = i;
		}
	}

	/* Return a pointer to the allocated object to the caller.  Note that
	 * only the pointer (the 2nd parameter) matters.  The size of the object
	 * (given by the 3rd parameter) is never used and can be any positive
	 * value.
	 */
	sql_result_blob(context, p, sizeof(*p), stat4Destructor);
}

static const FuncDef statInitFuncdef = {
	3,			/* nArg */
	0,			/* funcFlags */
	0,			/* pUserData */
	0,			/* pNext */
	statInit,		/* xSFunc */
	0,			/* xFinalize */
	"stat_init",		/* zName */
	{0},
	0
};

/*
 * pNew and pOld are both candidate non-periodic samples selected for
 * the same column (pNew->iCol==pOld->iCol). Ignoring this column and
 * considering only any trailing columns and the sample hash value, this
 * function returns true if sample pNew is to be preferred over pOld.
 * In other words, if we assume that the cardinalities of the selected
 * column for pNew and pOld are equal, is pNew to be preferred over pOld.
 *
 * This function assumes that for each argument sample, the contents of
 * the anEq[] array from pSample->anEq[pSample->iCol+1] onwards are valid.
 */
static int
sampleIsBetterPost(Stat4Accum * pAccum, Stat4Sample * pNew, Stat4Sample * pOld)
{
	int nCol = pAccum->nCol;
	int i;
	assert(pNew->iCol == pOld->iCol);
	for (i = pNew->iCol + 1; i < nCol + 1; i++) {
		if (pNew->anEq[i] > pOld->anEq[i])
			return 1;
		if (pNew->anEq[i] < pOld->anEq[i])
			return 0;
	}
	if (pNew->iHash > pOld->iHash)
		return 1;
	return 0;
}

/*
 * Return true if pNew is to be preferred over pOld.
 *
 * This function assumes that for each argument sample, the contents of
 * the anEq[] array from pSample->anEq[pSample->iCol] onwards are valid.
 */
static int
sampleIsBetter(Stat4Accum * pAccum, Stat4Sample * pNew, Stat4Sample * pOld)
{
	tRowcnt nEqNew = pNew->anEq[pNew->iCol];
	tRowcnt nEqOld = pOld->anEq[pOld->iCol];

	assert(pOld->isPSample == 0 && pNew->isPSample == 0);

	if ((nEqNew > nEqOld))
		return 1;
	if (nEqNew == nEqOld) {
		if (pNew->iCol < pOld->iCol)
			return 1;
		return (pNew->iCol == pOld->iCol
			&& sampleIsBetterPost(pAccum, pNew, pOld));
	}
	return 0;
}

/*
 * Copy the contents of sample *pNew into the p->a[] array. If necessary,
 * remove the least desirable sample from p->a[] to make room.
 */
static void
sampleInsert(Stat4Accum * p, Stat4Sample * pNew, int nEqZero)
{
	Stat4Sample *pSample = 0;
	int i;

	if (pNew->isPSample == 0) {
		Stat4Sample *pUpgrade = 0;
		assert(pNew->anEq[pNew->iCol] > 0);

		/* This sample is being added because the prefix that ends in column
		 * iCol occurs many times in the table. However, if we have already
		 * added a sample that shares this prefix, there is no need to add
		 * this one. Instead, upgrade the priority of the highest priority
		 * existing sample that shares this prefix.
		 */
		for (i = p->nSample - 1; i >= 0; i--) {
			Stat4Sample *pOld = &p->a[i];
			if (pOld->anEq[pNew->iCol] == 0) {
				if (pOld->isPSample)
					return;
				assert(pOld->iCol > pNew->iCol);
				assert(sampleIsBetter(p, pNew, pOld));
				if (pUpgrade == 0
				    || sampleIsBetter(p, pOld, pUpgrade)) {
					pUpgrade = pOld;
				}
			}
		}
		if (pUpgrade) {
			pUpgrade->iCol = pNew->iCol;
			pUpgrade->anEq[pUpgrade->iCol] =
			    pNew->anEq[pUpgrade->iCol];
			goto find_new_min;
		}
	}

	/* If necessary, remove sample iMin to make room for the new sample. */
	if (p->nSample >= p->mxSample) {
		Stat4Sample *pMin = &p->a[p->iMin];
		tRowcnt *anEq = pMin->anEq;
		tRowcnt *anLt = pMin->anLt;
		tRowcnt *anDLt = pMin->anDLt;
		sampleClear(p->db, pMin);
		memmove(pMin, &pMin[1],
			sizeof(p->a[0]) * (p->nSample - p->iMin - 1));
		pSample = &p->a[p->nSample - 1];
		pSample->nKey = 0;
		pSample->anEq = anEq;
		pSample->anDLt = anDLt;
		pSample->anLt = anLt;
		p->nSample = p->mxSample - 1;
	}

	assert(p->nSample==0 || pNew->anLt[p->nCol] > p->a[p->nSample-1].anLt[p->nCol]);

	/* Insert the new sample */
	pSample = &p->a[p->nSample];
	sampleCopy(p, pSample, pNew);
	p->nSample++;

	/* Zero the first nEqZero entries in the anEq[] array. */
	memset(pSample->anEq, 0, sizeof(tRowcnt) * nEqZero);

 find_new_min:
	if (p->nSample >= p->mxSample) {
		int iMin = -1;
		for (i = 0; i < p->mxSample; i++) {
			if (p->a[i].isPSample)
				continue;
			if (iMin < 0
			    || sampleIsBetter(p, &p->a[iMin], &p->a[i])) {
				iMin = i;
			}
		}
		assert(iMin >= 0);
		p->iMin = iMin;
	}
}

/*
 * Field iChng of the index being scanned has changed. So at this point
 * p->current contains a sample that reflects the previous row of the
 * index. The value of anEq[iChng] and subsequent anEq[] elements are
 * correct at this point.
 */

static void
samplePushPrevious(Stat4Accum * p, int iChng)
{
	int i;
	/* Check if any samples from the aBest[] array should be pushed
	 * into samples array at this point.
	 */
	for (i = (p->nCol - 1); i >= iChng; i--) {
		Stat4Sample *pBest = &p->aBest[i];
		pBest->anEq[i] = p->current.anEq[i];
		if (p->nSample < p->mxSample
		    || sampleIsBetter(p, pBest, &p->a[p->iMin])) {
			sampleInsert(p, pBest, i);
		}
	}

	/* Update the anEq[] fields of any samples already collected. */
	for (i = p->nSample - 1; i >= 0; i--) {
		int j;
		for (j = iChng; j < p->nCol + 1; j++) {
			if (p->a[i].anEq[j] == 0)
				p->a[i].anEq[j] = p->current.anEq[j];
		}
	}
}

/*
 * Implementation of the stat_push SQL function:  stat_push(P,C,R)
 * Arguments:
 *
 *    P     Pointer to the Stat4Accum object created by stat_init()
 *    C     Index of left-most column to differ from previous row
 *    R     Key record for the current row
 *
 * This SQL function always returns NULL.  It's purpose it to accumulate
 * statistical data and/or samples in the Stat4Accum object about the
 * index being analyzed.  The stat_get() SQL function will later be used to
 * extract relevant information for constructing the sql_statN tables.
 *
 * The R parameter is only used for STAT4
 */
static void
statPush(sql_context * context, int argc, sql_value ** argv)
{
	int i;
	/* The three function arguments */
	Stat4Accum *p = (Stat4Accum *) sql_value_blob(argv[0]);
	int iChng = sql_value_int(argv[1]);

	UNUSED_PARAMETER(argc);
	UNUSED_PARAMETER(context);
	assert(p->nCol > 0);
	/* iChng == p->nCol means that the current and previous rows are identical */
	assert(iChng <= p->nCol);
	if (p->nRow == 0) {
		/* This is the first call to this function. Do initialization. */
		for (i = 0; i < p->nCol + 1; i++)
			p->current.anEq[i] = 1;
	} else {
		/* Second and subsequent calls get processed here */
		samplePushPrevious(p, iChng);

		/* Update anDLt[], anLt[] and anEq[] to reflect the values that apply
		 * to the current row of the index.
		 */
		for (i = 0; i < iChng; i++) {
			p->current.anEq[i]++;
		}
		for (i = iChng; i < p->nCol + 1; i++) {
			p->current.anDLt[i]++;
			p->current.anLt[i] += p->current.anEq[i];
			p->current.anEq[i] = 1;
		}
	}
	p->nRow++;
	sampleSetKey(p->db, &p->current, sql_value_bytes(argv[2]),
		     sql_value_blob(argv[2]));
	p->current.iHash = p->iPrn = p->iPrn * 1103515245 + 12345;
	{
		tRowcnt nLt = p->current.anLt[p->nCol];

		/* Check if this is to be a periodic sample. If so, add it. */
		if ((nLt / p->nPSample) != (nLt + 1) / p->nPSample) {
			p->current.isPSample = 1;
			p->current.iCol = 0;
			sampleInsert(p, &p->current, p->nCol);
			p->current.isPSample = 0;
		}
		/* Update the aBest[] array. */
		for (i = 0; i < p->nCol; i++) {
			p->current.iCol = i;
			if (i >= iChng
			    || sampleIsBetterPost(p, &p->current,
						  &p->aBest[i])) {
				sampleCopy(p, &p->aBest[i], &p->current);
			}
		}
	}
}

static const FuncDef statPushFuncdef = {
	3,			/* nArg */
	0,			/* funcFlags */
	0,			/* pUserData */
	0,			/* pNext */
	statPush,		/* xSFunc */
	0,			/* xFinalize */
	"stat_push",		/* zName */
	{0},
	0
};

#define STAT_GET_STAT1 0	/* "stat" column of stat1 table */
#define STAT_GET_KEY   1	/* "key" column of stat4 entry */
#define STAT_GET_NEQ   2	/* "neq" column of stat4 entry */
#define STAT_GET_NLT   3	/* "nlt" column of stat4 entry */
#define STAT_GET_NDLT  4	/* "ndlt" column of stat4 entry */

/*
 * Implementation of the stat_get(P,J) SQL function.  This routine is
 * used to query statistical information that has been gathered into
 * the Stat4Accum object by prior calls to stat_push().  The P parameter
 * has type BLOB but it is really just a pointer to the Stat4Accum object.
 * The content to returned is determined by the parameter J
 * which is one of the STAT_GET_xxxx values defined above.
 */
static void
statGet(sql_context * context, int argc, sql_value ** argv)
{
	Stat4Accum *p = (Stat4Accum *) sql_value_blob(argv[0]);
	/* STAT4 have a parameter on this routine. */
	int eCall = sql_value_int(argv[1]);
	assert(argc == 2);
	assert(eCall == STAT_GET_STAT1 || eCall == STAT_GET_NEQ
	       || eCall == STAT_GET_KEY || eCall == STAT_GET_NLT
	       || eCall == STAT_GET_NDLT);
	if (eCall == STAT_GET_STAT1) {
		/* Return the value to store in the "stat" column of the _sql_stat1
		 * table for this index.
		 *
		 * The value is a string composed of a list of integers describing
		 * the index. The first integer in the list is the total number of
		 * entries in the index. There is one additional integer in the list
		 * for each indexed column. This additional integer is an estimate of
		 * the number of rows matched by a stabbing query on the index using
		 * a key with the corresponding number of fields. In other words,
		 * if the index is on columns (a,b) and the _sql_stat1 value is
		 * "100 10 2", then sql estimates that:
		 *
		 *   * the index contains 100 rows,
		 *   * "WHERE a=?" matches 10 rows, and
		 *   * "WHERE a=? AND b=?" matches 2 rows.
		 *
		 * If D is the count of distinct values and K is the total number of
		 * rows, then each estimate is computed as:
		 *
		 *        I = (K+D-1)/D
		 */
		char *z;
		int i;

		char *zRet = sqlMallocZero((p->nKeyCol + 1) * 25);
		if (zRet == 0) {
			sql_result_error_nomem(context);
			return;
		}

		sql_snprintf(24, zRet, "%llu", (u64) p->nRow);
		z = zRet + sqlStrlen30(zRet);
		for (i = 0; i < p->nKeyCol; i++) {
			u64 nDistinct = p->current.anDLt[i] + 1;
			u64 iVal = (p->nRow + nDistinct - 1) / nDistinct;
			sql_snprintf(24, z, " %llu", iVal);
			z += sqlStrlen30(z);
			assert(p->current.anEq[i]);
		}
		assert(z[0] == '\0' && z > zRet);

		sql_result_text(context, zRet, -1, sql_free);
	} else if (eCall == STAT_GET_KEY) {
		if (p->iGet < 0) {
			samplePushPrevious(p, 0);
			p->iGet = 0;
		}
		if (p->iGet < p->nSample) {
			Stat4Sample *pS = p->a + p->iGet;
			sql_result_blob(context, pS->aKey, pS->nKey,
					    SQL_TRANSIENT);
		}
	} else {
		tRowcnt *aCnt = 0;

		assert(p->iGet < p->nSample);
		switch (eCall) {
		case STAT_GET_NEQ:
			aCnt = p->a[p->iGet].anEq;
			break;
		case STAT_GET_NLT:
			aCnt = p->a[p->iGet].anLt;
			break;
		default:{
			aCnt = p->a[p->iGet].anDLt;
			p->iGet++;
			break;
		}
	}

	char *zRet = sqlMallocZero(p->nCol * 25);
	if (zRet == 0) {
		sql_result_error_nomem(context);
	} else {
		int i;
		char *z = zRet;
		for (i = 0; i < p->nCol; i++) {
			sql_snprintf(24, z, "%llu ", (u64) aCnt[i]);
			z += sqlStrlen30(z);
		}
		assert(z[0] == '\0' && z > zRet);
		z[-1] = '\0';
		sql_result_text(context, zRet, -1, sql_free);
	}

}
#ifndef SQL_DEBUG
UNUSED_PARAMETER(argc);
#endif
}

static const FuncDef statGetFuncdef = {
	2,			/* nArg */
	0,			/* funcFlags */
	0,			/* pUserData */
	0,			/* pNext */
	statGet,		/* xSFunc */
	0,			/* xFinalize */
	"stat_get",		/* zName */
	{0},
	0
};

static void
callStatGet(Vdbe * v, int regStat4, int iParam, int regOut)
{
	assert(regOut != regStat4 && regOut != regStat4 + 1);
	sqlVdbeAddOp2(v, OP_Integer, iParam, regStat4 + 1);
	sqlVdbeAddOp4(v, OP_Function0, 0, regStat4, regOut,
			  (char *)&statGetFuncdef, P4_FUNCDEF);
	sqlVdbeChangeP5(v, 2);
}

/**
 * Generate code to do an analysis of all indices associated with
 * a single table.
 *
 * @param parse Current parsing context.
 * @param space Space to be analyzed.
 */
static void
vdbe_emit_analyze_space(struct Parse *parse, struct space *space)
{
	/* Do not gather statistics on system tables. */
	if (space_is_system(space))
		return;
	assert(space != NULL);
	struct space *stat = space_by_id(BOX_SQL_STAT_ID);
	assert(stat != NULL);

	int space_id_reg = ++parse->nMem;
	int index_id_reg = ++parse->nMem;
	int stat_reg = ++parse->nMem;
	int neq_reg = ++parse->nMem;
	int nlt_reg = ++parse->nMem;
	int ndlt_reg = ++parse->nMem;
	int sample_reg = ++parse->nMem;

	int stat_accum_reg = ++parse->nMem;
	int changed_field_reg = ++parse->nMem;
	int key_reg = ++parse->nMem;
	int tmp_reg = ++parse->nMem;
	int neq_array_reg = ++parse->nMem;
	parse->nMem += SQL_STAT4_SAMPLES;
	int nlt_array_reg = parse->nMem;
	parse->nMem += SQL_STAT4_SAMPLES;
	int ndlt_array_reg = parse->nMem;
	parse->nMem += SQL_STAT4_SAMPLES;
	int sample_array_reg = parse->nMem;
	parse->nMem += SQL_STAT4_SAMPLES;
	int prev_reg = parse->nMem;

	/*
	 * Open a read-only cursor on the table. Also allocate
	 * a cursor number to use for scanning indexes.
	 */
	int tab_cursor = parse->nTab;
	parse->nTab += 2;
	assert(space->index_count != 0);
	struct index *pk = space_index(space, 0);
	int pk_part_count = pk->def->key_def->part_count;
	struct Vdbe *vdbe = sqlGetVdbe(parse);
	assert(vdbe != NULL);
	sqlVdbeAddOp4(vdbe, OP_IteratorOpen, tab_cursor, 0, 0, (void *) space,
		      P4_SPACEPTR);
	sqlVdbeAddOp2(vdbe, OP_Integer, space->def->id, space_id_reg);
	for (uint32_t j = 0; j < space->index_count; ++j) {
		struct index *idx = space->index[j];
		sqlVdbeAddOp2(vdbe, OP_Integer, idx->def->iid,
				  index_id_reg);
		VdbeComment((vdbe, "Analysis for %s.%s", space_name(space),
			    idx->def->name));

		int part_count = idx->def->key_def->part_count;
		/*
		 * Make sure there are enough memory cells
		 * allocated to accommodate the prev_reg array
		 * and a trailing key (the key slot is required
		 * when building a record to insert into
		 * the sample column of the _sql_stat4 table).
		 */
		parse->nMem = MAX(parse->nMem, prev_reg + part_count +
				  pk_part_count);
		/* Open a cursor on the index being analyzed. */
		int idx_cursor;
		if (j != 0) {
			idx_cursor = parse->nTab - 1;
			sqlVdbeAddOp4(vdbe, OP_IteratorOpen, idx_cursor,
				      idx->def->iid, 0, (void *) space,
				      P4_SPACEPTR);
			VdbeComment((vdbe, "%s", idx->def->name));
		} else {
			/* We have already opened cursor on PK. */
			idx_cursor = tab_cursor;
		}
		/*
		 * Invoke the stat_init() function.
		 * The arguments are:
		 *  (1) The number of columns in the index
		 *      (including the number of PK columns);
		 *  (2) The number of columns in the key
		 *       without the pk;
		 *  (3) the number of rows in the index;
		 * FIXME: for Tarantool first and second args
		 * are the same.
		 *
		 * The third argument is only used for STAT4
		 */
		sqlVdbeAddOp2(vdbe, OP_Count, idx_cursor, stat_accum_reg + 3);
		sqlVdbeAddOp2(vdbe, OP_Integer, part_count, stat_accum_reg + 1);
		sqlVdbeAddOp2(vdbe, OP_Integer, part_count, stat_accum_reg + 2);
		sqlVdbeAddOp4(vdbe, OP_Function0, 0, stat_accum_reg + 1,
			      stat_accum_reg, (char *)&statInitFuncdef,
			      P4_FUNCDEF);
		sqlVdbeChangeP5(vdbe, 3);
		int rewind_addr = sqlVdbeAddOp1(vdbe, OP_Rewind, idx_cursor);


		sqlVdbeAddOp2(vdbe, OP_Integer, 0, changed_field_reg);
		int distinct_addr = sqlVdbeMakeLabel(vdbe);
		/* Array of jump instruction addresses. */
		int *jump_addrs = region_alloc(&parse->region,
					       sizeof(int) * part_count);
		if (jump_addrs == NULL) {
			diag_set(OutOfMemory, sizeof(int) * part_count,
				 "region_alloc", "jump_addrs");
			parse->rc = SQL_TARANTOOL_ERROR;
			parse->nErr++;
			return;
		}

		sqlVdbeAddOp0(vdbe, OP_Goto);
		int next_row_addr = sqlVdbeCurrentAddr(vdbe);

		/*
		 * For a single-column UNIQUE index, once we have found a
		 * non-NULL row, we know that all the rest will be
		 * distinct, so skip subsequent distinctness tests.
		 */
		if (part_count == 1 && idx->def->opts.is_unique) {
			sqlVdbeAddOp2(vdbe, OP_NotNull, prev_reg,
				      distinct_addr);
		}

		struct key_part *part = idx->def->key_def->parts;
		for (int i = 0; i < part_count; ++i, ++part) {
			struct coll *coll = part->coll;
			sqlVdbeAddOp2(vdbe, OP_Integer, i, changed_field_reg);
			sqlVdbeAddOp3(vdbe, OP_Column, idx_cursor,
				      part->fieldno, tmp_reg);
			jump_addrs[i] = sqlVdbeAddOp4(vdbe, OP_Ne, tmp_reg, 0,
						      prev_reg + i,
						      (char *)coll, P4_COLLSEQ);
			sqlVdbeChangeP5(vdbe, SQL_NULLEQ);
		}
		sqlVdbeAddOp2(vdbe, OP_Integer, part_count, changed_field_reg);
		sqlVdbeGoto(vdbe, distinct_addr);

		sqlVdbeJumpHere(vdbe, next_row_addr - 1);
		part = idx->def->key_def->parts;
		for (int i = 0; i < part_count; ++i, ++part) {
			sqlVdbeJumpHere(vdbe, jump_addrs[i]);
			sqlVdbeAddOp3(vdbe, OP_Column, idx_cursor,
				      part->fieldno, prev_reg + i);
		}
		sqlVdbeResolveLabel(vdbe, distinct_addr);

		int stat_key_reg = prev_reg + part_count;
		for (int i = 0; i < pk_part_count; i++) {
			uint32_t k = pk->def->key_def->parts[i].fieldno;
			assert(k < space->def->field_count);
			sqlVdbeAddOp3(vdbe, OP_Column, idx_cursor, k,
				      stat_key_reg + i);
			VdbeComment((vdbe, "%s", space->def->fields[k].name));
		}
		sqlVdbeAddOp3(vdbe, OP_MakeRecord, stat_key_reg, pk_part_count,
			      key_reg);
		sqlVdbeAddOp4(vdbe, OP_Function0, 1, stat_accum_reg, tmp_reg,
			      (char *)&statPushFuncdef, P4_FUNCDEF);
		sqlVdbeChangeP5(vdbe, 3);
		sqlVdbeAddOp2(vdbe, OP_Next, idx_cursor, next_row_addr);


		int sample_key_reg = prev_reg + part_count;
		callStatGet(vdbe, stat_accum_reg, STAT_GET_STAT1, stat_reg);
		sqlVdbeAddOp2(vdbe, OP_Integer, 0, tmp_reg);
		int is_null_addr = sqlVdbeMakeLabel(vdbe);
		for (int i = 0; i < SQL_STAT4_SAMPLES; ++i) {
			int next_addr = sqlVdbeCurrentAddr(vdbe);
			callStatGet(vdbe, stat_accum_reg, STAT_GET_KEY,
				    sample_key_reg);
			sqlVdbeAddOp2(vdbe, OP_IsNull, sample_key_reg,
				      is_null_addr);
			callStatGet(vdbe, stat_accum_reg, STAT_GET_NEQ,
				    neq_array_reg + i);
			callStatGet(vdbe, stat_accum_reg, STAT_GET_NLT,
				    nlt_array_reg + i);
			callStatGet(vdbe, stat_accum_reg, STAT_GET_NDLT,
				    ndlt_array_reg + i);
			sqlVdbeAddOp4Int(vdbe, OP_NotFound, tab_cursor,
					 next_addr, sample_key_reg, 0);
			/*
			 * We know that the sample_key_reg row exists
			 * because it was read by the previous loop.
			 * Thus the not-found jump of seekOp will never
			 * be taken.
			 */
			for (int j = 0; j < part_count; j++) {
				uint32_t tabl_col =
					idx->def->key_def->parts[j].fieldno;
				sqlExprCodeGetColumnOfTable(vdbe, space->def,
							    tab_cursor,
							    tabl_col,
							    prev_reg + j);
			}
			sqlVdbeAddOp3(vdbe, OP_MakeRecord, prev_reg, part_count,
					  sample_array_reg + i);
			sqlVdbeChangeP5(vdbe, OPFLAG_MAKE_ARRAY);
			sqlVdbeAddOp2(vdbe, OP_Integer, i + 1, tmp_reg);
		}

		sqlVdbeResolveLabel(vdbe, is_null_addr);
		sqlVdbeAddOp3(vdbe, OP_MakeRecord, neq_array_reg, tmp_reg,
			      neq_reg);
		sqlVdbeChangeP5(vdbe, OPFLAG_P2_IS_REG | OPFLAG_MAKE_ARRAY);
		sqlVdbeAddOp3(vdbe, OP_MakeRecord, nlt_array_reg, tmp_reg,
			      nlt_reg);
		sqlVdbeChangeP5(vdbe, OPFLAG_P2_IS_REG | OPFLAG_MAKE_ARRAY);
		sqlVdbeAddOp3(vdbe, OP_MakeRecord, ndlt_array_reg, tmp_reg,
			      ndlt_reg);
		sqlVdbeChangeP5(vdbe, OPFLAG_P2_IS_REG | OPFLAG_MAKE_ARRAY);
		sqlVdbeAddOp3(vdbe, OP_MakeRecord, sample_array_reg, tmp_reg,
			      sample_reg);
		sqlVdbeChangeP5(vdbe, OPFLAG_P2_IS_REG | OPFLAG_MAKE_ARRAY);
		sqlVdbeAddOp3(vdbe, OP_MakeRecord, space_id_reg, 7, tmp_reg);

		sqlVdbeAddOp4(vdbe, OP_IdxReplace, tmp_reg, 0, 0,
			      (char *)stat, P4_SPACEPTR);
		sqlVdbeJumpHere(vdbe, rewind_addr);
	}
}

/*
 * Generate code that will cause the most recent index analysis to
 * be loaded into internal hash tables where is can be used.
 */
static void
loadAnalysis(Parse * pParse)
{
	Vdbe *v = sqlGetVdbe(pParse);
	if (v) {
		sqlVdbeAddOp1(v, OP_LoadAnalysis, 0);
	}
}

static int
sql_space_foreach_analyze(struct space *space, void *data)
{
	if (space->def->opts.is_view)
		return 0;
	vdbe_emit_analyze_space((struct Parse*)data, space);
	return 0;
}

/**
 * Generate code that will do an analysis of all spaces created
 * via SQL facilities.
 */
static void
sql_analyze_database(struct Parse *parser)
{
	sql_set_multi_write(parser, false);
	vdbe_emit_stat_space_open(parser, BOX_SQL_STAT_ID);
	space_foreach(sql_space_foreach_analyze, (void *)parser);
	loadAnalysis(parser);
}

/**
 * Generate code that will do an analysis of a single table in
 * a database.
 *
 * @param parse Parser context.
 * @param table Target table to analyze.
 */
static void
vdbe_emit_analyze_table(struct Parse *parse, struct space *space)
{
	assert(space != NULL);
	sql_set_multi_write(parse, false);
	/*
	 * There are two system spaces for statistics: _sql_stat1
	 * and _sql_stat4.
	 */
	vdbe_emit_stat_space_open(parse, space->def->id);
	vdbe_emit_analyze_space(parse, space);
	loadAnalysis(parse);
}

/*
 * Generate code for the ANALYZE command.  The parser calls this routine
 * when it recognizes an ANALYZE command.
 *
 *        ANALYZE                            -- 1
 *        ANALYZE  <tablename>               -- 2
 *
 * Form 1 analyzes all indices the single database named.
 * Form 2 analyzes all indices associated with the named table.
 */
void
sqlAnalyze(Parse * pParse, Token * pName)
{
	sql *db = pParse->db;
	if (pName == NULL) {
		/* Form 1:  Analyze everything */
		sql_analyze_database(pParse);
	} else {
		/* Form 2:  Analyze table named */
		char *z = sqlNameFromToken(db, pName);
		if (z != NULL) {
			struct space *sp = space_by_name(z);
			if (sp != NULL) {
				if (sp->def->opts.is_view) {
					sqlErrorMsg(pParse, "VIEW isn't "\
							"allowed to be "\
							"analyzed");
				} else {
					vdbe_emit_analyze_table(pParse, sp);
				}
			} else {
				diag_set(ClientError, ER_NO_SUCH_SPACE, z);
				pParse->rc = SQL_TARANTOOL_ERROR;
				pParse->nErr++;
			}
			sqlDbFree(db, z);
		}
	}
	Vdbe *v = sqlGetVdbe(pParse);
	if (v != NULL)
		sqlVdbeAddOp0(v, OP_Expire);
}

ssize_t
sql_index_tuple_size(struct space *space, struct index *idx)
{
	assert(space != NULL);
	assert(idx != NULL);
	assert(idx->def->space_id == space->def->id);
	ssize_t tuple_count = index_size(idx);
	ssize_t space_size = space_bsize(space);
	ssize_t avg_tuple_size = tuple_count != 0 ?
				 (space_size / tuple_count) : 0;
	return avg_tuple_size;
}

/**
 * Used to pass information from the analyzer reader through
 * to the callback routine.
 */
struct analysis_index_info {
	/** Database handler. */
	sql *db;
	/*** Array of statistics for each index. */
	struct index_stat *stats;
	/** Ordinal number of index to be processed. */
	uint32_t index_count;
};

/**
 * The first argument points to a nul-terminated string
 * containing a list of space separated integers. Load
 * the first stat_size of these into the output arrays.
 *
 * @param stat_string String containing array of integers.
 * @param stat_size Size of output arrays.
 * @param[out] stat_exact Decoded array of statistics.
 * @param[out] stat_log Decoded array of stat logariphms.
 */
static void
decode_stat_string(const char *stat_string, int stat_size, tRowcnt *stat_exact,
		   LogEst *stat_log) {
	const char *z = stat_string;
	if (z == NULL)
		z = "";
	for (int i = 0; *z && i < stat_size; i++) {
		tRowcnt v = 0;
		int c;
		while ((c = z[0]) >= '0' && c <= '9') {
			v = v * 10 + c - '0';
			z++;
		}
		if (stat_exact != NULL)
			stat_exact[i] = v;
		if (stat_log != NULL)
			stat_log[i] = sqlLogEst(v);
		if (*z == ' ')
			z++;
	}
}

/**
 * This callback is invoked once for each index when reading the
 * _sql_stat1 table.
 *
 *     argv[0] = name of the table
 *     argv[1] = name of the index (might be NULL)
 *     argv[2] = results of analysis - array of integers
 *
 * Entries for which argv[1]==NULL simply record the number
 * of rows in the table. This routine also allocates memory
 * for stat struct itself and statistics which are not related
 * to stat4 samples.
 *
 * @param data Additional analysis info.
 * @param argc Number of arguments.
 * @param argv Callback arguments.
 * @retval 0 on success, -1 otherwise.
 */
static int
load_stat1(struct index_stat *stat, struct index *index, const char *stat1_str)
{
	/*
	 * Additional field is used to describe total
	 * count of tuples in index. Although now all
	 * indexes feature the same number of tuples,
	 * partial indexes are going to be implemented
	 * someday.
	 */
	uint32_t column_count = index->def->key_def->part_count + 1;
	/*
	 * Stat arrays may already be set here if there
	 * are duplicate _sql_stat1 entries for this
	 * index. In that case just clobber the old data
	 * with the new instead of allocating a new array.
	 */
	uint32_t stat1_size = column_count * sizeof(uint32_t);
	stat->tuple_stat1 = region_alloc(&fiber()->gc, stat1_size);
	if (stat->tuple_stat1 == NULL) {
		diag_set(OutOfMemory, stat1_size, "region", "tuple_stat1");
		return -1;
	}
	stat->tuple_log_est = region_alloc(&fiber()->gc, stat1_size);
	if (stat->tuple_log_est == NULL) {
		diag_set(OutOfMemory, stat1_size, "region", "tuple_log_est");
		return -1;
	}
	decode_stat_string(stat1_str, column_count, stat->tuple_stat1,
			   stat->tuple_log_est);
	stat->is_unordered = false;
	stat->skip_scan_enabled = true;
	const char *z = stat1_str;
	/* Position ptr at the end of stat string. */
	for (; *z == ' ' || (*z >= '0' && *z <= '9'); ++z);
	while (z[0]) {
		if (sql_strlike_cs("unordered%", z, '[') == 0)
			index->def->opts.stat->is_unordered = true;
		else if (sql_strlike_cs("noskipscan%", z, '[') == 0)
			index->def->opts.stat->skip_scan_enabled = false;
		while (z[0] != 0 && z[0] != ' ')
			z++;
		while (z[0] == ' ')
			z++;
	}
	return 0;
}

/**
 * Calculate avg_eq array based on the samples from index.
 * Some *magic* calculations happen here.
 */
static void
init_avg_eq(struct index *index, struct index_stat *stat)
{
	assert(stat != NULL);
	struct index_sample *samples = stat->samples;
	uint32_t sample_count = stat->sample_count;
	uint32_t field_count = stat->sample_field_count;
	struct index_sample *last_sample = &samples[sample_count - 1];
	if (field_count > 1)
		stat->avg_eq[--field_count] = 1;
	for (uint32_t i = 0; i < field_count; ++i) {
		uint32_t column_count = index->def->key_def->part_count;
		tRowcnt eq_sum = 0;
		tRowcnt eq_avg = 0;
		uint32_t tuple_count = index->vtab->size(index);
		uint64_t distinct_tuple_count;
		uint64_t terms_sum = 0;
		if (i >= column_count || stat->tuple_stat1[i + 1] == 0) {
			distinct_tuple_count = 100 * last_sample->dlt[i];
			sample_count--;
		} else {
			assert(stat->tuple_stat1 != NULL);
			distinct_tuple_count = (100 * tuple_count) /
				stat->tuple_stat1[i + 1];
		}
		for (uint32_t j = 0; j < sample_count; ++j) {
			if (j == (stat->sample_count - 1) ||
			    samples[j].dlt[i] != samples[j + 1].dlt[i]) {
				eq_sum += samples[j].eq[i];
				terms_sum += 100;
			}
		}
		if (distinct_tuple_count > terms_sum) {
			eq_avg = 100 * (tuple_count - eq_sum) /
				(distinct_tuple_count - terms_sum);
		}
		if (eq_avg == 0)
			eq_avg = 1;
		stat->avg_eq[i] = eq_avg;
	}
}

/**
 * Given two key arguments, compare there payloads.
 * This is a simple wrapper around key_compare() to support
 * qsort() interface.
 */
static int
sample_compare(const void *a, const void *b, void *arg)
{
	struct key_def *def = (struct key_def *)arg;
	return key_compare(((struct index_sample *) a)->sample_key,
			   ((struct index_sample *) b)->sample_key, def);
}

/**
 * Load the content from the _sql_stat4 table
 * into the relevant index->stat->samples[] arrays.
 *
 * Arguments must point to SQL statements that return
 * data equivalent to the following:
 *
 *    prepare: SELECT tbl,idx,count(*) FROM _sql_stat4 GROUP BY tbl,idx;
 *    load: SELECT tbl,idx,neq,nlt,ndlt,sample FROM _sql_stat4;
 *
 * 'prepare' statement is used to allocate enough memory for
 * statistics (i.e. arrays lt, dt, dlt and avg_eq). 'load' query
 * is needed for
 *
 * @param db Database handler.
 * @param sql_select_prepare SELECT statement, see above.
 * @param sql_select_load SELECT statement, see above.
 * @param[out] stats Statistics is saved here.
 * @retval 0 on success, -1 otherwise.
 */
static int
load_stat_from_space(struct sql *db, struct index **indexes,
		     struct index_stat *stats)
{
	const char *load_query = "SELECT \"space_id\", \"index_id\", "\
				 "\"stat\", \"neq\", \"nlt\", \"ndlt\", "\
				 "\"sample\" FROM \"_sql_stat\";";
	sql_stmt *stmt = NULL;
	if (sql_prepare(db, load_query, -1, &stmt, 0) < 0)
		return -1;
	uint32_t current_idx_count = 0;
	while (sql_step(stmt) == SQL_ROW) {
		uint32_t space_id = sql_column_int(stmt, 0);
		if (space_id == 0)
			continue;
		struct space *space = space_by_id(space_id);
		if (space == NULL)
			continue;
		uint32_t iid = sql_column_int(stmt, 1);
		struct index *index = space_index(space, iid);
		if (index == NULL)
			continue;
		const char *stat1 = (char *)sql_column_text(stmt, 2);
		if (stat1 == NULL)
			continue;
		const char *neq = (const char *)sql_column_blob(stmt, 3);
		const char *nlt = (const char *)sql_column_blob(stmt, 4);
		const char *ndlt = (const char *)sql_column_blob(stmt, 5);
		const char *sample_key = (const char *)sql_column_blob(stmt, 6);

		uint32_t column_count = index->def->key_def->part_count;

		uint32_t sample_count = mp_decode_array(&neq);
		mp_decode_array(&nlt);
		mp_decode_array(&ndlt);
		mp_decode_array(&sample_key);

		struct index_stat *stat = &stats[current_idx_count];
		stat->sample_field_count = column_count;
		stat->sample_count = 0;

		/* Space for sample structs. */
		size_t alloc_size = sizeof(struct index_sample) * sample_count;
		/* Space for eq, lt and dlt stats. */
		alloc_size += sizeof(tRowcnt) * column_count * 3 * sample_count;
		/* Space for avg_eq statistics. */
		alloc_size += column_count * sizeof(tRowcnt);
		/*
		 * We are trying to fit into one chunk samples,
		 * eq_avg and arrays of eq, lt and dlt stats.
		 * Firstly comes memory for structs of samples,
		 * than array of eq_avg and finally arrays of
		 * eq, lt and dlt stats.
		 */
		stat->samples = region_alloc(&fiber()->gc, alloc_size);
		if (stat->samples == NULL) {
			diag_set(OutOfMemory, alloc_size, "region", "samples");
			return -1;
		}
		memset(stat->samples, 0, alloc_size);
		/* Marking memory manually. */
		tRowcnt *pSpace = (tRowcnt *) & stat->samples[sample_count];
		stat->avg_eq = pSpace;
		pSpace += column_count;
		for (uint32_t i = 0; i < sample_count; i++) {
			stat->samples[i].eq = pSpace;
			pSpace += column_count;
			stat->samples[i].lt = pSpace;
			pSpace += column_count;
			stat->samples[i].dlt = pSpace;
			pSpace += column_count;

		}
		indexes[current_idx_count] = index;

		if (load_stat1(stat, index, stat1) < 0)
			return -1;
		for (uint32_t i = 0; i < sample_count; ++i) {
			struct index_sample *sample = &stat->samples[i];
			const char *data;
			uint32_t data_len;
			data = mp_decode_str(&neq, &data_len);
			decode_stat_string(data, column_count, sample->eq, 0);
			data = mp_decode_str(&nlt, &data_len);
			decode_stat_string(data, column_count, sample->lt, 0);
			data = mp_decode_str(&ndlt, &data_len);
			decode_stat_string(data, column_count, sample->dlt, 0);
			const char *key = sample_key;
			mp_next(&sample_key);
			sample->key_size = sample_key - key;
			sample->sample_key = region_alloc(&fiber()->gc,
							  sample->key_size);
			if (sample->sample_key == NULL) {
				sql_finalize(stmt);
				diag_set(OutOfMemory, sample->key_size,
					 "region", "sample_key");
				return -1;
			}
			if (sample->key_size > 0) {
				memcpy(sample->sample_key, key,
				       sample->key_size);
			}
			stats[current_idx_count].sample_count++;
		}
		init_avg_eq(index, &stats[current_idx_count]);
		indexes[current_idx_count] = index;
		current_idx_count++;
	}
	for (uint32_t i = 0; i < current_idx_count; ++i) {
		qsort_arg(stats[i].samples,
			  stats[i].sample_count,
			  sizeof(struct index_sample),
			  sample_compare, indexes[i]->def->key_def);
	}
	return current_idx_count;
}

/**
 * This function performs copy of statistics.
 * In contrast to index_stat_dup(), there is no assumption
 * that source statistics is allocated within chunk. But
 * destination place is still one big chunk of heap memory.
 * See also index_stat_sizeof() for understanding memory layout.
 *
 * @param dest One chunk of memory where statistics
 *             will be placed.
 * @param src Statistics to be copied.
 */
static void
stat_copy(struct index_stat *dest, const struct index_stat *src)
{
	assert(dest != NULL);
	assert(src != NULL);
	dest->sample_count = src->sample_count;
	dest->sample_field_count = src->sample_field_count;
	dest->skip_scan_enabled = src->skip_scan_enabled;
	dest->is_unordered = src->is_unordered;
	uint32_t array_size = src->sample_field_count * sizeof(uint32_t);
	uint32_t stat1_offset = sizeof(struct index_stat);
	char *pos = (char *) dest + stat1_offset;
	memcpy(pos, src->tuple_stat1, array_size + sizeof(uint32_t));
	dest->tuple_stat1 = (uint32_t *) pos;
	pos += array_size + sizeof(uint32_t);
	memcpy(pos, src->tuple_log_est, array_size + sizeof(uint32_t));
	dest->tuple_log_est = (log_est_t *) pos;
	pos += array_size + sizeof(uint32_t);
	memcpy(pos, src->avg_eq, array_size);
	dest->avg_eq = (uint32_t *) pos;
	pos += array_size;
	dest->samples = (struct index_sample *) pos;
	pos += dest->sample_count * sizeof(struct index_sample);
	for (uint32_t i = 0; i < dest->sample_count; ++i) {
		dest->samples[i].key_size = src->samples[i].key_size;
		memcpy(pos, src->samples[i].eq, array_size);
		dest->samples[i].eq = (uint32_t *) pos;
		pos += array_size;
		memcpy(pos, src->samples[i].lt, array_size);
		dest->samples[i].lt = (uint32_t *) pos;
		pos += array_size;
		memcpy(pos, src->samples[i].dlt, array_size);
		dest->samples[i].dlt = (uint32_t *) pos;
		pos += array_size;
		memcpy(pos, src->samples[i].sample_key,
		       src->samples[i].key_size);
		dest->samples[i].sample_key = pos;
		pos += dest->samples[i].key_size;
	}
}

static int
load_stat_to_index(struct index **indexes, int index_count,
		   struct index_stat *stats)
{
	/*
	 * Now we have complete statistics for each index
	 * allocated on the region. Time to copy it on the heap.
	 */
	size_t heap_stats_size = index_count * sizeof(struct index_stat *);
	struct index_stat **heap_stats = region_alloc(&fiber()->gc,
						      heap_stats_size);
	if (heap_stats == NULL) {
		diag_set(OutOfMemory, heap_stats_size, "region_alloc",
			 "heap_stats");
		return -1;
	}
	/*
	 * We are using 'everything or nothing' policy:
	 * if there is no enough memory for statistics even for
	 * one index, then refresh it for no one.
	 */
	for (int i = 0; i < index_count; ++i) {
		size_t size = index_stat_sizeof(stats[i].samples,
						stats[i].sample_count,
						stats[i].sample_field_count);
		heap_stats[i] = malloc(size);
		if (heap_stats[i] == NULL) {
			diag_set(OutOfMemory, size, "malloc", "heap_stats");
			for (int j = 0; j < i; ++j)
				free(heap_stats[j]);
			return -1;
		}
	}
	/*
	 * We can't use stat_dup function since statistics on
	 * region doesn't fit into one memory chunk. Lets
	 * manually copy memory chunks and mark memory.
	 */
	for (int i = 0; i < index_count; ++i)
		stat_copy(heap_stats[i], &stats[i]);
	/* Load stats to index. */
	for (int i = 0; i < index_count; ++i) {
		free(indexes[i]->def->opts.stat);
		indexes[i]->def->opts.stat = heap_stats[i];
	}
	return 0;
}

/**
 * default_tuple_est[] array contains default information
 * which is used when we don't have real space, e.g. temporary
 * objects representing result set of nested SELECT or VIEW.
 *
 * First number is supposed to contain the number of elements
 * in the index. Since we do not know, guess 1 million.
 * Second one is an estimate of the number of rows in the
 * table that match any particular value of the first column of
 * the index. Third one is an estimate of the number of
 * rows that match any particular combination of the first 2
 * columns of the index. And so on. It must always be true:
 *
 *           default_tuple_est[N] <= default_tuple_est[N-1]
 *           default_tuple_est[N] >= 1
 *
 * Apart from that, we have little to go on besides intuition
 * as to how default values should be initialized. The numbers
 * generated here are based on typical values found in actual
 * indices.
 */
const log_est_t default_tuple_est[] = {DEFAULT_TUPLE_LOG_COUNT,
/**                  [10*log_{2}(x)]:  10, 9,  8,  7,  6,  5 */
				       33, 32, 30, 28, 26, 23};

LogEst
sql_space_tuple_log_count(struct space *space)
{
	if (space == NULL || space->index_map == NULL)
		return 0;

	struct index *pk = space_index(space, 0);
	assert(sqlLogEst(DEFAULT_TUPLE_COUNT) == DEFAULT_TUPLE_LOG_COUNT);
	/* If space represents VIEW, return default number. */
	if (pk == NULL)
		return DEFAULT_TUPLE_LOG_COUNT;
	return sqlLogEst(pk->vtab->size(pk));
}

log_est_t
index_field_tuple_est(const struct index_def *idx_def, uint32_t field)
{
	assert(idx_def != NULL);
	struct space *space = space_by_id(idx_def->space_id);
	if (space == NULL || strcmp(idx_def->name, "fake_autoindex") == 0)
		return idx_def->opts.stat->tuple_log_est[field];
	assert(field <= idx_def->key_def->part_count);
	/* Statistics is held only in real indexes. */
	struct index *tnt_idx = space_index(space, idx_def->iid);
	assert(tnt_idx != NULL);
	if (tnt_idx->def->opts.stat == NULL) {
		/*
		 * Last number for unique index is always 0:
		 * only one tuple exists with given full key
		 * in unique index and log(1) == 0.
		 */
		if (field == idx_def->key_def->part_count &&
		    idx_def->opts.is_unique)
			return 0;
		return default_tuple_est[field + 1 >= 6 ? 6 : field];
	}
	return tnt_idx->def->opts.stat->tuple_log_est[field];
}

int
sql_analysis_load(struct sql *db)
{
	ssize_t index_count = box_index_len(BOX_SQL_STAT_ID, 0);
	if (index_count < 0)
		return SQL_TARANTOOL_ERROR;
	if (box_txn_begin() != 0)
		goto fail;
	if (index_count == 0) {
		box_txn_commit();
		return SQL_OK;
	}
	size_t stats_size = index_count * sizeof(struct index_stat);
	struct index_stat *stats = region_alloc(&fiber()->gc, stats_size);
	if (stats == NULL) {
		diag_set(OutOfMemory, stats_size, "region", "stats");
		goto fail;
	}
	memset(stats, 0, stats_size);

	size_t indexes_size = index_count * sizeof(struct index *);
	struct index **indexes = region_alloc(&fiber()->gc, indexes_size);
	if (stats == NULL) {
		diag_set(OutOfMemory, indexes_size, "region", "indexes");
		goto fail;
	}
	memset(indexes, 0, indexes_size);

	/* Load the statistics from the _sql_stat table. */
	int index_count_new = load_stat_from_space(db, indexes, stats);
	if (index_count_new < 0)
		goto fail;
	if (load_stat_to_index(indexes, index_count_new, stats) != 0)
		goto fail;
	if (box_txn_commit() != 0)
		return SQL_TARANTOOL_ERROR;
	return SQL_OK;
fail:
	box_txn_rollback();
	return SQL_TARANTOOL_ERROR;
}
