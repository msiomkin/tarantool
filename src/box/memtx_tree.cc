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
#include "memtx_tree.h"
#include "memtx_engine.h"
#include "space.h"
#include "schema.h" /* space_cache_find() */
#include "errinj.h"
#include "memory.h"
#include "fiber.h"
#include "tuple.h"
#include <third_party/qsort_arg.h>
#include <small/mempool.h>
#include "memtx_tree_proxy.h"

struct memtx_tree_index {
	struct index base;
	TreeProxy tree;
	MemtxTreeData *build_array;
	size_t build_array_size, build_array_alloc_size;
	struct memtx_gc_task gc_task;
	TreeProxy::iterator gc_iterator;
};

/* {{{ Utilities. *************************************************/

static int
memtx_tree_qcompare(const void* a, const void *b, void *c)
{
	MemtxTreeData *elem_a = (MemtxTreeData *)a;
	return elem_a->compare((MemtxTreeData *)b, (struct key_def *)c);
}

/* {{{ MemtxTree Iterators ****************************************/
struct tree_iterator {
	struct iterator base;
	TreeProxy *tree;
	struct index_def *index_def;
	TreeProxy::iterator tree_iterator;
	enum iterator_type type;
	MemtxTreeKeyData key_data;
	MemtxTreeData current;
	/** Memory pool the iterator was allocated from. */
	struct mempool *pool;
};

static_assert(sizeof(struct tree_iterator) <= MEMTX_ITERATOR_SIZE,
	      "sizeof(struct tree_iterator) must be less than or equal "
	      "to MEMTX_ITERATOR_SIZE");

static void
tree_iterator_free(struct iterator *iterator);

static inline struct tree_iterator *
tree_iterator_cast(struct iterator *it)
{
	assert(it->free == &tree_iterator_free);
	return (struct tree_iterator *) it;
}

static void
tree_iterator_free(struct iterator *iterator)
{
	struct tree_iterator *it = tree_iterator_cast(iterator);
	struct tuple *tuple = it->current.get();
	if (tuple != NULL)
		tuple_unref(tuple);
	mempool_free(it->pool, it);
}

static int
tree_iterator_dummie(struct iterator *iterator, struct tuple **ret)
{
	(void)iterator;
	*ret = NULL;
	return 0;
}

static int
tree_iterator_next(struct iterator *iterator, struct tuple **ret)
{
	struct tree_iterator *it = tree_iterator_cast(iterator);
	assert(it->current.get() != NULL);
	MemtxTreeData *check = it->tree->iterator_get_elem(&it->tree_iterator);
	if (check == NULL || !check->is_identical(&it->current)) {
		it->tree_iterator =
			it->tree->upper_bound_elem(&it->current, NULL);
	} else {
		it->tree->iterator_next(&it->tree_iterator);
	}
	tuple_unref(it->current.get());
	MemtxTreeData *res =
		it->tree->iterator_get_elem(&it->tree_iterator);
	if (res == NULL) {
		iterator->next = tree_iterator_dummie;
		it->current.clear();
		*ret = NULL;
	} else {
		*ret = res->get();
		tuple_ref(*ret);
		it->current = *res;
	}
	return 0;
}

static int
tree_iterator_prev(struct iterator *iterator, struct tuple **ret)
{
	struct tree_iterator *it = tree_iterator_cast(iterator);
	assert(it->current.get() != NULL);
	MemtxTreeData *check = it->tree->iterator_get_elem(&it->tree_iterator);
	if (check == NULL || !check->is_identical(&it->current)) {
		it->tree_iterator =
			it->tree->lower_bound_elem(&it->current, NULL);
	}
	it->tree->iterator_prev(&it->tree_iterator);
	tuple_unref(it->current.get());
	MemtxTreeData *res =
		it->tree->iterator_get_elem(&it->tree_iterator);
	if (!res) {
		iterator->next = tree_iterator_dummie;
		it->current.clear();
		*ret = NULL;
	} else {
		*ret = res->get();
		tuple_ref(*ret);
		it->current = *res;
	}
	return 0;
}

static int
tree_iterator_next_equal(struct iterator *iterator, struct tuple **ret)
{
	struct tree_iterator *it = tree_iterator_cast(iterator);
	assert(it->current.get() != NULL);
	MemtxTreeData *check = it->tree->iterator_get_elem(&it->tree_iterator);
	if (check == NULL || !check->is_identical(&it->current)) {
		it->tree_iterator =
			it->tree->upper_bound_elem(&it->current, NULL);
	} else {
		it->tree->iterator_next(&it->tree_iterator);
	}
	tuple_unref(it->current.get());
	MemtxTreeData *res = it->tree->iterator_get_elem(&it->tree_iterator);
	/* Use user key def to save a few loops. */
	if (res == NULL ||
	    res->compare_with_key(&it->key_data, it->index_def->key_def) != 0) {
		iterator->next = tree_iterator_dummie;
		it->current.clear();
		*ret = NULL;
	} else {
		*ret = res->get();
		tuple_ref(*ret);
		it->current = *res;
	}
	return 0;
}

static int
tree_iterator_prev_equal(struct iterator *iterator, struct tuple **ret)
{
	struct tree_iterator *it = tree_iterator_cast(iterator);
	assert(it->current.get() != NULL);
	MemtxTreeData *check = it->tree->iterator_get_elem(&it->tree_iterator);
	if (check == NULL || !check->is_identical(&it->current)) {
		it->tree_iterator =
			it->tree->lower_bound_elem(&it->current, NULL);
	}
	it->tree->iterator_prev(&it->tree_iterator);
	tuple_unref(it->current.get());
	MemtxTreeData *res = it->tree->iterator_get_elem(&it->tree_iterator);
	/* Use user key def to save a few loops. */
	if (res == NULL ||
	    res->compare_with_key(&it->key_data, it->index_def->key_def) != 0) {
		iterator->next = tree_iterator_dummie;
		it->current.clear();
		*ret = NULL;
	} else {
		*ret = res->get();
		tuple_ref(*ret);
		it->current = *res;
	}
	return 0;
}

static void
tree_iterator_set_next_method(struct tree_iterator *it)
{
	assert(it->current.get() != NULL);
	switch (it->type) {
	case ITER_EQ:
		it->base.next = tree_iterator_next_equal;
		break;
	case ITER_REQ:
		it->base.next = tree_iterator_prev_equal;
		break;
	case ITER_ALL:
		it->base.next = tree_iterator_next;
		break;
	case ITER_LT:
	case ITER_LE:
		it->base.next = tree_iterator_prev;
		break;
	case ITER_GE:
	case ITER_GT:
		it->base.next = tree_iterator_next;
		break;
	default:
		/* The type was checked in initIterator */
		assert(false);
	}
}

static int
tree_iterator_start(struct iterator *iterator, struct tuple **ret)
{
	*ret = NULL;
	struct tree_iterator *it = tree_iterator_cast(iterator);
	it->base.next = tree_iterator_dummie;
	const TreeProxy *tree = it->tree;
	enum iterator_type type = it->type;
	bool exact = false;
	assert(it->current.get() == NULL);
	uint32_t part_count;
	const char *key = it->key_data.get(&part_count);
	if (key == NULL) {
		if (iterator_type_is_reverse(it->type))
			it->tree_iterator = tree->iterator_last();
		else
			it->tree_iterator = tree->iterator_first();
	} else {
		if (type == ITER_ALL || type == ITER_EQ ||
		    type == ITER_GE || type == ITER_LT) {
			it->tree_iterator =
				tree->lower_bound(&it->key_data, &exact);
			if (type == ITER_EQ && !exact)
				return 0;
		} else { // ITER_GT, ITER_REQ, ITER_LE
			it->tree_iterator =
				tree->upper_bound(&it->key_data, &exact);
			if (type == ITER_REQ && !exact)
				return 0;
		}
		if (iterator_type_is_reverse(type)) {
			/*
			 * Because of limitations of tree search API we use use
			 * lower_bound for LT search and upper_bound for LE
			 * and REQ searches. Thus we found position to the
			 * right of the target one. Let's make a step to the
			 * left to reach target position.
			 * If we found an invalid iterator all the elements in
			 * the tree are less (less or equal) to the key, and
			 * iterator_next call will convert the iterator to the
			 * last position in the tree, that's what we need.
			 */
			tree->iterator_prev(&it->tree_iterator);
		}
	}

	MemtxTreeData *res = tree->iterator_get_elem(&it->tree_iterator);
	if (!res)
		return 0;
	*ret = res->get();
	tuple_ref(*ret);
	it->current = *res;
	tree_iterator_set_next_method(it);
	return 0;
}

/* }}} */

/* {{{ MemtxTree  **********************************************************/

/**
 * Return the key def to use for comparing tuples stored
 * in the given tree index.
 *
 * We use extended key def for non-unique and nullable
 * indexes. Unique but nullable index can store multiple
 * NULLs. To correctly compare these NULLs extended key
 * def must be used. For details @sa tuple_compare.cc.
 */
static struct key_def *
memtx_tree_index_cmp_def(struct memtx_tree_index *index)
{
	struct index_def *def = index->base.def;
	return def->opts.is_unique && !def->key_def->is_nullable ?
		def->key_def : def->cmp_def;
}

static void
memtx_tree_index_free(struct memtx_tree_index *index)
{
	index->tree.destroy();
	free(index->build_array);
	free(index);
}

static void
memtx_tree_index_gc_run(struct memtx_gc_task *task, bool *done)
{
	/*
	 * Yield every 1K tuples to keep latency < 0.1 ms.
	 * Yield more often in debug mode.
	 */
#ifdef NDEBUG
	enum { YIELD_LOOPS = 1000 };
#else
	enum { YIELD_LOOPS = 10 };
#endif

	struct memtx_tree_index *index = container_of(task,
			struct memtx_tree_index, gc_task);
	TreeProxy *tree = &index->tree;
	TreeProxy::iterator *itr = &index->gc_iterator;

	unsigned int loops = 0;
	while (!tree->iterator_is_invalid(itr)) {
		MemtxTreeData *res = tree->iterator_get_elem(itr);
		struct tuple *tuple = res->get();
		tree->iterator_next(itr);
		tuple_unref(tuple);
		if (++loops >= YIELD_LOOPS) {
			*done = false;
			return;
		}
	}
	*done = true;
}

static void
memtx_tree_index_gc_free(struct memtx_gc_task *task)
{
	struct memtx_tree_index *index = container_of(task,
			struct memtx_tree_index, gc_task);
	memtx_tree_index_free(index);
}

static const struct memtx_gc_task_vtab memtx_tree_index_gc_vtab = {
	.run = memtx_tree_index_gc_run,
	.free = memtx_tree_index_gc_free,
};

static void
memtx_tree_index_destroy(struct index *base)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	struct memtx_engine *memtx = (struct memtx_engine *)base->engine;
	if (base->def->iid == 0) {
		/*
		 * Primary index. We need to free all tuples stored
		 * in the index, which may take a while. Schedule a
		 * background task in order not to block tx thread.
		 */
		index->gc_task.vtab = &memtx_tree_index_gc_vtab;
		index->gc_iterator = index->tree.iterator_first();
		memtx_engine_schedule_gc(memtx, &index->gc_task);
	} else {
		/*
		 * Secondary index. Destruction is fast, no need to
		 * hand over to background fiber.
		 */
		memtx_tree_index_free(index);
	}
}

static void
memtx_tree_index_update_def(struct index *base)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	index->tree.set_key_def(memtx_tree_index_cmp_def(index));
}

static bool
memtx_tree_index_depends_on_pk(struct index *base)
{
	struct index_def *def = base->def;
	/* See comment to memtx_tree_index_cmp_def(). */
	return !def->opts.is_unique || def->key_def->is_nullable;
}

static ssize_t
memtx_tree_index_size(struct index *base)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	return index->tree.size();
}

static ssize_t
memtx_tree_index_bsize(struct index *base)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	return index->tree.mem_used();
}

static int
memtx_tree_index_random(struct index *base, uint32_t rnd, struct tuple **result)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	MemtxTreeData *res = index->tree.random(rnd);
	*result = res != NULL ? res->get() : NULL;
	return 0;
}

static ssize_t
memtx_tree_index_count(struct index *base, enum iterator_type type,
		       const char *key, uint32_t part_count)
{
	if (type == ITER_ALL)
		return memtx_tree_index_size(base); /* optimization */
	return generic_index_count(base, type, key, part_count);
}

static int
memtx_tree_index_get(struct index *base, const char *key,
		     uint32_t part_count, struct tuple **result)
{
	assert(base->def->opts.is_unique &&
	       part_count == base->def->key_def->part_count);
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	MemtxTreeKeyData key_data;
	struct key_def *cmp_def = memtx_tree_index_cmp_def(index);
	key_data.set(key, part_count, cmp_def);
	MemtxTreeData *res = index->tree.find(&key_data);
	*result = res != NULL ? res->get() : NULL;
	return 0;
}



static int
memtx_tree_index_replace(struct index *base, struct tuple *old_tuple,
			 struct tuple *new_tuple, enum dup_replace_mode mode,
			 struct tuple **result)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	struct key_def *key_def = index->tree.get_key_def();
	if (new_tuple) {
		/* Try to optimistically replace the new_tuple. */
		MemtxTreeData data;
		data.set(new_tuple, key_def);
		MemtxTreeData dup_data;
		dup_data.clear();
		int tree_res = index->tree.insert(&data, &dup_data);
		if (tree_res) {
			diag_set(OutOfMemory, MEMTX_EXTENT_SIZE,
				 "memtx_tree_index", "replace");
			return -1;
		}
		struct tuple *dup_tuple = dup_data.get();
		uint32_t errcode =
			replace_check_dup(old_tuple, dup_tuple, mode);
		if (errcode) {
			index->tree.remove(&data);
			if (dup_tuple != NULL)
				index->tree.insert(&dup_data, NULL);
			struct space *sp = space_cache_find(base->def->space_id);
			if (sp != NULL)
				diag_set(ClientError, errcode, base->def->name,
					 space_name(sp));
			return -1;
		}
		if (dup_tuple != NULL) {
			*result = dup_tuple;
			return 0;
		}
	}
	if (old_tuple) {
		MemtxTreeData old_data;
		old_data.set(old_tuple, key_def);
		index->tree.remove(&old_data);
	}
	*result = old_tuple;
	return 0;
}

static struct iterator *
memtx_tree_index_create_iterator(struct index *base, enum iterator_type type,
				 const char *key, uint32_t part_count)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	struct memtx_engine *memtx = (struct memtx_engine *)base->engine;

	assert(part_count == 0 || key != NULL);
	if (type > ITER_GT) {
		diag_set(UnsupportedIndexFeature, base->def,
			 "requested iterator type");
		return NULL;
	}

	if (part_count == 0) {
		/*
		 * If no key is specified, downgrade equality
		 * iterators to a full range.
		 */
		type = iterator_type_is_reverse(type) ? ITER_LE : ITER_GE;
		key = NULL;
	}

	struct tree_iterator *it =
		(struct tree_iterator *)mempool_alloc(&memtx->iterator_pool);
	if (it == NULL) {
		diag_set(OutOfMemory, sizeof(struct tree_iterator),
			 "memtx_tree_index", "iterator");
		return NULL;
	}
	iterator_create(&it->base, base);
	it->pool = &memtx->iterator_pool;
	it->base.next = tree_iterator_start;
	it->base.free = tree_iterator_free;
	it->type = type;
	struct key_def *cmp_def = memtx_tree_index_cmp_def(index);
	it->key_data.set(key, part_count, cmp_def);
	it->index_def = base->def;
	it->tree = &index->tree;
	it->tree_iterator = it->tree->invalid_iterator();
	it->current.clear();
	return (struct iterator *)it;
}

static void
memtx_tree_index_begin_build(struct index *base)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	assert(index->tree.size() == 0);
	(void)index;
}

static int
memtx_tree_index_reserve(struct index *base, uint32_t size_hint)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	if (size_hint < index->build_array_alloc_size)
		return 0;
	MemtxTreeData *tmp =
		(MemtxTreeData *)realloc(index->build_array,
					 size_hint * sizeof(*tmp));
	if (tmp == NULL) {
		diag_set(OutOfMemory, size_hint * sizeof(*tmp),
			 "memtx_tree_index", "reserve");
		return -1;
	}
	index->build_array = tmp;
	index->build_array_alloc_size = size_hint;
	return 0;
}

static int
memtx_tree_index_build_next(struct index *base, struct tuple *tuple)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	if (index->build_array == NULL) {
		index->build_array =
			(MemtxTreeData *)malloc(MEMTX_EXTENT_SIZE);
		if (index->build_array == NULL) {
			diag_set(OutOfMemory, MEMTX_EXTENT_SIZE,
				 "memtx_tree_index", "build_next");
			return -1;
		}
		index->build_array_alloc_size =
			MEMTX_EXTENT_SIZE / sizeof(index->build_array[0]);
	}
	assert(index->build_array_size <= index->build_array_alloc_size);
	if (index->build_array_size == index->build_array_alloc_size) {
		index->build_array_alloc_size = index->build_array_alloc_size +
					index->build_array_alloc_size / 2;
		MemtxTreeData *tmp =
			(MemtxTreeData *)realloc(index->build_array,
			index->build_array_alloc_size * sizeof(*tmp));
		if (tmp == NULL) {
			diag_set(OutOfMemory, index->build_array_alloc_size *
				 sizeof(*tmp), "memtx_tree_index", "build_next");
			return -1;
		}
		index->build_array = tmp;
	}
	MemtxTreeData *elem =
		&index->build_array[index->build_array_size++];
	elem->set(tuple, memtx_tree_index_cmp_def(index));
	return 0;
}

static void
memtx_tree_index_end_build(struct index *base)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	struct key_def *cmp_def = memtx_tree_index_cmp_def(index);
	qsort_arg(index->build_array, index->build_array_size,
		  sizeof(index->build_array[0]), memtx_tree_qcompare, cmp_def);
	index->tree.build(index->build_array, index->build_array_size);

	free(index->build_array);
	index->build_array = NULL;
	index->build_array_size = 0;
	index->build_array_alloc_size = 0;
}

struct tree_snapshot_iterator {
	struct snapshot_iterator base;
	TreeProxy *tree;
	TreeProxy::iterator tree_iterator;
};

static void
tree_snapshot_iterator_free(struct snapshot_iterator *iterator)
{
	assert(iterator->free == tree_snapshot_iterator_free);
	struct tree_snapshot_iterator *it =
		(struct tree_snapshot_iterator *)iterator;
	it->tree->iterator_destroy(&it->tree_iterator);
	free(iterator);
}

static const char *
tree_snapshot_iterator_next(struct snapshot_iterator *iterator, uint32_t *size)
{
	assert(iterator->free == tree_snapshot_iterator_free);
	struct tree_snapshot_iterator *it =
		(struct tree_snapshot_iterator *)iterator;
	MemtxTreeData *res = it->tree->iterator_get_elem(&it->tree_iterator);
	if (res == NULL)
		return NULL;
	it->tree->iterator_next(&it->tree_iterator);
	return tuple_data_range(res->get(), size);
}

/**
 * Create an ALL iterator with personal read view so further
 * index modifications will not affect the iteration results.
 * Must be destroyed by iterator->free after usage.
 */
static struct snapshot_iterator *
memtx_tree_index_create_snapshot_iterator(struct index *base)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	struct tree_snapshot_iterator *it = (struct tree_snapshot_iterator *)
		calloc(1, sizeof(*it));
	if (it == NULL) {
		diag_set(OutOfMemory, sizeof(struct tree_snapshot_iterator),
			 "memtx_tree_index", "create_snapshot_iterator");
		return NULL;
	}

	it->base.free = tree_snapshot_iterator_free;
	it->base.next = tree_snapshot_iterator_next;
	it->tree = &index->tree;
	it->tree_iterator = it->tree->iterator_first();
	it->tree->iterator_freeze(&it->tree_iterator);
	return (struct snapshot_iterator *) it;
}

static const struct index_vtab memtx_tree_index_vtab = {
	/* .destroy = */ memtx_tree_index_destroy,
	/* .commit_create = */ generic_index_commit_create,
	/* .abort_create = */ generic_index_abort_create,
	/* .commit_modify = */ generic_index_commit_modify,
	/* .commit_drop = */ generic_index_commit_drop,
	/* .update_def = */ memtx_tree_index_update_def,
	/* .depends_on_pk = */ memtx_tree_index_depends_on_pk,
	/* .def_change_requires_rebuild = */
		memtx_index_def_change_requires_rebuild,
	/* .size = */ memtx_tree_index_size,
	/* .bsize = */ memtx_tree_index_bsize,
	/* .min = */ generic_index_min,
	/* .max = */ generic_index_max,
	/* .random = */ memtx_tree_index_random,
	/* .count = */ memtx_tree_index_count,
	/* .get = */ memtx_tree_index_get,
	/* .replace = */ memtx_tree_index_replace,
	/* .create_iterator = */ memtx_tree_index_create_iterator,
	/* .create_snapshot_iterator = */
		memtx_tree_index_create_snapshot_iterator,
	/* .stat = */ generic_index_stat,
	/* .compact = */ generic_index_compact,
	/* .reset_stat = */ generic_index_reset_stat,
	/* .begin_build = */ memtx_tree_index_begin_build,
	/* .reserve = */ memtx_tree_index_reserve,
	/* .build_next = */ memtx_tree_index_build_next,
	/* .end_build = */ memtx_tree_index_end_build,
};

struct index *
memtx_tree_index_new(struct memtx_engine *memtx, struct index_def *def)
{
	struct memtx_tree_index *index =
		(struct memtx_tree_index *)calloc(1, sizeof(*index));
	if (index == NULL) {
		diag_set(OutOfMemory, sizeof(*index),
			 "malloc", "struct memtx_tree_index");
		return NULL;
	}
	if (index_create(&index->base, (struct engine *)memtx,
			 &memtx_tree_index_vtab, def) != 0) {
		free(index);
		return NULL;
	}

	struct key_def *cmp_def = memtx_tree_index_cmp_def(index);
	index->tree.create(cmp_def, memtx_index_extent_alloc,
			   memtx_index_extent_free, memtx);
	return &index->base;
}
