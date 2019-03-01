#ifndef TARANTOOL_BOX_MEMTX_TREE_PROXY_H_INCLUDED
#define TARANTOOL_BOX_MEMTX_TREE_PROXY_H_INCLUDED
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
#include "stdint.h"
#include "tuple_hint.h"
#include "tuple_compare.h"

/**
 * Struct that is used as a key in BPS tree definition.
 */
class MemtxTreeKeyData {
	/** Sequence of msgpacked search fields. */
	const char *key;
	/** Number of msgpacked search fields. */
	uint32_t part_count;
	/**
	 * Comparison hint. Calculated automatically on method
	 * MemtxTreeKeyData::set.
	 */
	uint64_t hint;
public:
	/**
	 * Return "payload" data that this object stores:
	 * key and part_count.
	 */
	const char *
	get(uint32_t *part_count) const
	{
		*part_count = this->part_count;
		return this->key;
	}

	/**
	 * Return comparasion hint is calculated before "payload"
	 * store on MemtxTreeKeyData::set call.
	 */
	uint64_t
	get_hint(void) const
	{
		return this->hint;
	}

	/**
	 * Perform this MemtxTreeKeyData object
	 * initialization.
	 */
	void
	set(const char *key, uint32_t part_count, struct key_def *key_def)
	{
		this->key = key;
		this->part_count = part_count;
		this->hint = part_count > 0 ? key_hint(key, key_def) : 0;
	}
};

/**
 * Struct that is used as a node in BPS tree definition.
 */
class MemtxTreeData {
	/** Data tuple pointer. */
	struct tuple *tuple;
	/**
	 * Comparison hint. Calculated automatically on method
	 * MemtxTreeData::set.
	 */
	uint64_t hint;
public:
	/**
	 * Return "payload" data that this object stores:
	 * tuple.
	 */
	struct tuple *
	get(void) const
	{
		return tuple;
	}

	/** Perform this MemtxTreeData object initialization. */
	void
	set(struct tuple *tuple, struct key_def *key_def)
	{
		(void)key_def;
		this->tuple = tuple;
		this->hint = tuple_hint(tuple, key_def);
	}

	/** Clear this MemtxTreeData object. */
	void
	clear(void)
	{
		this->tuple = NULL;
	}

	/**
	 * Test if this MemtxTreeData and elem MemtxTreeData
	 * represent exactly the same data.
	 * While MemtxTreeData::compare performs binary data
	 * comparison, this helper checks if the elements are
	 * identical, i.e. initialized with the same arguments.
	 */
	bool
	is_identical(const MemtxTreeData *elem) const
	{
		return this->tuple == elem->tuple;
	}

	/**
	 * Compare this MemtxTreeData object with other elem
	 * MemtxTreeData using the key definition is specified.
	 */
	int
	compare(const MemtxTreeData *elem, struct key_def *key_def) const
	{
		if (likely(this->hint != elem->hint &&
			   this->hint != INVALID_HINT &&
			   elem->hint != INVALID_HINT)) {
			return this->hint < elem->hint ? -1 : 1;
		} else {
			return tuple_compare(this->tuple, elem->tuple, key_def);
		}
	}

	/**
	 * Compare this MemtxTreeData object with key
	 * MemtxTreeKeyData using the key definition is specified.
	 */
	int
	compare_with_key(const MemtxTreeKeyData *key,
			 struct key_def *key_def) const
	{
		uint32_t part_count;
		const char *key_data = key->get(&part_count);
		uint64_t key_hint = key->get_hint();
		if (likely(this->hint != key_hint &&
			   this->hint != INVALID_HINT &&
			   key_hint != INVALID_HINT)) {
			return this->hint < key_hint ? -1 : 1;
		} else {
			return tuple_compare_with_key(this->tuple, key_data,
						      part_count, key_def);
		}
	}
};

#define BPS_TREE_NAME memtx_tree
#define BPS_TREE_BLOCK_SIZE (512)
#define BPS_TREE_EXTENT_SIZE MEMTX_EXTENT_SIZE
#define BPS_TREE_COMPARE(elem_a, elem_b, key_def)				\
	(&elem_a)->compare(&elem_b, key_def)
#define BPS_TREE_COMPARE_KEY(elem, key_ptr, key_def)				\
	(&elem)->compare_with_key(key_ptr, key_def)
#define BPS_TREE_IDENTICAL(elem_a, elem_b) (&elem_a)->is_identical(&elem_b)
#define bps_tree_elem_t MemtxTreeData
#define bps_tree_key_t MemtxTreeKeyData *
#define bps_tree_arg_t struct key_def *

#include "salad/bps_tree.h"

#undef BPS_TREE_NAME
#undef BPS_TREE_BLOCK_SIZE
#undef BPS_TREE_EXTENT_SIZE
#undef BPS_TREE_COMPARE
#undef BPS_TREE_COMPARE_KEY
#undef BPS_TREE_IDENTICAL
#undef bps_tree_elem_t
#undef bps_tree_key_t
#undef bps_tree_arg_t

class TreeProxy {
public:
	struct iterator	{
		memtx_tree_iterator iterator;
	};

	void
	create(struct key_def *def, bps_tree_extent_alloc_f alloc,
	       bps_tree_extent_free_f free, void *alloc_ctx)
	{
		memtx_tree_create(&tree, def, alloc, free, alloc_ctx);
	}

	void
	destroy(void)
	{
		memtx_tree_destroy(&tree);
	}

	class MemtxTreeData *
	iterator_get_elem(struct iterator *it) const
	{
		return memtx_tree_iterator_get_elem(&tree, &it->iterator);
	}

	class MemtxTreeData *
	random(uint32_t seed) const
	{
		return memtx_tree_random(&tree, seed);
	}

	class MemtxTreeData *
	find(MemtxTreeKeyData *key_data) const
	{
		return memtx_tree_find(&tree, key_data);
	}

	struct iterator
	lower_bound_elem(const MemtxTreeData *data, bool *exact) const
	{
		iterator result;
		result.iterator =
			memtx_tree_lower_bound_elem(&tree, *data, exact);
		return result;
	}

	struct iterator
	upper_bound_elem(const MemtxTreeData *data, bool *exact) const
	{
		iterator result;
		result.iterator =
			memtx_tree_upper_bound_elem(&tree, *data, exact);
		return result;
	}

	struct iterator
	lower_bound(MemtxTreeKeyData *data, bool *exact) const
	{
		iterator result;
		result.iterator = memtx_tree_lower_bound(&tree, data, exact);
		return result;
	}

	struct iterator
	upper_bound(MemtxTreeKeyData *data, bool *exact) const
	{
		struct iterator result;
		result.iterator = memtx_tree_upper_bound(&tree, data, exact);
		return result;
	}

	struct iterator
	iterator_first(void) const
	{
		struct iterator result;
		result.iterator = memtx_tree_iterator_first(&tree);
		return result;
	}

	struct iterator
	iterator_last() const
	{
		struct iterator result;
		result.iterator = memtx_tree_iterator_last(&tree);
		return result;
	}

	struct iterator
	invalid_iterator(void) const
	{
		struct iterator result;
		result.iterator = memtx_tree_invalid_iterator();
		return result;
	}

	void
	iterator_freeze(iterator *itr)
	{
		memtx_tree_iterator_freeze(&tree, &itr->iterator);
	}

	void
	iterator_destroy(iterator *itr)
	{
		memtx_tree_iterator_destroy(&tree, &itr->iterator);
	}

	bool
	iterator_next(iterator *itr) const
	{
		return memtx_tree_iterator_next(&tree, &itr->iterator);
	}

	bool
	iterator_prev(iterator *itr) const
	{
		return memtx_tree_iterator_prev(&tree, &itr->iterator);
	}

	bool
	iterator_is_invalid(iterator *itr) const
	{
		return memtx_tree_iterator_is_invalid(&itr->iterator);
	}

	size_t
	size(void) const
	{
		return memtx_tree_size(&tree);
	}

	size_t
	mem_used(void) const
	{
		return memtx_tree_mem_used(&tree);
	}

	int
	build(MemtxTreeData *data, size_t size)
	{
		return memtx_tree_build(&tree, data, size);

	}

	int
	insert(MemtxTreeData *data, MemtxTreeData *replaced)
	{
		return memtx_tree_insert(&tree, *data, replaced);
	}

	void
	remove(MemtxTreeData *data)
	{
		memtx_tree_delete(&tree, *data);
	}

	void
	set_key_def(struct key_def *key_def)
	{
		tree.arg = key_def;
	}

	struct key_def *
	get_key_def(void)
	{
		return tree.arg;
	}
private:
	memtx_tree tree;
};

#endif /* TARANTOOL_BOX_MEMTX_TREE_PROXY_H_INCLUDED */
