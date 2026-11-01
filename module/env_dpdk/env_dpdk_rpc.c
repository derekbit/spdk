/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2019 Intel Corporation. All rights reserved.
 *   Copyright (c) 2019 Mellanox Technologies LTD. All rights reserved.
 */

#include "spdk/stdinc.h"
#include "spdk/rpc.h"
#include "spdk/env.h"
#include "spdk/env_dpdk.h"
#include "spdk/log.h"

static void
rpc_env_dpdk_get_mem_stats(struct spdk_jsonrpc_request *request,
			   const struct spdk_json_val *params)
{
	FILE *file = NULL;
	struct spdk_json_write_ctx *w;
	char default_filename[] = "/tmp/spdk_mem_dump.txt";

	if (params != NULL) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
						 "env_dpdk_get_mem_stats doesn't accept any parameters.\n");
		return;
	}

	file = fopen(default_filename, "w");
	if (!file) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "Unable to open file for writing.\n");
		return;
	}

	spdk_env_dpdk_dump_mem_stats(file);
	fclose(file);
	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_object_begin(w);
	spdk_json_write_named_string(w, "filename", default_filename);
	spdk_json_write_object_end(w);
	spdk_jsonrpc_end_result(request, w);
}
SPDK_RPC_REGISTER("env_dpdk_get_mem_stats", rpc_env_dpdk_get_mem_stats, SPDK_RPC_RUNTIME)

static void
mem_usage_write_heap(struct spdk_json_write_ctx *w, int32_t numa_id)
{
	struct spdk_env_dpdk_mem_stats stats = {};

	if (spdk_env_dpdk_get_mem_stats(&stats, numa_id) != 0 || stats.heap_totalsz_bytes == 0) {
		return;
	}

	spdk_json_write_object_begin(w);
	spdk_json_write_named_int32(w, "numa_id", numa_id);
	spdk_json_write_named_uint64(w, "total_bytes", stats.heap_totalsz_bytes);
	spdk_json_write_named_uint64(w, "free_bytes", stats.heap_freesz_bytes);
	spdk_json_write_named_uint64(w, "allocated_bytes", stats.heap_allocsz_bytes);
	spdk_json_write_named_uint64(w, "greatest_free_bytes", stats.greatest_free_size);
	spdk_json_write_named_uint32(w, "free_count", stats.free_count);
	spdk_json_write_named_uint32(w, "alloc_count", stats.alloc_count);
	spdk_json_write_object_end(w);
}

static void
mem_usage_write_mempool(const struct spdk_env_dpdk_mempool_info *info, void *ctx)
{
	struct spdk_json_write_ctx *w = ctx;

	spdk_json_write_object_begin(w);
	spdk_json_write_named_string(w, "name", info->name);
	spdk_json_write_named_int32(w, "numa_id", info->numa_id);
	spdk_json_write_named_uint32(w, "size", info->size);
	spdk_json_write_named_uint32(w, "cache_size", info->cache_size);
	spdk_json_write_named_uint32(w, "object_size", info->object_size);
	spdk_json_write_named_uint32(w, "available", info->available);
	spdk_json_write_named_uint32(w, "in_use", info->in_use);
	spdk_json_write_object_end(w);
}

static void
mem_usage_write_memzone(const struct spdk_env_dpdk_memzone_info *info, void *ctx)
{
	struct spdk_json_write_ctx *w = ctx;

	spdk_json_write_object_begin(w);
	spdk_json_write_named_string(w, "name", info->name);
	spdk_json_write_named_int32(w, "numa_id", info->numa_id);
	spdk_json_write_named_uint64(w, "length", info->length);
	spdk_json_write_object_end(w);
}

static void
rpc_env_dpdk_get_mem_usage(struct spdk_jsonrpc_request *request,
			   const struct spdk_json_val *params)
{
	struct spdk_json_write_ctx *w;
	int32_t numa_id;

	if (params != NULL) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
						 "env_dpdk_get_mem_usage doesn't accept any parameters.");
		return;
	}

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_object_begin(w);

	spdk_json_write_named_array_begin(w, "heaps");
	SPDK_ENV_FOREACH_NUMA_ID(numa_id) {
		mem_usage_write_heap(w, numa_id);
	}
	spdk_json_write_array_end(w);

	spdk_json_write_named_array_begin(w, "mempools");
	spdk_env_dpdk_foreach_mempool(mem_usage_write_mempool, w);
	spdk_json_write_array_end(w);

	spdk_json_write_named_array_begin(w, "memzones");
	spdk_env_dpdk_foreach_memzone(mem_usage_write_memzone, w);
	spdk_json_write_array_end(w);

	spdk_json_write_object_end(w);
	spdk_jsonrpc_end_result(request, w);
}
SPDK_RPC_REGISTER("env_dpdk_get_mem_usage", rpc_env_dpdk_get_mem_usage, SPDK_RPC_RUNTIME)
