/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2018 Intel Corporation.
 *   All rights reserved.
 */

/** \file
 * Encapsulated DPDK specific dependencies
 */

#include "spdk/stdinc.h"

#ifndef SPDK_ENV_DPDK_H
#define SPDK_ENV_DPDK_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Memory allocation statistics.
 */
struct spdk_env_dpdk_mem_stats {
	/**
	 * Total bytes on heap
	 */
	uint64_t heap_totalsz_bytes;

	/**
	 * Total free bytes on heap
	 */
	uint64_t heap_freesz_bytes;

	/**
	 * Size in bytes of largest free block
	 */
	uint64_t greatest_free_size;

	/**
	 * Total allocated bytes on heap
	 */
	uint64_t heap_allocsz_bytes;

	/**
	 * Number of free elements on heap
	 */
	uint32_t free_count;

	/**
	 * Number of allocated elements on heap
	 */
	uint32_t alloc_count;
};

/**
 * Initialize the environment library after DPDK env is already initialized.
 * If DPDK's rte_eal_init is already called, this function must be called
 * instead of spdk_env_init, prior to using any other functions in SPDK
 * env library.
 *
 * \param legacy_mem Indicates whether DPDK was initialized with --legacy-mem
 *                   eal parameter.
 * \return 0 on success, or negative errno on failure.
 */
int spdk_env_dpdk_post_init(bool legacy_mem);

/**
 * Release any resources of the environment library that were allocated with
 * spdk_env_dpdk_post_init(). After this call, no DPDK function calls may
 * be made. It is expected that common usage of this function is to call it
 * just before terminating the process.
 */
void spdk_env_dpdk_post_fini(void);

/**
 * Check if DPDK was initialized external to the SPDK env_dpdk library.
 *
 * \return true if DPDK was initialized external to the SPDK env_dpdk library.
 * \return false otherwise
 */
bool spdk_env_dpdk_external_init(void);

/**
 * Dump the env allocated memory to the given file.
 *
 * \param file The file object to write to.
 */
void spdk_env_dpdk_dump_mem_stats(FILE *file);

/**
 * Retrieve memory allocation statistics.
 *
 * \param stats Pointer to structure to fill with statistics.
 * \param numa_id NUMA node ID for which statistics are retrieved.
 *
 * \return 0 on success, negative errno on failure.
 */
int spdk_env_dpdk_get_mem_stats(struct spdk_env_dpdk_mem_stats *stats, uint32_t numa_id);

#define SPDK_ENV_DPDK_NAME_MAX 64

/**
 * Usage of one DPDK mempool.
 */
struct spdk_env_dpdk_mempool_info {
	/** Name of the mempool */
	char name[SPDK_ENV_DPDK_NAME_MAX];

	/** NUMA node the mempool was created on, or -1 for any */
	int32_t numa_id;

	/** Number of objects in the mempool */
	uint32_t size;

	/** Size of the per-lcore cache of the mempool */
	uint32_t cache_size;

	/** Size in bytes of an object, including the mempool header and trailer */
	uint32_t object_size;

	/** Number of objects that are free, including the ones in the per-lcore caches */
	uint32_t available;

	/** Number of objects that are in use */
	uint32_t in_use;
};

/**
 * Callback for spdk_env_dpdk_foreach_mempool().
 *
 * \param info Usage of a mempool. It is only valid during the call.
 * \param ctx Context passed to spdk_env_dpdk_foreach_mempool().
 */
typedef void (*spdk_env_dpdk_mempool_fn)(const struct spdk_env_dpdk_mempool_info *info, void *ctx);

/**
 * Call a function for each DPDK mempool, with its usage. The mempool list is locked during the
 * call, so the function must not create or free a mempool.
 *
 * \param fn Function to call.
 * \param ctx Context passed to the function.
 */
void spdk_env_dpdk_foreach_mempool(spdk_env_dpdk_mempool_fn fn, void *ctx);

/**
 * A DPDK memzone.
 */
struct spdk_env_dpdk_memzone_info {
	/** Name of the memzone */
	char name[SPDK_ENV_DPDK_NAME_MAX];

	/** NUMA node of the memzone, or -1 if unknown */
	int32_t numa_id;

	/** Length of the memzone in bytes */
	uint64_t length;
};

/**
 * Callback for spdk_env_dpdk_foreach_memzone().
 *
 * \param info A memzone. It is only valid during the call.
 * \param ctx Context passed to spdk_env_dpdk_foreach_memzone().
 */
typedef void (*spdk_env_dpdk_memzone_fn)(const struct spdk_env_dpdk_memzone_info *info, void *ctx);

/**
 * Call a function for each DPDK memzone. The memzone list is locked during the call, so the
 * function must not reserve or free a memzone.
 *
 * \param fn Function to call.
 * \param ctx Context passed to the function.
 */
void spdk_env_dpdk_foreach_memzone(spdk_env_dpdk_memzone_fn fn, void *ctx);

#ifdef __cplusplus
}
#endif

#endif
