/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2018 Intel Corporation.
 *   All rights reserved.
 */

#ifndef SPDK_BDEV_EC_INTERNAL_H
#define SPDK_BDEV_EC_INTERNAL_H

#include "spdk/bdev_module.h"
#include "spdk/uuid.h"

/* TAIL head for ec bdev list */
TAILQ_HEAD(ec_all_tailq, ec_bdev);

extern struct ec_all_tailq		g_ec_bdev_list;

/* Maximum number of base bdevs supported */
#define EC_MAX_BASE_BDEVS 255

/* Size of the name field in the superblock */
#define EC_BDEV_SB_NAME_SIZE	64

/* Forward declaration of the function table */
static const struct spdk_bdev_fn_table g_ec_fn_table;

struct ec_bdev {
	struct spdk_bdev bdev;

	/*
	 * Base device descriptors for underlying bdevs
	 * Used to send I/O to the underlying physical disks (Data + Parity).
	 * Array indices 0 ~ (k-1) are Data Disks.
	 * Array indices k ~ (n-1) are Parity Disks.
	 */
	struct spdk_bdev_desc *descs[EC_MAX_BASE_BDEVS];

	/*
	 *  Geometry Parameters
	 *  These parameters define the structure and layout of the EC bdev.
	 */
	uint32_t k; /* Number of data chunks (Data Chunks) */
	uint32_t m; /* Number of parity chunks (Parity Chunks) */
	uint32_t n; /* Total number of disks (n = k + m) */

	uint32_t strip_size_kb;     /* Original strip size setting (KB) */
	uint32_t strip_size;        /* Converted strip size (Blocks) - e.g., 4KB=1, 64KB=16 (if blocklen=4k) */
	uint32_t stripe_blocks;     /* Total data in one complete stripe (Blocks) = k * strip_size */

	/*
	 * ISA-L Acceleration Tables
	 * To avoid recalculating matrices in the I/O hot path, we precompute and cache them here during initialization.
	 */
	uint8_t *encode_matrix;     /* Original encoding matrix (size: m * k bytes) */
	uint8_t *g_tbls;            /* Expanded tables for AVX instruction set (size: 32 * k * m bytes) */

	/*
	 * Management Link
	 * Used to link this bdev into the global g_ec_bdevs list for easy management and destruction.
	 */
	TAILQ_ENTRY(ec_bdev) link;
};


/*
 * Context structure for a single EC I/O request.
 *
 * instead of mallocing this for every IO, we define it here and tell SPDK
 * to reserve this much space inside every spdk_bdev_io structure via get_ctx_size.
 * This effectively achieves Zero-Malloc for the I/O submission path.
 */
struct ec_bdev_io {
	/* Pointer back to the parent bdev_io */
	struct spdk_bdev_io *bdev_io;

	/* Thread-local IO channel */
	struct ec_io_channel *ch;

	/* EC Logic: Dynamic resources
	 * Note: These still need allocation if k/m are large, or we can use 
	 * a pre-allocated pool/cache for these arrays too.
	 */
	struct iovec *data_iovs;
	struct iovec *parity_iovs;
	void **parity_bufs;

	/* Completion tracking */
	uint32_t outstanding_ios;
	int status;
};

int ec_bdev_create(const char *name, uint32_t strip_size, uint32_t k, uint32_t m,
	const char **base_bdev_names, const struct spdk_uuid *uuid);

#endif /* SPDK_BDEV_EC_INTERNAL_H */
