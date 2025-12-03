/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2022 Intel Corporation.
 *   All rights reserved.
 */

/**
 * \file
 * Erasure coding (EC) utility functions
 */

#ifndef SPDK_EC_H
#define SPDK_Ec_H

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Generate Erasure Coding parity (Encode).
 *
 * \param dsts Array of destination (parity) buffers. (Output)
 * \param m    Number of destination buffers (parity count).
 * \param srcs Array of source (data) buffers. (Input)
 * \param k    Number of source buffers (data count).
 * \param g_tbls Pointer to pre-calculated encoding tables (ISA-L format).
 * \param len  Length of each buffer in bytes.
 * \return 0 on success, negative error code otherwise.
 */
int spdk_ec_encode(void **dsts, uint32_t m, void **srcs, uint32_t k, uint8_t *g_tbls, uint64_t len);

#ifdef __cplusplus
}
#endif

#endif /* SPDK_EC_H */
