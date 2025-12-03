/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2022 Intel Corporation.
 *   All rights reserved.
 */

#include "spdk/ec.h"
#include "spdk/config.h"
#include "spdk/assert.h"
#include "spdk/util.h"
#include "isa-l/erasure_code.h"

int
spdk_ec_encode(void **dsts, uint32_t m, 
               void **srcs, uint32_t k, 
               uint8_t *g_tbls, uint64_t len)
{
    ec_encode_data(len, k, m, g_tbls, 
                   (unsigned char **)srcs, 
                   (unsigned char **)dsts);

    return 0;
}
