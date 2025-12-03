/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (C) 2025 Intel Corporation.
 * All rights reserved.
 */

#include "spdk/stdinc.h"

#include "spdk_internal/cunit.h"
#include "common/lib/test_env.c"

/* ISA-L header is required for raw API comparison and table generation */
#include "isa-l/erasure_code.h"

/* Defines for EC geometry (4+2 configuration) */
#define EC_K 4
#define EC_M 2
#define BUF_SIZE 4096
#define ALIGNMENT 64

/* * Prototype for the function under test.
 * In a real build system, this would come from "spdk/util.h".
 */
void spdk_ec_encode(void **dsts, uint32_t m,
                    void **srcs, uint32_t k,
                    uint8_t *g_tbls, uint64_t len);

static void
test_ec_encode(void)
{
    /* Arrays of pointers for Data (Sources) and Parity (Destinations) */
    void *data_bufs[EC_K];
    void *parity_bufs[EC_M];

    /* Reference buffers to verify against raw ISA-L output */
    void *ref_data_bufs[EC_K];
    void *ref_parity_bufs[EC_M];

    /* EC Matrix and Tables */
    uint8_t *encode_matrix = NULL;
    uint8_t *g_tbls = NULL;

    int ret;
    size_t i, j;
    uint8_t *tmp_ptr;

    /* ======================================================
     * 1. Setup: Allocate and Initialize EC Tables
     * ====================================================== */
    encode_matrix = malloc(EC_M * EC_K);
    SPDK_CU_ASSERT_FATAL(encode_matrix != NULL);

    /* 32 bytes * K * M is required for ISA-L tables */
    g_tbls = malloc(32 * EC_K * EC_M);
    SPDK_CU_ASSERT_FATAL(g_tbls != NULL);

    /* Generate Vandermonde matrix and init encoding tables */
    gf_gen_rs_matrix(encode_matrix, EC_M, EC_K);
    ec_init_tables(EC_K, EC_M, encode_matrix, g_tbls);

    /* ======================================================
     * 2. Setup: Allocate and Fill Buffers
     * ====================================================== */
    for (i = 0; i < EC_K; i++) {
        /* Allocate aligned memory for Test Data */
        ret = posix_memalign(&data_bufs[i], ALIGNMENT, BUF_SIZE);
        SPDK_CU_ASSERT_FATAL(ret == 0);

        /* Allocate aligned memory for Reference Data */
        ret = posix_memalign(&ref_data_bufs[i], ALIGNMENT, BUF_SIZE);
        SPDK_CU_ASSERT_FATAL(ret == 0);

        /* Fill data with a predictable pattern */
        tmp_ptr = data_bufs[i];
        for (j = 0; j < BUF_SIZE; j++) {
            tmp_ptr[j] = (uint8_t)((i << 4) + j);
        }

        /* Copy pattern to reference buffer */
        memcpy(ref_data_bufs[i], data_bufs[i], BUF_SIZE);
    }

    for (i = 0; i < EC_M; i++) {
        /* Allocate aligned memory for Test Parity */
        ret = posix_memalign(&parity_bufs[i], ALIGNMENT, BUF_SIZE);
        SPDK_CU_ASSERT_FATAL(ret == 0);
        memset(parity_bufs[i], 0, BUF_SIZE);

        /* Allocate aligned memory for Reference Parity */
        ret = posix_memalign(&ref_parity_bufs[i], ALIGNMENT, BUF_SIZE);
        SPDK_CU_ASSERT_FATAL(ret == 0);
        memset(ref_parity_bufs[i], 0, BUF_SIZE);
    }

    /* ======================================================
     * Test Case 1: Standard Full Stripe Encoding
     * ====================================================== */
    
    /* A. Run Reference calculation (Direct ISA-L call) */
    ec_encode_data(BUF_SIZE, EC_K, EC_M, g_tbls,
                   (unsigned char **)ref_data_bufs,
                   (unsigned char **)ref_parity_bufs);

    /* B. Run Target calculation (SPDK Wrapper) */
    spdk_ec_encode(parity_bufs, EC_M, data_bufs, EC_K, g_tbls, BUF_SIZE);

    /* C. Verify results */
    for (i = 0; i < EC_M; i++) {
        ret = memcmp(parity_bufs[i], ref_parity_bufs[i], BUF_SIZE);
        if (ret != 0) {
            printf("Mismatch detected in Parity Buffer index %zu\n", i);
        }
        CU_ASSERT(ret == 0);
    }

    /* ======================================================
     * Test Case 2: Unaligned Length (BUF_SIZE - 1)
     * ====================================================== */
    
    /* Reset destination buffers with a dirty pattern */
    for (i = 0; i < EC_M; i++) {
        memset(parity_bufs[i], 0xCC, BUF_SIZE);
        memset(ref_parity_bufs[i], 0xCC, BUF_SIZE);
    }

    /* Encode with length = BUF_SIZE - 1 */
    ec_encode_data(BUF_SIZE - 1, EC_K, EC_M, g_tbls,
                   (unsigned char **)ref_data_bufs,
                   (unsigned char **)ref_parity_bufs);

    spdk_ec_encode(parity_bufs, EC_M, data_bufs, EC_K, g_tbls, BUF_SIZE - 1);

    for (i = 0; i < EC_M; i++) {
        /* Compare the encoded part */
        ret = memcmp(parity_bufs[i], ref_parity_bufs[i], BUF_SIZE - 1);
        CU_ASSERT(ret == 0);

        /* Verify the last byte was NOT touched (should remain 0xCC) */
        tmp_ptr = parity_bufs[i];
        CU_ASSERT(tmp_ptr[BUF_SIZE - 1] == 0xCC);
    }

    /* ======================================================
     * Test Case 3: Zero Input (All Zeros)
     * ====================================================== */
    
    /* Set all data sources to zero */
    for (i = 0; i < EC_K; i++) {
        memset(data_bufs[i], 0, BUF_SIZE);
    }

    spdk_ec_encode(parity_bufs, EC_M, data_bufs, EC_K, g_tbls, BUF_SIZE);

    /* * In Galois Field arithmetic, Matrix * 0 vector must equal 0 vector.
     * Check if all parity bytes are zero.
     */
    for (i = 0; i < EC_M; i++) {
        tmp_ptr = parity_bufs[i];
        for (j = 0; j < BUF_SIZE; j++) {
            if (tmp_ptr[j] != 0) {
                CU_FAIL("Parity buffer should be all zeros");
                break;
            }
        }
    }

    /* ======================================================
     * Cleanup
     * ====================================================== */
    for (i = 0; i < EC_K; i++) {
        free(data_bufs[i]);
        free(ref_data_bufs[i]);
    }
    for (i = 0; i < EC_M; i++) {
        free(parity_bufs[i]);
        free(ref_parity_bufs[i]);
    }
    free(encode_matrix);
    free(g_tbls);
}

int
main(int argc, char **argv)
{
    CU_pSuite suite = NULL;
    unsigned int num_failures;

    CU_initialize_registry();

    suite = CU_add_suite("ec_encode", NULL, NULL);

    CU_ADD_TEST(suite, test_ec_encode);

    num_failures = spdk_ut_run_tests(argc, argv, NULL);

    CU_cleanup_registry();

    return num_failures;
}