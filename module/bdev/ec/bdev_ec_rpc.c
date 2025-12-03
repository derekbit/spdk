/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (C) 2025 Intel Corporation.
 * All rights reserved.
 */

#include "bdev_ec.h"

#include "spdk/rpc.h"
#include "spdk/util.h"
#include "spdk/string.h"
#include "spdk/log.h"
#include "spdk/env.h"

#define EC_MAX_BASE_BDEVS 255

/*
 * Structure to hold parameters decoded from the JSON RPC request.
 */
struct rpc_bdev_ec_create {
    char *name;                          /* Name of the EC bdev */
    char *base_bdevs[EC_MAX_BASE_BDEVS]; /* Array of base bdev names */
    size_t num_base_bdevs;               /* Count of base bdevs */
    uint32_t k;                          /* Number of data chunks */
    uint32_t m;                          /* Number of parity chunks */
    uint32_t strip_size_kb;              /* Strip size in KB */
};

/*
 * Helper function to free memory allocated during JSON parsing.
 */
static void
free_rpc_bdev_ec_create(struct rpc_bdev_ec_create *req)
{
    size_t i;

    free(req->name);
    for (i = 0; i < req->num_base_bdevs; i++) {
        free(req->base_bdevs[i]);
    }
}

/*
 * Decoder function for RPC bdev_ec_create to decode the array of base bdev names.
 */
static int
decode_base_bdevs(const struct spdk_json_val *val, void *out)
{
    struct rpc_bdev_ec_create *req = out;
    return spdk_json_decode_array(val, spdk_json_decode_string, req->base_bdevs,
                                  EC_MAX_BASE_BDEVS, &req->num_base_bdevs, sizeof(char *));
}

/*
 * Decoder object for RPC bdev_ec_create
 */
static const struct spdk_json_object_decoder rpc_bdev_ec_create_decoders[] = {
    {"name", offsetof(struct rpc_bdev_ec_create, name), spdk_json_decode_string},
    {"base_bdevs", offsetof(struct rpc_bdev_ec_create, base_bdevs), decode_base_bdevs},
    {"k", offsetof(struct rpc_bdev_ec_create, k), spdk_json_decode_uint32},
    {"m", offsetof(struct rpc_bdev_ec_create, m), spdk_json_decode_uint32},
    {"strip_size_kb", offsetof(struct rpc_bdev_ec_create, strip_size_kb), spdk_json_decode_uint32},
};

/*
 * brief:
 * rpc_bdev_ec_create function is the RPC for creating Erasure Coding (EC) bdev.
 * It takes input as EC bdev name, k, m, strip size in KB and list of base bdev names.
 * params:
 * request - pointer to json rpc request
 * params - pointer to request parameters
 * returns:
 * none
 */
static void
rpc_bdev_ec_create(struct spdk_jsonrpc_request *request, const struct spdk_json_val *params)
{
	struct rpc_bdev_ec_create req = {0};
	struct spdk_json_write_ctx *w;
	int rc;

	if (spdk_json_decode_object(params, rpc_bdev_ec_create_decoders,
				    SPDK_COUNTOF(rpc_bdev_ec_create_decoders),
				    &req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_PARSE_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	if (req.num_base_bdevs != (req.k + req.m)) {
		SPDK_ERRLOG("Base bdevs count (%lu) does not match k+m (%u+%u)\n", 
			    req.num_base_bdevs, req.k, req.m);
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS, 
						 "Mismatch between base_bdevs count and k+m");
		goto cleanup;
	}

	if (req.k == 0 || req.m == 0) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
						 "k and m must be > 0");
		goto cleanup;
	}

	rc = ec_bdev_create(req.name, req.strip_size_kb, req.k, req.m, (const char **)req.base_bdevs, NULL);
	if (rc != 0) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR, spdk_strerror(-rc));
	} else {
		w = spdk_jsonrpc_begin_result(request);
		spdk_json_write_string(w, req.name); /* Return the name of the created bdev */
		spdk_jsonrpc_end_result(request, w);
	}

cleanup:
    free_rpc_bdev_ec_create(&req);
}
SPDK_RPC_REGISTER("bdev_ec_create", rpc_bdev_ec_create, SPDK_RPC_RUNTIME)