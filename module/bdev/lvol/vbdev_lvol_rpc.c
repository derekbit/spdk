/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2017 Intel Corporation.
 *   All rights reserved.
 *   Copyright (c) 2022-2023 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

#include "spdk/rpc.h"
#include "spdk/bdev.h"
#include "spdk/util.h"
#include "vbdev_lvol.h"
#include "spdk/string.h"
#include "spdk/log.h"
#include "spdk/bdev_module.h"
#include "spdk/bit_array.h"
#include "spdk/base64.h"

SPDK_LOG_REGISTER_COMPONENT(lvol_rpc)

struct rpc_bdev_lvol_create_lvstore {
	char *lvs_name;
	char *bdev_name;
	uint32_t cluster_sz;
	char *clear_method;
	uint32_t num_md_pages_per_cluster_ratio;
};

static int
vbdev_get_lvol_store_by_uuid_xor_name(const char *uuid, const char *lvs_name,
				      struct spdk_lvol_store **lvs)
{
	if ((uuid == NULL && lvs_name == NULL)) {
		SPDK_INFOLOG(lvol_rpc, "lvs UUID nor lvs name specified\n");
		return -EINVAL;
	} else if ((uuid && lvs_name)) {
		SPDK_INFOLOG(lvol_rpc, "both lvs UUID '%s' and lvs name '%s' specified\n", uuid,
			     lvs_name);
		return -EINVAL;
	} else if (uuid) {
		*lvs = vbdev_get_lvol_store_by_uuid(uuid);

		if (*lvs == NULL) {
			SPDK_INFOLOG(lvol_rpc, "blobstore with UUID '%s' not found\n", uuid);
			return -ENODEV;
		}
	} else if (lvs_name) {

		*lvs = vbdev_get_lvol_store_by_name(lvs_name);

		if (*lvs == NULL) {
			SPDK_INFOLOG(lvol_rpc, "blobstore with name '%s' not found\n", lvs_name);
			return -ENODEV;
		}
	}
	return 0;
}

static void
free_rpc_bdev_lvol_create_lvstore(struct rpc_bdev_lvol_create_lvstore *req)
{
	free(req->bdev_name);
	free(req->lvs_name);
	free(req->clear_method);
}

static const struct spdk_json_object_decoder rpc_bdev_lvol_create_lvstore_decoders[] = {
	{"bdev_name", offsetof(struct rpc_bdev_lvol_create_lvstore, bdev_name), spdk_json_decode_string},
	{"cluster_sz", offsetof(struct rpc_bdev_lvol_create_lvstore, cluster_sz), spdk_json_decode_uint32, true},
	{"lvs_name", offsetof(struct rpc_bdev_lvol_create_lvstore, lvs_name), spdk_json_decode_string},
	{"clear_method", offsetof(struct rpc_bdev_lvol_create_lvstore, clear_method), spdk_json_decode_string, true},
	{"num_md_pages_per_cluster_ratio", offsetof(struct rpc_bdev_lvol_create_lvstore, num_md_pages_per_cluster_ratio), spdk_json_decode_uint32, true},
};

static void
rpc_lvol_store_construct_cb(void *cb_arg, struct spdk_lvol_store *lvol_store, int lvserrno)
{
	struct spdk_json_write_ctx *w;
	char lvol_store_uuid[SPDK_UUID_STRING_LEN];
	struct spdk_jsonrpc_request *request = cb_arg;

	if (lvserrno != 0) {
		goto invalid;
	}

	spdk_uuid_fmt_lower(lvol_store_uuid, sizeof(lvol_store_uuid), &lvol_store->uuid);

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_string(w, lvol_store_uuid);
	spdk_jsonrpc_end_result(request, w);
	return;

invalid:
	spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
					 spdk_strerror(-lvserrno));
}

static void
rpc_bdev_lvol_create_lvstore(struct spdk_jsonrpc_request *request,
			     const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_create_lvstore req = {};
	int rc = 0;
	enum lvs_clear_method clear_method;

	if (spdk_json_decode_object(params, rpc_bdev_lvol_create_lvstore_decoders,
				    SPDK_COUNTOF(rpc_bdev_lvol_create_lvstore_decoders),
				    &req)) {
		SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	if (req.clear_method != NULL) {
		if (!strcasecmp(req.clear_method, "none")) {
			clear_method = LVS_CLEAR_WITH_NONE;
		} else if (!strcasecmp(req.clear_method, "unmap")) {
			clear_method = LVS_CLEAR_WITH_UNMAP;
		} else if (!strcasecmp(req.clear_method, "write_zeroes")) {
			clear_method = LVS_CLEAR_WITH_WRITE_ZEROES;
		} else {
			spdk_jsonrpc_send_error_response(request, -EINVAL, "Invalid clear_method parameter");
			goto cleanup;
		}
	} else {
		clear_method = LVS_CLEAR_WITH_UNMAP;
	}

	rc = vbdev_lvs_create(req.bdev_name, req.lvs_name, req.cluster_sz, clear_method,
			      req.num_md_pages_per_cluster_ratio, rpc_lvol_store_construct_cb, request);
	if (rc < 0) {
		spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
		goto cleanup;
	}
	free_rpc_bdev_lvol_create_lvstore(&req);

	return;

cleanup:
	free_rpc_bdev_lvol_create_lvstore(&req);
}
SPDK_RPC_REGISTER("bdev_lvol_create_lvstore", rpc_bdev_lvol_create_lvstore, SPDK_RPC_RUNTIME)

struct rpc_bdev_lvol_rename_lvstore {
	char *old_name;
	char *new_name;
};

static void
free_rpc_bdev_lvol_rename_lvstore(struct rpc_bdev_lvol_rename_lvstore *req)
{
	free(req->old_name);
	free(req->new_name);
}

static const struct spdk_json_object_decoder rpc_bdev_lvol_rename_lvstore_decoders[] = {
	{"old_name", offsetof(struct rpc_bdev_lvol_rename_lvstore, old_name), spdk_json_decode_string},
	{"new_name", offsetof(struct rpc_bdev_lvol_rename_lvstore, new_name), spdk_json_decode_string},
};

static void
rpc_bdev_lvol_rename_lvstore_cb(void *cb_arg, int lvserrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	if (lvserrno != 0) {
		goto invalid;
	}

	spdk_jsonrpc_send_bool_response(request, true);
	return;

invalid:
	spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
					 spdk_strerror(-lvserrno));
}

static void
rpc_bdev_lvol_rename_lvstore(struct spdk_jsonrpc_request *request,
			     const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_rename_lvstore req = {};
	struct spdk_lvol_store *lvs;

	if (spdk_json_decode_object(params, rpc_bdev_lvol_rename_lvstore_decoders,
				    SPDK_COUNTOF(rpc_bdev_lvol_rename_lvstore_decoders),
				    &req)) {
		SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	lvs = vbdev_get_lvol_store_by_name(req.old_name);
	if (lvs == NULL) {
		SPDK_INFOLOG(lvol_rpc, "no lvs existing for given name\n");
		spdk_jsonrpc_send_error_response_fmt(request, -ENOENT, "Lvol store %s not found", req.old_name);
		goto cleanup;
	}

	vbdev_lvs_rename(lvs, req.new_name, rpc_bdev_lvol_rename_lvstore_cb, request);

cleanup:
	free_rpc_bdev_lvol_rename_lvstore(&req);
}
SPDK_RPC_REGISTER("bdev_lvol_rename_lvstore", rpc_bdev_lvol_rename_lvstore, SPDK_RPC_RUNTIME)

struct rpc_bdev_lvol_delete_lvstore {
	char *uuid;
	char *lvs_name;
};

static void
free_rpc_bdev_lvol_delete_lvstore(struct rpc_bdev_lvol_delete_lvstore *req)
{
	free(req->uuid);
	free(req->lvs_name);
}

static const struct spdk_json_object_decoder rpc_bdev_lvol_delete_lvstore_decoders[] = {
	{"uuid", offsetof(struct rpc_bdev_lvol_delete_lvstore, uuid), spdk_json_decode_string, true},
	{"lvs_name", offsetof(struct rpc_bdev_lvol_delete_lvstore, lvs_name), spdk_json_decode_string, true},
};

static void
rpc_lvol_store_destroy_cb(void *cb_arg, int lvserrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	if (lvserrno != 0) {
		goto invalid;
	}

	spdk_jsonrpc_send_bool_response(request, true);
	return;

invalid:
	spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
					 spdk_strerror(-lvserrno));
}

static void
rpc_bdev_lvol_delete_lvstore(struct spdk_jsonrpc_request *request,
			     const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_delete_lvstore req = {};
	struct spdk_lvol_store *lvs = NULL;
	int rc;

	if (spdk_json_decode_object(params, rpc_bdev_lvol_delete_lvstore_decoders,
				    SPDK_COUNTOF(rpc_bdev_lvol_delete_lvstore_decoders),
				    &req)) {
		SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	rc = vbdev_get_lvol_store_by_uuid_xor_name(req.uuid, req.lvs_name, &lvs);
	if (rc != 0) {
		spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
		goto cleanup;
	}

	vbdev_lvs_destruct(lvs, rpc_lvol_store_destroy_cb, request);

cleanup:
	free_rpc_bdev_lvol_delete_lvstore(&req);
}
SPDK_RPC_REGISTER("bdev_lvol_delete_lvstore", rpc_bdev_lvol_delete_lvstore, SPDK_RPC_RUNTIME)

struct rpc_bdev_lvol_create {
	char *uuid;
	char *lvs_name;
	char *lvol_name;
	uint64_t size;
	uint64_t size_in_mib;
	bool thin_provision;
	char *clear_method;
};

static void
free_rpc_bdev_lvol_create(struct rpc_bdev_lvol_create *req)
{
	free(req->uuid);
	free(req->lvs_name);
	free(req->lvol_name);
	free(req->clear_method);
}

static const struct spdk_json_object_decoder rpc_bdev_lvol_create_decoders[] = {
	{"uuid", offsetof(struct rpc_bdev_lvol_create, uuid), spdk_json_decode_string, true},
	{"lvs_name", offsetof(struct rpc_bdev_lvol_create, lvs_name), spdk_json_decode_string, true},
	{"lvol_name", offsetof(struct rpc_bdev_lvol_create, lvol_name), spdk_json_decode_string},
	{"size", offsetof(struct rpc_bdev_lvol_create, size), spdk_json_decode_uint64, true},
	{"size_in_mib", offsetof(struct rpc_bdev_lvol_create, size_in_mib), spdk_json_decode_uint64, true},
	{"thin_provision", offsetof(struct rpc_bdev_lvol_create, thin_provision), spdk_json_decode_bool, true},
	{"clear_method", offsetof(struct rpc_bdev_lvol_create, clear_method), spdk_json_decode_string, true},
};

static void
rpc_bdev_lvol_create_cb(void *cb_arg, struct spdk_lvol *lvol, int lvolerrno)
{
	struct spdk_json_write_ctx *w;
	struct spdk_jsonrpc_request *request = cb_arg;

	if (lvolerrno != 0) {
		goto invalid;
	}

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_string(w, lvol->unique_id);
	spdk_jsonrpc_end_result(request, w);
	return;

invalid:
	spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
					 spdk_strerror(-lvolerrno));
}

SPDK_LOG_DEPRECATION_REGISTER(vbdev_lvol_rpc_req_size,
			      "rpc_bdev_lvol_create/resize req.size",
			      "v23.09", 0);

static void
rpc_bdev_lvol_create(struct spdk_jsonrpc_request *request,
		     const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_create req = {};
	enum lvol_clear_method clear_method;
	int rc = 0;
	struct spdk_lvol_store *lvs = NULL;
	uint64_t size = 0;

	SPDK_INFOLOG(lvol_rpc, "Creating blob\n");

	if (spdk_json_decode_object(params, rpc_bdev_lvol_create_decoders,
				    SPDK_COUNTOF(rpc_bdev_lvol_create_decoders),
				    &req)) {
		SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	if (req.size > 0 && req.size_in_mib > 0) {
		SPDK_LOG_DEPRECATED(vbdev_lvol_rpc_req_size);
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "size is deprecated. Specify only size_in_mib instead.");
		goto cleanup;
	} else if (req.size_in_mib > 0) {
		size = req.size_in_mib * 1024 * 1024;
	} else {
		SPDK_LOG_DEPRECATED(vbdev_lvol_rpc_req_size);
		size = req.size;
	}

	rc = vbdev_get_lvol_store_by_uuid_xor_name(req.uuid, req.lvs_name, &lvs);
	if (rc != 0) {
		spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
		goto cleanup;
	}

	if (req.clear_method != NULL) {
		if (!strcasecmp(req.clear_method, "none")) {
			clear_method = LVOL_CLEAR_WITH_NONE;
		} else if (!strcasecmp(req.clear_method, "unmap")) {
			clear_method = LVOL_CLEAR_WITH_UNMAP;
		} else if (!strcasecmp(req.clear_method, "write_zeroes")) {
			clear_method = LVOL_CLEAR_WITH_WRITE_ZEROES;
		} else {
			spdk_jsonrpc_send_error_response(request, -EINVAL, "Invalid clean_method option");
			goto cleanup;
		}
	} else {
		clear_method = LVOL_CLEAR_WITH_DEFAULT;
	}

	rc = vbdev_lvol_create(lvs, req.lvol_name, size, req.thin_provision,
			       clear_method, rpc_bdev_lvol_create_cb, request);
	if (rc < 0) {
		spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
		goto cleanup;
	}

cleanup:
	free_rpc_bdev_lvol_create(&req);
}

SPDK_RPC_REGISTER("bdev_lvol_create", rpc_bdev_lvol_create, SPDK_RPC_RUNTIME)

struct rpc_bdev_lvol_snapshot {
	char *lvol_name;
	char *snapshot_name;
};

static void
free_rpc_bdev_lvol_snapshot(struct rpc_bdev_lvol_snapshot *req)
{
	free(req->lvol_name);
	free(req->snapshot_name);
}

static const struct spdk_json_object_decoder rpc_bdev_lvol_snapshot_decoders[] = {
	{"lvol_name", offsetof(struct rpc_bdev_lvol_snapshot, lvol_name), spdk_json_decode_string},
	{"snapshot_name", offsetof(struct rpc_bdev_lvol_snapshot, snapshot_name), spdk_json_decode_string},
};

static void
rpc_bdev_lvol_snapshot_cb(void *cb_arg, struct spdk_lvol *lvol, int lvolerrno)
{
	struct spdk_json_write_ctx *w;
	struct spdk_jsonrpc_request *request = cb_arg;

	if (lvolerrno != 0) {
		goto invalid;
	}

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_string(w, lvol->unique_id);
	spdk_jsonrpc_end_result(request, w);
	return;

invalid:
	spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
					 spdk_strerror(-lvolerrno));
}

static void
rpc_bdev_lvol_snapshot(struct spdk_jsonrpc_request *request,
		       const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_snapshot req = {};
	struct spdk_bdev *bdev;
	struct spdk_lvol *lvol;

	SPDK_INFOLOG(lvol_rpc, "Snapshotting blob\n");

	if (spdk_json_decode_object(params, rpc_bdev_lvol_snapshot_decoders,
				    SPDK_COUNTOF(rpc_bdev_lvol_snapshot_decoders),
				    &req)) {
		SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	bdev = spdk_bdev_get_by_name(req.lvol_name);
	if (bdev == NULL) {
		SPDK_ERRLOG("bdev '%s' does not exist\n", req.lvol_name);
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	lvol = vbdev_lvol_get_from_bdev(bdev);
	if (lvol == NULL) {
		SPDK_ERRLOG("lvol does not exist\n");
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	vbdev_lvol_create_snapshot(lvol, req.snapshot_name, rpc_bdev_lvol_snapshot_cb, request);

cleanup:
	free_rpc_bdev_lvol_snapshot(&req);
}

SPDK_RPC_REGISTER("bdev_lvol_snapshot", rpc_bdev_lvol_snapshot, SPDK_RPC_RUNTIME)

struct rpc_bdev_lvol_clone {
	char *snapshot_name;
	char *clone_name;
};

static void
free_rpc_bdev_lvol_clone(struct rpc_bdev_lvol_clone *req)
{
	free(req->snapshot_name);
	free(req->clone_name);
}

static const struct spdk_json_object_decoder rpc_bdev_lvol_clone_decoders[] = {
	{"snapshot_name", offsetof(struct rpc_bdev_lvol_clone, snapshot_name), spdk_json_decode_string},
	{"clone_name", offsetof(struct rpc_bdev_lvol_clone, clone_name), spdk_json_decode_string, true},
};

static void
rpc_bdev_lvol_clone_cb(void *cb_arg, struct spdk_lvol *lvol, int lvolerrno)
{
	struct spdk_json_write_ctx *w;
	struct spdk_jsonrpc_request *request = cb_arg;

	if (lvolerrno != 0) {
		goto invalid;
	}

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_string(w, lvol->unique_id);
	spdk_jsonrpc_end_result(request, w);
	return;

invalid:
	spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
					 spdk_strerror(-lvolerrno));
}

static void
rpc_bdev_lvol_clone(struct spdk_jsonrpc_request *request,
		    const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_clone req = {};
	struct spdk_bdev *bdev;
	struct spdk_lvol *lvol;

	SPDK_INFOLOG(lvol_rpc, "Cloning blob\n");

	if (spdk_json_decode_object(params, rpc_bdev_lvol_clone_decoders,
				    SPDK_COUNTOF(rpc_bdev_lvol_clone_decoders),
				    &req)) {
		SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	bdev = spdk_bdev_get_by_name(req.snapshot_name);
	if (bdev == NULL) {
		SPDK_ERRLOG("bdev '%s' does not exist\n", req.snapshot_name);
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	lvol = vbdev_lvol_get_from_bdev(bdev);
	if (lvol == NULL) {
		SPDK_ERRLOG("lvol does not exist\n");
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	vbdev_lvol_create_clone(lvol, req.clone_name, rpc_bdev_lvol_clone_cb, request);

cleanup:
	free_rpc_bdev_lvol_clone(&req);
}

SPDK_RPC_REGISTER("bdev_lvol_clone", rpc_bdev_lvol_clone, SPDK_RPC_RUNTIME)

struct rpc_bdev_lvol_clone_bdev {
	/* name or UUID. Whichever is used, the UUID will be stored in the lvol's metadata. */
	char *bdev_name;
	char *lvs_name;
	char *clone_name;
};

static void
free_rpc_bdev_lvol_clone_bdev(struct rpc_bdev_lvol_clone_bdev *req)
{
	free(req->bdev_name);
	free(req->lvs_name);
	free(req->clone_name);
}

static const struct spdk_json_object_decoder rpc_bdev_lvol_clone_bdev_decoders[] = {
	{
		"bdev", offsetof(struct rpc_bdev_lvol_clone_bdev, bdev_name),
		spdk_json_decode_string, false
	},
	{
		"lvs_name", offsetof(struct rpc_bdev_lvol_clone_bdev, lvs_name),
		spdk_json_decode_string, false
	},
	{
		"clone_name", offsetof(struct rpc_bdev_lvol_clone_bdev, clone_name),
		spdk_json_decode_string, false
	},
};

static void
rpc_bdev_lvol_clone_bdev(struct spdk_jsonrpc_request *request, const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_clone_bdev req = {};
	struct spdk_bdev *bdev;
	struct spdk_lvol_store *lvs = NULL;
	struct spdk_lvol *lvol;
	int rc;

	SPDK_INFOLOG(lvol_rpc, "Cloning bdev\n");

	if (spdk_json_decode_object(params, rpc_bdev_lvol_clone_bdev_decoders,
				    SPDK_COUNTOF(rpc_bdev_lvol_clone_bdev_decoders), &req)) {
		SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	rc = vbdev_get_lvol_store_by_uuid_xor_name(NULL, req.lvs_name, &lvs);
	if (rc != 0) {
		SPDK_INFOLOG(lvol_rpc, "lvs_name '%s' not found\n", req.lvs_name);
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
						 "lvs does not exist");
		goto cleanup;
	}

	bdev = spdk_bdev_get_by_name(req.bdev_name);
	if (bdev == NULL) {
		SPDK_ERRLOG("bdev '%s' does not exist\n", req.bdev_name);
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
						 "bdev does not exist");
		goto cleanup;
	}

	lvol = vbdev_lvol_get_from_bdev(bdev);
	if (lvol != NULL && lvol->lvol_store == lvs) {
		SPDK_INFOLOG(lvol_rpc, "bdev '%s' is an lvol in lvstore '%s\n", req.bdev_name,
			     req.lvs_name);
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
						 "bdev is an lvol in same lvs as clone; "
						 "use bdev_lvol_clone instead");
		goto cleanup;
	}

	vbdev_lvol_create_bdev_clone(req.bdev_name, lvs, req.clone_name,
				     rpc_bdev_lvol_clone_cb, request);
cleanup:
	free_rpc_bdev_lvol_clone_bdev(&req);
}

SPDK_RPC_REGISTER("bdev_lvol_clone_bdev", rpc_bdev_lvol_clone_bdev, SPDK_RPC_RUNTIME)

struct rpc_bdev_lvol_rename {
	char *old_name;
	char *new_name;
};

static void
free_rpc_bdev_lvol_rename(struct rpc_bdev_lvol_rename *req)
{
	free(req->old_name);
	free(req->new_name);
}

static const struct spdk_json_object_decoder rpc_bdev_lvol_rename_decoders[] = {
	{"old_name", offsetof(struct rpc_bdev_lvol_rename, old_name), spdk_json_decode_string},
	{"new_name", offsetof(struct rpc_bdev_lvol_rename, new_name), spdk_json_decode_string},
};

static void
rpc_bdev_lvol_rename_cb(void *cb_arg, int lvolerrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	if (lvolerrno != 0) {
		goto invalid;
	}

	spdk_jsonrpc_send_bool_response(request, true);
	return;

invalid:
	spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
					 spdk_strerror(-lvolerrno));
}

static void
rpc_bdev_lvol_rename(struct spdk_jsonrpc_request *request,
		     const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_rename req = {};
	struct spdk_bdev *bdev;
	struct spdk_lvol *lvol;

	SPDK_INFOLOG(lvol_rpc, "Renaming lvol\n");

	if (spdk_json_decode_object(params, rpc_bdev_lvol_rename_decoders,
				    SPDK_COUNTOF(rpc_bdev_lvol_rename_decoders),
				    &req)) {
		SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	bdev = spdk_bdev_get_by_name(req.old_name);
	if (bdev == NULL) {
		SPDK_ERRLOG("bdev '%s' does not exist\n", req.old_name);
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	lvol = vbdev_lvol_get_from_bdev(bdev);
	if (lvol == NULL) {
		SPDK_ERRLOG("lvol does not exist\n");
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	vbdev_lvol_rename(lvol, req.new_name, rpc_bdev_lvol_rename_cb, request);

cleanup:
	free_rpc_bdev_lvol_rename(&req);
}

SPDK_RPC_REGISTER("bdev_lvol_rename", rpc_bdev_lvol_rename, SPDK_RPC_RUNTIME)

struct rpc_bdev_lvol_inflate {
	char *name;
};

static void
free_rpc_bdev_lvol_inflate(struct rpc_bdev_lvol_inflate *req)
{
	free(req->name);
}

static const struct spdk_json_object_decoder rpc_bdev_lvol_inflate_decoders[] = {
	{"name", offsetof(struct rpc_bdev_lvol_inflate, name), spdk_json_decode_string},
};

static void
rpc_bdev_lvol_inflate_cb(void *cb_arg, int lvolerrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	if (lvolerrno != 0) {
		goto invalid;
	}

	spdk_jsonrpc_send_bool_response(request, true);
	return;

invalid:
	spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
					 spdk_strerror(-lvolerrno));
}

static void
rpc_bdev_lvol_inflate(struct spdk_jsonrpc_request *request,
		      const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_inflate req = {};
	struct spdk_bdev *bdev;
	struct spdk_lvol *lvol;

	SPDK_INFOLOG(lvol_rpc, "Inflating lvol\n");

	if (spdk_json_decode_object(params, rpc_bdev_lvol_inflate_decoders,
				    SPDK_COUNTOF(rpc_bdev_lvol_inflate_decoders),
				    &req)) {
		SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	bdev = spdk_bdev_get_by_name(req.name);
	if (bdev == NULL) {
		SPDK_ERRLOG("bdev '%s' does not exist\n", req.name);
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	lvol = vbdev_lvol_get_from_bdev(bdev);
	if (lvol == NULL) {
		SPDK_ERRLOG("lvol does not exist\n");
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	spdk_lvol_inflate(lvol, rpc_bdev_lvol_inflate_cb, request);

cleanup:
	free_rpc_bdev_lvol_inflate(&req);
}

SPDK_RPC_REGISTER("bdev_lvol_inflate", rpc_bdev_lvol_inflate, SPDK_RPC_RUNTIME)

static void
rpc_bdev_lvol_decouple_parent(struct spdk_jsonrpc_request *request,
			      const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_inflate req = {};
	struct spdk_bdev *bdev;
	struct spdk_lvol *lvol;

	SPDK_INFOLOG(lvol_rpc, "Decoupling parent of lvol\n");

	if (spdk_json_decode_object(params, rpc_bdev_lvol_inflate_decoders,
				    SPDK_COUNTOF(rpc_bdev_lvol_inflate_decoders),
				    &req)) {
		SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	bdev = spdk_bdev_get_by_name(req.name);
	if (bdev == NULL) {
		SPDK_ERRLOG("bdev '%s' does not exist\n", req.name);
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	lvol = vbdev_lvol_get_from_bdev(bdev);
	if (lvol == NULL) {
		SPDK_ERRLOG("lvol does not exist\n");
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	spdk_lvol_decouple_parent(lvol, rpc_bdev_lvol_inflate_cb, request);

cleanup:
	free_rpc_bdev_lvol_inflate(&req);
}

SPDK_RPC_REGISTER("bdev_lvol_decouple_parent", rpc_bdev_lvol_decouple_parent, SPDK_RPC_RUNTIME)

struct rpc_bdev_lvol_resize {
	char *name;
	uint64_t size;
	uint64_t size_in_mib;
};

static void
free_rpc_bdev_lvol_resize(struct rpc_bdev_lvol_resize *req)
{
	free(req->name);
}

static const struct spdk_json_object_decoder rpc_bdev_lvol_resize_decoders[] = {
	{"name", offsetof(struct rpc_bdev_lvol_resize, name), spdk_json_decode_string},
	{"size", offsetof(struct rpc_bdev_lvol_resize, size), spdk_json_decode_uint64, true},
	{"size_in_mib", offsetof(struct rpc_bdev_lvol_resize, size_in_mib), spdk_json_decode_uint64, true},
};

static void
rpc_bdev_lvol_resize_cb(void *cb_arg, int lvolerrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	if (lvolerrno != 0) {
		goto invalid;
	}

	spdk_jsonrpc_send_bool_response(request, true);
	return;

invalid:
	spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
					 spdk_strerror(-lvolerrno));
}

static void
rpc_bdev_lvol_resize(struct spdk_jsonrpc_request *request,
		     const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_resize req = {};
	struct spdk_bdev *bdev;
	struct spdk_lvol *lvol;
	uint64_t size = 0;

	SPDK_INFOLOG(lvol_rpc, "Resizing lvol\n");

	if (spdk_json_decode_object(params, rpc_bdev_lvol_resize_decoders,
				    SPDK_COUNTOF(rpc_bdev_lvol_resize_decoders),
				    &req)) {
		SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	if (req.size > 0 && req.size_in_mib > 0) {
		SPDK_LOG_DEPRECATED(vbdev_lvol_rpc_req_size);
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "size is deprecated. Specify only size_in_mib instead.");
		goto cleanup;
	} else if (req.size_in_mib > 0) {
		size = req.size_in_mib * 1024 * 1024;
	} else {
		SPDK_LOG_DEPRECATED(vbdev_lvol_rpc_req_size);
		size = req.size;
	}

	bdev = spdk_bdev_get_by_name(req.name);
	if (bdev == NULL) {
		SPDK_ERRLOG("no bdev for provided name %s\n", req.name);
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	lvol = vbdev_lvol_get_from_bdev(bdev);
	if (lvol == NULL) {
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}


	vbdev_lvol_resize(lvol, size, rpc_bdev_lvol_resize_cb, request);

cleanup:
	free_rpc_bdev_lvol_resize(&req);
}

SPDK_RPC_REGISTER("bdev_lvol_resize", rpc_bdev_lvol_resize, SPDK_RPC_RUNTIME)

struct rpc_set_ro_lvol_bdev {
	char *name;
};

static void
free_rpc_set_ro_lvol_bdev(struct rpc_set_ro_lvol_bdev *req)
{
	free(req->name);
}

static const struct spdk_json_object_decoder rpc_set_ro_lvol_bdev_decoders[] = {
	{"name", offsetof(struct rpc_set_ro_lvol_bdev, name), spdk_json_decode_string},
};

static void
rpc_set_ro_lvol_bdev_cb(void *cb_arg, int lvolerrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	if (lvolerrno != 0) {
		goto invalid;
	}

	spdk_jsonrpc_send_bool_response(request, true);
	return;

invalid:
	spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
					 spdk_strerror(-lvolerrno));
}

static void
rpc_bdev_lvol_set_read_only(struct spdk_jsonrpc_request *request,
			    const struct spdk_json_val *params)
{
	struct rpc_set_ro_lvol_bdev req = {};
	struct spdk_bdev *bdev;
	struct spdk_lvol *lvol;

	SPDK_INFOLOG(lvol_rpc, "Setting lvol as read only\n");

	if (spdk_json_decode_object(params, rpc_set_ro_lvol_bdev_decoders,
				    SPDK_COUNTOF(rpc_set_ro_lvol_bdev_decoders),
				    &req)) {
		SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	if (req.name == NULL) {
		SPDK_ERRLOG("missing name param\n");
		spdk_jsonrpc_send_error_response(request, -EINVAL, "Missing name parameter");
		goto cleanup;
	}

	bdev = spdk_bdev_get_by_name(req.name);
	if (bdev == NULL) {
		SPDK_ERRLOG("no bdev for provided name %s\n", req.name);
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	lvol = vbdev_lvol_get_from_bdev(bdev);
	if (lvol == NULL) {
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	vbdev_lvol_set_read_only(lvol, rpc_set_ro_lvol_bdev_cb, request);

cleanup:
	free_rpc_set_ro_lvol_bdev(&req);
}

SPDK_RPC_REGISTER("bdev_lvol_set_read_only", rpc_bdev_lvol_set_read_only, SPDK_RPC_RUNTIME)

struct rpc_bdev_lvol_delete {
	char *name;
};

static void
free_rpc_bdev_lvol_delete(struct rpc_bdev_lvol_delete *req)
{
	free(req->name);
}

static const struct spdk_json_object_decoder rpc_bdev_lvol_delete_decoders[] = {
	{"name", offsetof(struct rpc_bdev_lvol_delete, name), spdk_json_decode_string},
};

static void
rpc_bdev_lvol_delete_cb(void *cb_arg, int lvolerrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	if (lvolerrno != 0) {
		goto invalid;
	}

	spdk_jsonrpc_send_bool_response(request, true);
	return;

invalid:
	spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
					 spdk_strerror(-lvolerrno));
}

static void
rpc_bdev_lvol_delete(struct spdk_jsonrpc_request *request,
		     const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_delete req = {};
	struct spdk_bdev *bdev;
	struct spdk_lvol *lvol;
	struct spdk_uuid uuid;
	char *lvs_name, *lvol_name;

	if (spdk_json_decode_object(params, rpc_bdev_lvol_delete_decoders,
				    SPDK_COUNTOF(rpc_bdev_lvol_delete_decoders),
				    &req)) {
		SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	/* lvol is not degraded, get lvol via bdev name or alias */
	bdev = spdk_bdev_get_by_name(req.name);
	if (bdev != NULL) {
		lvol = vbdev_lvol_get_from_bdev(bdev);
		if (lvol != NULL) {
			goto done;
		}
	}

	/* lvol is degraded, get lvol via UUID */
	if (spdk_uuid_parse(&uuid, req.name) == 0) {
		lvol = spdk_lvol_get_by_uuid(&uuid);
		if (lvol != NULL) {
			goto done;
		}
	}

	/* lvol is degraded, get lvol via lvs_name/lvol_name */
	lvol_name = strchr(req.name, '/');
	if (lvol_name != NULL) {
		*lvol_name = '\0';
		lvol_name++;
		lvs_name = req.name;
		lvol = spdk_lvol_get_by_names(lvs_name, lvol_name);
		if (lvol != NULL) {
			goto done;
		}
	}

	/* Could not find lvol, degraded or not. */
	spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
	goto cleanup;

done:
	vbdev_lvol_destroy(lvol, rpc_bdev_lvol_delete_cb, request);

cleanup:
	free_rpc_bdev_lvol_delete(&req);
}

SPDK_RPC_REGISTER("bdev_lvol_delete", rpc_bdev_lvol_delete, SPDK_RPC_RUNTIME)

struct rpc_bdev_lvol_get_lvstores {
	char *uuid;
	char *lvs_name;
};

static void
free_rpc_bdev_lvol_get_lvstores(struct rpc_bdev_lvol_get_lvstores *req)
{
	free(req->uuid);
	free(req->lvs_name);
}

static const struct spdk_json_object_decoder rpc_bdev_lvol_get_lvstores_decoders[] = {
	{"uuid", offsetof(struct rpc_bdev_lvol_get_lvstores, uuid), spdk_json_decode_string, true},
	{"lvs_name", offsetof(struct rpc_bdev_lvol_get_lvstores, lvs_name), spdk_json_decode_string, true},
};

static void
rpc_dump_lvol_store_info(struct spdk_json_write_ctx *w, struct lvol_store_bdev *lvs_bdev)
{
	struct spdk_blob_store *bs;
	uint64_t cluster_size;
	char uuid[SPDK_UUID_STRING_LEN];

	bs = lvs_bdev->lvs->blobstore;
	cluster_size = spdk_bs_get_cluster_size(bs);

	spdk_json_write_object_begin(w);

	spdk_uuid_fmt_lower(uuid, sizeof(uuid), &lvs_bdev->lvs->uuid);
	spdk_json_write_named_string(w, "uuid", uuid);

	spdk_json_write_named_string(w, "name", lvs_bdev->lvs->name);

	spdk_json_write_named_string(w, "base_bdev", spdk_bdev_get_name(lvs_bdev->bdev));

	spdk_json_write_named_uint64(w, "total_data_clusters", spdk_bs_total_data_cluster_count(bs));

	spdk_json_write_named_uint64(w, "free_clusters", spdk_bs_free_cluster_count(bs));

	spdk_json_write_named_uint64(w, "block_size", spdk_bs_get_io_unit_size(bs));

	spdk_json_write_named_uint64(w, "cluster_size", cluster_size);

	spdk_json_write_object_end(w);
}

static void
rpc_bdev_lvol_get_lvstores(struct spdk_jsonrpc_request *request,
			   const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_get_lvstores req = {};
	struct spdk_json_write_ctx *w;
	struct lvol_store_bdev *lvs_bdev = NULL;
	struct spdk_lvol_store *lvs = NULL;
	int rc;

	if (params != NULL) {
		if (spdk_json_decode_object(params, rpc_bdev_lvol_get_lvstores_decoders,
					    SPDK_COUNTOF(rpc_bdev_lvol_get_lvstores_decoders),
					    &req)) {
			SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
			spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
							 "spdk_json_decode_object failed");
			goto cleanup;
		}

		rc = vbdev_get_lvol_store_by_uuid_xor_name(req.uuid, req.lvs_name, &lvs);
		if (rc != 0) {
			spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
			goto cleanup;
		}

		lvs_bdev = vbdev_get_lvs_bdev_by_lvs(lvs);
		if (lvs_bdev == NULL) {
			spdk_jsonrpc_send_error_response(request, ENODEV, spdk_strerror(-ENODEV));
			goto cleanup;
		}
	}

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_array_begin(w);

	if (lvs_bdev != NULL) {
		rpc_dump_lvol_store_info(w, lvs_bdev);
	} else {
		for (lvs_bdev = vbdev_lvol_store_first(); lvs_bdev != NULL;
		     lvs_bdev = vbdev_lvol_store_next(lvs_bdev)) {
			rpc_dump_lvol_store_info(w, lvs_bdev);
		}
	}
	spdk_json_write_array_end(w);

	spdk_jsonrpc_end_result(request, w);

cleanup:
	free_rpc_bdev_lvol_get_lvstores(&req);
}

SPDK_RPC_REGISTER("bdev_lvol_get_lvstores", rpc_bdev_lvol_get_lvstores, SPDK_RPC_RUNTIME)
SPDK_RPC_REGISTER_ALIAS_DEPRECATED(bdev_lvol_get_lvstores, get_lvol_stores)

struct rpc_bdev_lvol_get_lvols {
	char *lvs_uuid;
	char *lvs_name;
};

static void
free_rpc_bdev_lvol_get_lvols(struct rpc_bdev_lvol_get_lvols *req)
{
	free(req->lvs_uuid);
	free(req->lvs_name);
}

static const struct spdk_json_object_decoder rpc_bdev_lvol_get_lvols_decoders[] = {
	{"lvs_uuid", offsetof(struct rpc_bdev_lvol_get_lvols, lvs_uuid), spdk_json_decode_string, true},
	{"lvs_name", offsetof(struct rpc_bdev_lvol_get_lvols, lvs_name), spdk_json_decode_string, true},
};

static void
rpc_dump_lvol(struct spdk_json_write_ctx *w, struct spdk_lvol *lvol)
{
	struct spdk_lvol_store *lvs = lvol->lvol_store;
	char uuid[SPDK_UUID_STRING_LEN];

	spdk_json_write_object_begin(w);

	spdk_json_write_named_string_fmt(w, "alias", "%s/%s", lvs->name, lvol->name);
	spdk_json_write_named_string(w, "uuid", lvol->uuid_str);
	spdk_json_write_named_string(w, "name", lvol->name);
	spdk_json_write_named_bool(w, "is_thin_provisioned", spdk_blob_is_thin_provisioned(lvol->blob));
	spdk_json_write_named_bool(w, "is_snapshot", spdk_blob_is_snapshot(lvol->blob));
	spdk_json_write_named_bool(w, "is_clone", spdk_blob_is_clone(lvol->blob));
	spdk_json_write_named_bool(w, "is_esnap_clone", spdk_blob_is_esnap_clone(lvol->blob));
	spdk_json_write_named_bool(w, "is_degraded", spdk_blob_is_degraded(lvol->blob));

	spdk_json_write_named_object_begin(w, "lvs");
	spdk_json_write_named_string(w, "name", lvs->name);
	spdk_uuid_fmt_lower(uuid, sizeof(uuid), &lvs->uuid);
	spdk_json_write_named_string(w, "uuid", uuid);
	spdk_json_write_object_end(w);

	spdk_json_write_object_end(w);
}

static void
rpc_dump_lvols(struct spdk_json_write_ctx *w, struct lvol_store_bdev *lvs_bdev)
{
	struct spdk_lvol_store *lvs = lvs_bdev->lvs;
	struct spdk_lvol *lvol;

	TAILQ_FOREACH(lvol, &lvs->lvols, link) {
		rpc_dump_lvol(w, lvol);
	}
}

static void
rpc_bdev_lvol_get_lvols(struct spdk_jsonrpc_request *request, const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_get_lvols req = {};
	struct spdk_json_write_ctx *w;
	struct lvol_store_bdev *lvs_bdev = NULL;
	struct spdk_lvol_store *lvs = NULL;
	int rc;

	if (params != NULL) {
		if (spdk_json_decode_object(params, rpc_bdev_lvol_get_lvols_decoders,
					    SPDK_COUNTOF(rpc_bdev_lvol_get_lvols_decoders),
					    &req)) {
			SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
			spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
							 "spdk_json_decode_object failed");
			goto cleanup;
		}

		rc = vbdev_get_lvol_store_by_uuid_xor_name(req.lvs_uuid, req.lvs_name, &lvs);
		if (rc != 0) {
			spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
			goto cleanup;
		}

		lvs_bdev = vbdev_get_lvs_bdev_by_lvs(lvs);
		if (lvs_bdev == NULL) {
			spdk_jsonrpc_send_error_response(request, ENODEV, spdk_strerror(-ENODEV));
			goto cleanup;
		}
	}

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_array_begin(w);

	if (lvs_bdev != NULL) {
		rpc_dump_lvols(w, lvs_bdev);
	} else {
		for (lvs_bdev = vbdev_lvol_store_first(); lvs_bdev != NULL;
		     lvs_bdev = vbdev_lvol_store_next(lvs_bdev)) {
			rpc_dump_lvols(w, lvs_bdev);
		}
	}
	spdk_json_write_array_end(w);

	spdk_jsonrpc_end_result(request, w);

cleanup:
	free_rpc_bdev_lvol_get_lvols(&req);
}

SPDK_RPC_REGISTER("bdev_lvol_get_lvols", rpc_bdev_lvol_get_lvols, SPDK_RPC_RUNTIME)

struct rpc_bdev_lvol_grow_lvstore {
	char *uuid;
	char *lvs_name;
};

static void
free_rpc_bdev_lvol_grow_lvstore(struct rpc_bdev_lvol_grow_lvstore *req)
{
	free(req->uuid);
	free(req->lvs_name);
}

static const struct spdk_json_object_decoder rpc_bdev_lvol_grow_lvstore_decoders[] = {
	{"uuid", offsetof(struct rpc_bdev_lvol_grow_lvstore, uuid), spdk_json_decode_string, true},
	{"lvs_name", offsetof(struct rpc_bdev_lvol_grow_lvstore, lvs_name), spdk_json_decode_string, true},
};

static void
rpc_bdev_lvol_grow_lvstore_cb(void *cb_arg, int lvserrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	if (lvserrno != 0) {
		goto invalid;
	}

	spdk_jsonrpc_send_bool_response(request, true);
	return;

invalid:
	spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
					 spdk_strerror(-lvserrno));
}

static void
rpc_bdev_lvol_grow_lvstore(struct spdk_jsonrpc_request *request,
			   const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_grow_lvstore req = {};
	struct spdk_lvol_store *lvs = NULL;
	int rc;

	if (spdk_json_decode_object(params, rpc_bdev_lvol_grow_lvstore_decoders,
				    SPDK_COUNTOF(rpc_bdev_lvol_grow_lvstore_decoders),
				    &req)) {
		SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	rc = vbdev_get_lvol_store_by_uuid_xor_name(req.uuid, req.lvs_name, &lvs);
	if (rc != 0) {
		spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
		goto cleanup;
	}
	vbdev_lvs_grow(lvs, rpc_bdev_lvol_grow_lvstore_cb, request);

cleanup:
	free_rpc_bdev_lvol_grow_lvstore(&req);
}
SPDK_RPC_REGISTER("bdev_lvol_grow_lvstore", rpc_bdev_lvol_grow_lvstore, SPDK_RPC_RUNTIME)

struct rpc_bdev_lvol_shallow_copy {
	char *src_lvol_name;
	char *dst_bdev_name;
};

static void
free_rpc_bdev_lvol_shallow_copy(struct rpc_bdev_lvol_shallow_copy *req)
{
	free(req->src_lvol_name);
	free(req->dst_bdev_name);
}

static const struct spdk_json_object_decoder rpc_bdev_lvol_shallow_copy_decoders[] = {
	{"src_lvol_name", offsetof(struct rpc_bdev_lvol_shallow_copy, src_lvol_name), spdk_json_decode_string},
	{"dst_bdev_name", offsetof(struct rpc_bdev_lvol_shallow_copy, dst_bdev_name), spdk_json_decode_string},
};

static void
rpc_bdev_lvol_shallow_copy_cb(void *cb_arg, int lvolerrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	if (lvolerrno != 0) {
		goto invalid;
	}

	spdk_jsonrpc_send_bool_response(request, true);
	return;

invalid:
	spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
					 spdk_strerror(-lvolerrno));
}

static void
rpc_bdev_lvol_shallow_copy(struct spdk_jsonrpc_request *request,
			   const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_shallow_copy req = {};
	struct spdk_lvol *src_lvol;
	struct spdk_bdev *src_lvol_bdev;
	struct spdk_bdev *dst_bdev;

	SPDK_INFOLOG(lvol_rpc, "Shallow copying lvol\n");

	if (spdk_json_decode_object(params, rpc_bdev_lvol_shallow_copy_decoders,
				    SPDK_COUNTOF(rpc_bdev_lvol_shallow_copy_decoders),
				    &req)) {
		SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	src_lvol_bdev = spdk_bdev_get_by_name(req.src_lvol_name);
	if (src_lvol_bdev == NULL) {
		SPDK_ERRLOG("lvol bdev '%s' does not exist\n", req.src_lvol_name);
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	src_lvol = vbdev_lvol_get_from_bdev(src_lvol_bdev);
	if (src_lvol == NULL) {
		SPDK_ERRLOG("lvol does not exist\n");
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	dst_bdev = spdk_bdev_get_by_name(req.dst_bdev_name);
	if (dst_bdev == NULL) {
		SPDK_ERRLOG("bdev '%s' does not exist\n", req.dst_bdev_name);
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	vbdev_lvol_shallow_copy(src_lvol, req.dst_bdev_name, rpc_bdev_lvol_shallow_copy_cb, request);

cleanup:
	free_rpc_bdev_lvol_shallow_copy(&req);
}

SPDK_RPC_REGISTER("bdev_lvol_shallow_copy", rpc_bdev_lvol_shallow_copy, SPDK_RPC_RUNTIME)

struct rpc_bdev_lvol_shallow_copy_status {
	char *src_lvol_name;
};

static void
free_rpc_bdev_lvol_shallow_copy_status(struct rpc_bdev_lvol_shallow_copy_status *req)
{
	free(req->src_lvol_name);
}

static const struct spdk_json_object_decoder rpc_bdev_lvol_shallow_copy_status_decoders[] = {
	{"src_lvol_name", offsetof(struct rpc_bdev_lvol_shallow_copy_status, src_lvol_name), spdk_json_decode_string},
};

static void
rpc_bdev_lvol_shallow_copy_status(struct spdk_jsonrpc_request *request,
				  const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_shallow_copy_status req = {};
	struct spdk_bdev *src_lvol_bdev;
	struct spdk_lvol *src_lvol;
	struct spdk_json_write_ctx *w;
	uint64_t copied_clusters, total_clusters;
	int result;

	SPDK_INFOLOG(lvol_rpc, "Shallow copy status\n");

	if (spdk_json_decode_object(params, rpc_bdev_lvol_shallow_copy_status_decoders,
				    SPDK_COUNTOF(rpc_bdev_lvol_shallow_copy_status_decoders),
				    &req)) {
		SPDK_INFOLOG(lvol_rpc, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	src_lvol_bdev = spdk_bdev_get_by_name(req.src_lvol_name);
	if (src_lvol_bdev == NULL) {
		SPDK_ERRLOG("lvol bdev '%s' does not exist\n", req.src_lvol_name);
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	src_lvol = vbdev_lvol_get_from_bdev(src_lvol_bdev);
	if (src_lvol == NULL) {
		SPDK_ERRLOG("lvol does not exist\n");
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	copied_clusters = spdk_blob_get_shallow_copy_copied_clusters(src_lvol->blob);
	total_clusters = spdk_blob_get_shallow_copy_total_clusters(src_lvol->blob);
	result = spdk_blob_get_shallow_copy_result(src_lvol->blob);

	w = spdk_jsonrpc_begin_result(request);

	spdk_json_write_object_begin(w);

	spdk_json_write_named_string_fmt(w, "progress", "%lu/%lu", copied_clusters, total_clusters);
	if (result > 0) {
		spdk_json_write_named_string(w, "state", "none");
	} else if (copied_clusters < total_clusters && result == 0) {
		spdk_json_write_named_string(w, "state", "in progress");
	} else if (copied_clusters == total_clusters && result == 0) {
		spdk_json_write_named_string(w, "state", "complete");
	} else {
		spdk_json_write_named_string(w, "state", "error");
		spdk_json_write_named_string(w, "error", spdk_strerror(-result));
	}

	spdk_json_write_object_end(w);

	spdk_jsonrpc_end_result(request, w);

cleanup:
	free_rpc_bdev_lvol_shallow_copy_status(&req);
}

SPDK_RPC_REGISTER("bdev_lvol_shallow_copy_status", rpc_bdev_lvol_shallow_copy_status,
		  SPDK_RPC_RUNTIME)
		  
struct rpc_bdev_lvol_get_fragmap {
	char *name;
	uint64_t offset;
	uint64_t size;
};

struct fragmap_io {
	struct spdk_bdev *bdev;
	struct spdk_bdev_desc *bdev_desc;
	struct spdk_io_channel *bdev_io_channel;
	struct spdk_jsonrpc_request *request;

	struct spdk_bit_array *fragmap;

	uint64_t cluster_size;
	uint64_t block_size;
	uint64_t num_allocated_clusters;

	uint64_t offset;
	uint64_t size;
	uint64_t current_offset;
};

static void
free_rpc_bdev_lvol_get_fragmap(struct rpc_bdev_lvol_get_fragmap *r)
{
	free(r->name);
}

static const struct spdk_json_object_decoder rpc_bdev_lvol_get_fragmap_decoders[] = {
	{"name", offsetof(struct rpc_bdev_lvol_get_fragmap, name), spdk_json_decode_string, true},
	{"offset", offsetof(struct rpc_bdev_lvol_get_fragmap, offset), spdk_json_decode_uint64, true},
	{"size", offsetof(struct rpc_bdev_lvol_get_fragmap, size), spdk_json_decode_uint64, true},
};

static void seek_hole_done_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg);

static void
get_fragmap_done(struct fragmap_io *io, int error_code, const char *error_msg)
{
	struct spdk_json_write_ctx *w = NULL;
	char *encoded;

	if (error_code != 0) {
		spdk_jsonrpc_send_error_response_fmt(io->request, error_code, "%s: %s",
						     error_msg, spdk_strerror(-error_code));
		goto cleanup;
	} 

	encoded = spdk_bit_array_to_base64(io->fragmap);
	if (encoded == NULL) {
		SPDK_ERRLOG("Failed to encode fragmap to base64\n");
		spdk_jsonrpc_send_error_response_fmt(io->request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						     "failed to encode fragmap");
		goto cleanup;
	}

	w = spdk_jsonrpc_begin_result(io->request);
	spdk_json_write_object_begin(w);

	spdk_json_write_named_uint64(w, "cluster_size", io->cluster_size);
	spdk_json_write_named_uint64(w, "num_clusters", spdk_bit_array_capacity(io->fragmap));
	spdk_json_write_named_uint64(w, "num_allocated_clusters", io->num_allocated_clusters);
	spdk_json_write_named_string(w, "fragmap", encoded);

	spdk_json_write_object_end(w);
	spdk_jsonrpc_end_result(io->request, w);

	free(encoded);

cleanup:
	spdk_bit_array_free(&io->fragmap);
	spdk_put_io_channel(io->bdev_io_channel);
	spdk_bdev_close(io->bdev_desc);

	spdk_free(io);
}

static void
seek_data_done_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct fragmap_io *io = cb_arg;
	uint64_t next_data_offset_blocks;
	int rc;

	next_data_offset_blocks = spdk_bdev_io_get_seek_offset(bdev_io);
	spdk_bdev_free_io(bdev_io);

	if (next_data_offset_blocks == UINT64_MAX) {
		get_fragmap_done(io, 0, NULL);
		return;
	}

	io->current_offset = next_data_offset_blocks * io->block_size;
	rc = spdk_bdev_seek_hole(io->bdev_desc, io->bdev_io_channel,
				 spdk_divide_round_up(io->current_offset, io->block_size),
				 seek_hole_done_cb, io);
	if (rc != 0) {
		get_fragmap_done(io, rc, "failed to seek hole");
	}
}

static void
seek_hole_done_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct fragmap_io *io = cb_arg;
	uint64_t next_offset;
	uint64_t start_cluster;
	uint64_t num_clusters;
	int rc;

	next_offset = spdk_bdev_io_get_seek_offset(bdev_io) * io->block_size;
	next_offset = spdk_min(next_offset, io->offset + io->size);

	start_cluster = spdk_divide_round_up(io->current_offset - io->offset, io->cluster_size);
	num_clusters = spdk_divide_round_up(next_offset - io->current_offset, io->cluster_size);

	for (uint64_t i = 0; i < num_clusters; i++) {
		spdk_bit_array_set(io->fragmap, start_cluster + i);
	}
	io->num_allocated_clusters += num_clusters;

	io->current_offset = next_offset;

	if (io->current_offset == io->offset + io->size) {
		get_fragmap_done(io, 0, NULL);
		return;
	}

	rc = spdk_bdev_seek_data(io->bdev_desc, io->bdev_io_channel,
				 spdk_divide_round_up(io->current_offset, io->block_size),
				 seek_data_done_cb, io);
	if (rc != 0) {
		get_fragmap_done(io, rc, "failed to seek data");
	}
}

static void
dummy_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev, void *ctx)
{
}

static void
rpc_bdev_lvol_get_fragmap(struct spdk_jsonrpc_request *request, const struct spdk_json_val *params)
{
	struct rpc_bdev_lvol_get_fragmap req = {};
	struct spdk_bdev *bdev;
	struct spdk_lvol *lvol;
	struct spdk_bdev_desc *desc;
	struct spdk_io_channel *channel;
	struct spdk_bit_array *fragmap;
	struct fragmap_io *io;
	uint64_t cluster_size, num_clusters, block_size, num_blocks, lvol_size, segment_size;
	int rc;

	if (spdk_json_decode_object(params, rpc_bdev_lvol_get_fragmap_decoders,
				    SPDK_COUNTOF(rpc_bdev_lvol_get_fragmap_decoders),
				    &req)) {
		SPDK_ERRLOG("spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	bdev = spdk_bdev_get_by_name(req.name);
	if (bdev == NULL) {
		SPDK_ERRLOG("bdev '%s' does not exist\n", req.name);
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	lvol = vbdev_lvol_get_from_bdev(bdev);
	if (lvol == NULL) {
		SPDK_ERRLOG("lvol does not exist\n");
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	// Create a bitmap recording the allocated clusters
	cluster_size = spdk_bs_get_cluster_size(lvol->lvol_store->blobstore);
	block_size = spdk_bdev_get_block_size(bdev);
	num_blocks = spdk_bdev_get_num_blocks(bdev);
	lvol_size = num_blocks * block_size;

	if (req.offset + req.size > lvol_size) {
		SPDK_ERRLOG("offset %lu and size %lu exceed lvol size %lu\n",
			    req.offset, req.size, lvol_size);
		spdk_jsonrpc_send_error_response_fmt(request, -EINVAL,
						     "offset %lu and size %lu exceed lvol size %lu",
						     req.offset, req.size, lvol_size);
		goto cleanup;
	}

	segment_size = req.size;
	if (req.size == 0) {
		segment_size = lvol_size;
	}

	if (!spdk_is_divisible_by(req.offset, cluster_size) ||
	    !spdk_is_divisible_by(segment_size, cluster_size)) {
		SPDK_ERRLOG("offset %lu and size %lu must be a multiple of cluster size %lu\n",
			    req.offset, segment_size, cluster_size);
		spdk_jsonrpc_send_error_response_fmt(request, -EINVAL,
						     "offset %lu and size %lu must be a multiple of cluster size %lu",
						     req.offset, segment_size, cluster_size);
		goto cleanup;
	}

	num_clusters = spdk_divide_round_up(segment_size, cluster_size);
	fragmap = spdk_bit_array_create(num_clusters);
	if (fragmap == NULL) {
		SPDK_ERRLOG("failed to allocate fragmap with num_clusters %lu\n", num_clusters);
		spdk_jsonrpc_send_error_response(request, -ENOMEM,
						 spdk_strerror(ENOMEM));
		goto cleanup;
	}

	// Construct a fragmap of the lvol
	rc = spdk_bdev_open_ext(bdev->name, false,
				dummy_bdev_event_cb, NULL, &desc);
	if (rc != 0) {
		spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
		goto cleanup;
	}

	channel = spdk_bdev_get_io_channel(desc);
	if (channel == NULL) {
		spdk_bdev_close(desc);
		SPDK_ERRLOG("could not allocate I/O channel.\n");
		spdk_jsonrpc_send_error_response(request, -ENOMEM,
						 spdk_strerror(ENOMEM));
		goto cleanup;
	}

	io = spdk_zmalloc(sizeof(struct fragmap_io), 0, NULL,
			  SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
	if (io == NULL) {
		spdk_put_io_channel(channel);
		spdk_bdev_close(desc);
		spdk_jsonrpc_send_error_response(request, -ENOMEM,
						 spdk_strerror(ENOMEM));
		goto cleanup;
	}

	io->bdev = bdev;
	io->bdev_desc = desc;
	io->bdev_io_channel = channel;
	io->request = request;
	io->fragmap = fragmap;
	io->block_size = block_size;
	io->cluster_size = cluster_size;
	io->offset = req.offset;
	io->size = segment_size;
	io->current_offset = req.offset;

	rc = spdk_bdev_seek_data(desc, channel,
				 spdk_divide_round_up(req.offset, block_size),
				 seek_data_done_cb, io);
cleanup:
	free_rpc_bdev_lvol_get_fragmap(&req);
}

SPDK_RPC_REGISTER("bdev_lvol_get_fragmap", rpc_bdev_lvol_get_fragmap, SPDK_RPC_RUNTIME)
