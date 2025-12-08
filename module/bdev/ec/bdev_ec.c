/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2018 Intel Corporation.
 *   All rights reserved.
 *   Copyright (c) 2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

#include "bdev_ec.h"
#include "bdev_ec.h"
#include "spdk/stdinc.h"
#include "spdk/bdev.h"
#include "spdk/bdev_module.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "spdk/thread.h"
#include "spdk/ec.h"

/* ISA-L header for matrix generation */
#include <isa-l/erasure_code.h>


#define EC_BDEV_MAX_NAME_LENGTH 256

/* List of all ec bdevs */
struct ec_all_tailq g_ec_bdev_list = TAILQ_HEAD_INITIALIZER(g_ec_bdev_list);

static TAILQ_HEAD(, ec_bdev) g_ec_bdevs = TAILQ_HEAD_INITIALIZER(g_ec_bdevs);


/*
 * struct ec_io_channel
 *
 * This structure holds the thread-local resources required for I/O.
 * It is allocated automatically by SPDK when a thread first accesses
 * the EC bdev, based on the size passed to spdk_io_device_register().
 */
struct ec_io_channel {
	/*
	 * Channel to the SPDK Acceleration Framework.
	 * Used for submitting accel tasks (like XOR, Copy, CRC) if needed.
	 * Even if using synchronous ISA-L, keeping this allows future
	 * hardware offload integration (e.g., Intel DSA).
	*/
	struct spdk_io_channel *accel_ch;
	/*
	 * Array of IO channels for the underlying base devices.
	 * base_chans[0..k-1] -> Data Disks
	 * base_chans[k..n-1] -> Parity Disks
	 *
	 * These allow submitting I/O to specific SSDs on this specific thread
	 * without lock contention.
	*/
	struct spdk_io_channel *base_chans[EC_MAX_BASE_BDEVS];
};

/*
 * ec_bdev_init is the module init function.
 * It is called during SPDK bdev initialization.
 * Here we can perform any global initialization required for the EC bdev module.
 */
static int
ec_bdev_init(void)
{
	return 0;
}

static int
ec_bdev_get_ctx_size(void)
{
	return sizeof(struct ec_bdev_io);
}

static struct spdk_bdev_module ec_if = {
	.name = "ec",
	.module_init = ec_bdev_init,
	// .fini_start = ec_bdev_fini_start,
	// .module_fini = ec_bdev_exit,
	// .config_json = ec_bdev_config_json,
	.get_ctx_size = ec_bdev_get_ctx_size,
	// .examine_disk = ec_bdev_examine,
	.async_init = false,
	.async_fini = false,
};
SPDK_BDEV_MODULE_REGISTER(ec, &ec_if)

/* * Helper function: Free EC bdev resources.
 * Used during error cleanup or destruction.
 */
static void
ec_bdev_free(struct ec_bdev *ec)
{
    if (!ec) {
        return;
    }

    /* Free generic bdev name */
    free(ec->bdev.name);

    /* Free ISA-L specific resources */
    free(ec->encode_matrix);
    free(ec->g_tbls);

    /* Free the structure itself */
    free(ec);
}

static int
bdev_ec_init_isa_l_tables(struct ec_bdev *ec)
{
    /* Allocate memory for matrix and tables */
    ec->encode_matrix = malloc(ec->m * ec->k);
    if (!ec->encode_matrix) return -ENOMEM;

    ec->g_tbls = malloc(32 * ec->k * ec->m);
    if (!ec->g_tbls) {
        free(ec->encode_matrix);
        return -ENOMEM;
    }

    /* Generate Vandermonde matrix */
    gf_gen_rs_matrix(ec->encode_matrix, ec->m, ec->k);

    /* Initialize decoding tables (g_tbls) for fast encoding */
    ec_init_tables(ec->k, ec->m, ec->encode_matrix, ec->g_tbls);

    return 0;
}

/*
 * Internal function: _ec_bdev_create
 */
static int
_ec_bdev_create(const char *name, uint32_t strip_size, uint32_t k, uint32_t m,
		const struct spdk_uuid *uuid, struct ec_bdev **ec_bdev_out)
{
	struct ec_bdev *ec;
	struct spdk_bdev *ec_bdev_gen;
	uint32_t num_base_bdevs = k + m;

	// Validate input parameters
	if (strnlen(name, EC_BDEV_SB_NAME_SIZE) == EC_BDEV_SB_NAME_SIZE) {
		SPDK_ERRLOG("EC bdev name '%s' exceeds %d characters\n", name, EC_BDEV_SB_NAME_SIZE - 1);
		return -EINVAL;
	}

	if (spdk_bdev_get_by_name(name) != NULL) {
		SPDK_ERRLOG("Duplicate EC bdev name found: %s\n", name);
		return -EEXIST;
	}

	if (k == 0 || m == 0) {
		SPDK_ERRLOG("Invalid EC geometry k=%u, m=%u\n", k, m);
		return -EINVAL;
	}

	if (num_base_bdevs > EC_MAX_BASE_BDEVS) {
		SPDK_ERRLOG("Too many base bdevs %u (max %d)\n", num_base_bdevs, EC_MAX_BASE_BDEVS);
		return -EINVAL;
	}

	if (strip_size == 0 || spdk_u32_is_pow2(strip_size) == false) {
		SPDK_ERRLOG("Invalid strip size %" PRIu32 "\n", strip_size);
		return -EINVAL;
	}

	// Allocate and initialize ec_bdev structure
	ec = calloc(1, sizeof(*ec));
	if (!ec) {
		SPDK_ERRLOG("Unable to allocate memory for ec bdev\n");
		return -ENOMEM;
	}

	ec->k = k;
	ec->m = m;
	ec->n = num_base_bdevs;
    
	/* Store KB for now; actual block count is calculated after opening base bdevs */
	ec->strip_size_kb = strip_size; 

	/* Initialize ISA-L tables (Pre-calculation for performance) */
	if (bdev_ec_init_isa_l_tables(ec) != 0) {
		SPDK_ERRLOG("Unable to initialize ISA-L tables\n");
		ec_bdev_free(ec);
		return -ENOMEM;
	}

	ec_bdev_gen = &ec->bdev;

	ec_bdev_gen->name = strdup(name);
	if (!ec_bdev_gen->name) {
		SPDK_ERRLOG("Unable to allocate name for ec bdev\n");
		ec_bdev_free(ec);
		return -ENOMEM;
	}

	ec_bdev_gen->product_name = "ErasureCode Volume";
	ec_bdev_gen->ctxt = ec;
	ec_bdev_gen->fn_table = &g_ec_fn_table;
	ec_bdev_gen->module = &ec_if; /* Using the module interface defined previously */
	ec_bdev_gen->write_cache = 0;
    
	/* Handle UUID: Copy if provided, otherwise leave it to the caller */
	if (uuid) {
		spdk_uuid_copy(&ec_bdev_gen->uuid, uuid);
	}

	// Add to global list
	TAILQ_INSERT_TAIL(&g_ec_bdevs, ec, link);

	*ec_bdev_out = ec;

	return 0;
}

/*
 * ec_create_ch
 *
 * Callback registered with spdk_io_device_register().
 * SPDK calls this function on a thread when that thread needs to submit IO
 * to this device for the first time.
 *
 * \param io_device Pointer to the ec_bdev structure (the unique key).
 * \param ctx_buf   Pointer to the memory allocated for struct ec_io_channel.
 * \return 0 on success, negative errno on failure.
 */
static int
ec_create_ch(void *io_device, void *ctx_buf)
{
	struct ec_bdev *ec = io_device;
	struct ec_io_channel *ec_ch = ctx_buf;
	uint32_t i;

	/* Get the Acceleration Framework Channel.
	 * Even if we use synchronous ISA-L currently, keeping this allows
	 * future expansion for hardware offload (DSA) or other accel ops.
	 */
	ec_ch->accel_ch = spdk_accel_get_io_channel();
	if (!ec_ch->accel_ch) {
		SPDK_ERRLOG("Failed to get accel io channel\n");
		return -ENOMEM;
	}

	/*
	 * Get IO channels for all underlying base bdevs.
	 * This ensures that when this thread submits IO to disk 0, 1, ... n,
	 * it uses a lock-free channel specific to this thread.
	 */
	for (i = 0; i < ec->n; i++) {
		/* * spdk_bdev_get_io_channel finds the channel for the specific
		 * bdev descriptor on the current thread.
		 */
		ec_ch->base_chans[i] = spdk_bdev_get_io_channel(ec->descs[i]);

		if (!ec_ch->base_chans[i]) {
			SPDK_ERRLOG("Failed to get io channel for base bdev index %u\n", i);
			goto err_cleanup;
		}
	}

	return 0;

err_cleanup:
	/* * Error Handling:
	 * If we failed to get the 5th channel, we must release channels 0-4
	 * and the accel channel to prevent resource leaks.
	 */
	spdk_put_io_channel(ec_ch->accel_ch);
	
	/* Loop backwards from the current index i down to 0 */
	while (i > 0) {
		i--;
		if (ec_ch->base_chans[i]) {
			spdk_put_io_channel(ec_ch->base_chans[i]);
		}
	}

	return -ENOMEM;
}

/*
 * ec_destroy_ch
 *
 * Callback to release resources allocated in create_ch.
 */
static void
ec_destroy_ch(void *io_device, void *ctx_buf)
{
	struct ec_bdev *ec = io_device;
	struct ec_io_channel *ec_ch = ctx_buf;
	uint32_t i;

	/* Release all base bdev channels */
	for (i = 0; i < ec->n; i++) {
		if (ec_ch->base_chans[i]) {
			spdk_put_io_channel(ec_ch->base_chans[i]);
			ec_ch->base_chans[i] = NULL; /* Good practice */
		}
	}

	/* Release the accel channel */
	if (ec_ch->accel_ch) {
		spdk_put_io_channel(ec_ch->accel_ch);
		ec_ch->accel_ch = NULL;
	}
}

/*
 * Event Callback for base bdevs.
 * SPDK requires this if we open the bdev for writing.
 */
static void
ec_base_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev, void *event_ctx)
{
    struct ec_bdev *ec = event_ctx;

    switch (type) {
    case SPDK_BDEV_EVENT_REMOVE:
        SPDK_NOTICELOG("Base bdev %s removed from EC bdev %s\n", 
                       spdk_bdev_get_name(bdev), ec->bdev.name);
        /* * TODO: Handle Hot Removal
         * In a real implementation, we would mark the disk as missing/faulty here.
         * For this simple demo, we might just unregister the EC bdev.
         */
        break;
    default:
        SPDK_NOTICELOG("Received event %d from base bdev %s\n", type, spdk_bdev_get_name(bdev));
        break;
    }
}

/*
 * Public API: ec_bdev_create
 *
 * Allocates, initializes, and registers the EC Bdev.
 */
int
ec_bdev_create(const char *name, uint32_t strip_size_kb, uint32_t k, uint32_t m,
	       const char **base_bdev_names, const struct spdk_uuid *uuid)
{
	struct ec_bdev *ec;
	struct spdk_bdev *base_bdev;
	uint32_t i;
	int rc;
	bool io_device_registered = false; /* Flag to track registration status */

	rc = _ec_bdev_create(name, strip_size_kb, k, m, uuid, &ec);
	if (rc != 0) {
		return rc;
	}

	if (spdk_uuid_is_null(&ec->bdev.uuid)) {
		spdk_uuid_generate(&ec->bdev.uuid);
	}

	for (i = 0; i < ec->n; i++) {
		SPDK_NOTICELOG("Opening base bdev %s for EC bdev %s\n", base_bdev_names[i], name);

		/*
		 * FIX: Pass 'ec_base_bdev_event_cb' and 'ec' (context).
		 * This is required because we set write_desc=true.
		 */
		rc = spdk_bdev_open_ext(base_bdev_names[i], true, ec_base_bdev_event_cb, ec, &ec->descs[i]);
		if (rc != 0) {
			SPDK_ERRLOG("Failed to open base bdev %s: %s\n", base_bdev_names[i], spdk_strerror(-rc));
			goto error_cleanup;
		}

		/* Validate Geometry: Block length must match */
		base_bdev = spdk_bdev_desc_get_bdev(ec->descs[i]);
		if (i == 0) {
			ec->bdev.blocklen = base_bdev->blocklen;
		} else if (ec->bdev.blocklen != base_bdev->blocklen) {
			SPDK_ERRLOG("Block length mismatch for %s\n", base_bdev_names[i]);
			rc = -EINVAL;
			goto error_cleanup;
		}
	}

	/* Calculate Final Geometry */
	/* Convert strip size from KB to blocks */
	ec->strip_size = (ec->strip_size_kb * 1024) / ec->bdev.blocklen;

	/* Calculate full stripe data size (k * strip_size) */
	ec->stripe_blocks = ec->k * ec->strip_size;

	/* Calculate total capacity based on the first disk */
	/* Note: Real implementation might handle mixed disk sizes by picking the smallest */
	base_bdev = spdk_bdev_desc_get_bdev(ec->descs[0]);
	ec->bdev.blockcnt = (base_bdev->blockcnt / ec->strip_size) * ec->stripe_blocks;

	/* Set Constraints for Full Stripe Writes */
	ec->bdev.write_unit_size = ec->stripe_blocks;
	ec->bdev.optimal_io_boundary = ec->strip_size;
	ec->bdev.split_on_write_unit = true;
	ec->bdev.split_on_optimal_io_boundary = true;

	/* Register IO Device */
	spdk_io_device_register(ec, ec_create_ch, ec_destroy_ch,
				sizeof(struct ec_io_channel), name);
	io_device_registered = true; /* Mark as registered to enable safe cleanup */

	/* Register Bdev */
	rc = spdk_bdev_register(&ec->bdev);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to register bdev\n");
		goto error_cleanup;
	}

	SPDK_NOTICELOG("Created EC bdev %s (k=%u, m=%u)\n", name, k, m);
	return 0;

error_cleanup:
	/* FIX: Only unregister if we actually registered it */
	if (io_device_registered) {
		spdk_io_device_unregister(ec, NULL);
	}

	/* Close opened descriptors */
	for (i = 0; i < ec->n; i++) {
		if (ec->descs[i]) {
			spdk_bdev_close(ec->descs[i]);
		}
	}

	/* Remove from list and free memory */
	TAILQ_REMOVE(&g_ec_bdevs, ec, link);
	ec_bdev_free(ec);

	return rc;
}

/*
 * Destruct: Called when the Bdev is being unregistered (deleted).
 * We need to close base devices, unregister the IO device, and free memory.
 */
static int
ec_destruct(void *ctx)
{
	struct ec_bdev *ec = ctx;
	uint32_t i;

	/* Remove from the global tailq */
	TAILQ_REMOVE(&g_ec_bdevs, ec, link);

	/* Unregister the IO device to stop new channels from being created */
	spdk_io_device_unregister(ec, NULL);

	/* Close all underlying base bdev descriptors */
	for (i = 0; i < ec->n; i++) {
		if (ec->descs[i]) {
			spdk_bdev_close(ec->descs[i]);
		}
	}

	/* Free the structure and its resources (using the helper we defined earlier) */
	ec_bdev_free(ec);
	return 0;
}

/*
 * Check Supported IO Types: Tell SPDK what operations we allow.
 */
static bool
ec_io_type_supported(void *ctx, enum spdk_bdev_io_type type)
{
	switch (type) {
	case SPDK_BDEV_IO_TYPE_READ:
	case SPDK_BDEV_IO_TYPE_WRITE:
		return true;
    
	/* * We can optionally support FLUSH/RESET if we implement the logic.
	 * For a simple EC, we might pass them through to all base bdevs.
	 * For now, let's strictly support Read/Write only.
	 */
	default:
		return false;
	}
}

/*
 * Get IO Channel: Returns the thread-local channel for this bdev.
 * This wraps spdk_get_io_channel(), which looks up the registered IO device.
 */
static struct spdk_io_channel *
ec_get_io_channel(void *ctx)
{
	struct ec_bdev *ec = ctx;
	return spdk_get_io_channel(ec);
}

/*
 * Dump Info JSON: Used for 'bdev_get_bdevs' RPC output.
 * Shows EC-specific details in the "driver_specific" section.
 */
static int
ec_dump_info_json(void *ctx, struct spdk_json_write_ctx *w)
{
	struct ec_bdev *ec = ctx;
	struct spdk_bdev *base_bdev;
	uint32_t i;

	spdk_json_write_named_object_begin(w, "ec");

	spdk_json_write_named_uint32(w, "data_chunk_count", ec->k);
	spdk_json_write_named_uint32(w, "parity_chunk_count", ec->m);
	spdk_json_write_named_uint32(w, "strip_size_kb", ec->strip_size_kb);
	spdk_json_write_named_uint32(w, "num_base_bdevs", ec->n);

	/* Output the list of base bdev names for debugging */
	spdk_json_write_named_array_begin(w, "base_bdevs");
	for (i = 0; i < ec->n; i++) {
		base_bdev = spdk_bdev_desc_get_bdev(ec->descs[i]);
		if (base_bdev) {
			spdk_json_write_string(w, spdk_bdev_get_name(base_bdev));
		}
	}
	spdk_json_write_array_end(w);

	spdk_json_write_object_end(w);

	return 0;
}

/*
 * Write Config JSON: Used for 'save_config' RPC to persist the bdev.
 * This must output a JSON object that matches the 'bdev_ec_create' RPC parameters.
 */
static void
ec_write_config_json(struct spdk_bdev *bdev, struct spdk_json_write_ctx *w)
{
	struct ec_bdev *ec = SPDK_CONTAINEROF(bdev, struct ec_bdev, bdev);
	struct spdk_bdev *base_bdev;
	uint32_t i;

	spdk_json_write_object_begin(w);

	/* The RPC method name to call when restoring this config */
	spdk_json_write_named_string(w, "method", "bdev_ec_create");

	spdk_json_write_named_object_begin(w, "params");
	spdk_json_write_named_string(w, "name", bdev->name);
	spdk_json_write_named_uint32(w, "data_chunk_count", ec->k);
	spdk_json_write_named_uint32(w, "parity_chunk_count", ec->m);
	spdk_json_write_named_uint32(w, "strip_size_kb", ec->strip_size_kb);

	spdk_json_write_named_array_begin(w, "base_bdevs");
	for (i = 0; i < ec->n; i++) {
		base_bdev = spdk_bdev_desc_get_bdev(ec->descs[i]);
		if (base_bdev) {
			spdk_json_write_string(w, spdk_bdev_get_name(base_bdev));
		}
	}
	spdk_json_write_array_end(w);

	spdk_json_write_object_end(w); /* End params */
	spdk_json_write_object_end(w); /* End method object */
}

/*
 * ec_bdev_io_init
 *
 * Initializes the ec_bdev_io structure.
 * This is called on the hot path for every I/O.
 *
 * \param ec_io   Pointer to the driver_ctx memory.
 * \param ch      The thread-local channel.
 * \param bdev_io The parent I/O request.
 */
static inline void
ec_bdev_io_init(struct ec_bdev_io *ec_io, struct ec_io_channel *ch,
                struct spdk_bdev_io *bdev_io)
{
	/*
	 * OPTIMIZATION: Do not use memset(ec_io, 0, sizeof(*ec_io)).
	 * Since this memory comes from a pre-allocated pool, we only need to
	 * overwrite fields we actually use or need to reset.
	 * Direct assignment is faster than memset.
	 */

	/* Setup Links */
	ec_io->bdev_io = bdev_io;
	ec_io->ch = ch;

	/* Copy Parameters to Private Workspace */
	ec_io->offset_blocks = bdev_io->u.bdev.offset_blocks;
	ec_io->num_blocks = bdev_io->u.bdev.num_blocks;
	ec_io->iovs = bdev_io->u.bdev.iovs;
	ec_io->iovcnt = bdev_io->u.bdev.iovcnt;

	/* Reset State & Counters */
	ec_io->base_io_remaining = 0;
	ec_io->status = SPDK_BDEV_IO_STATUS_SUCCESS;

	/* Nullify Resource Pointers
	 * This is crucial because driver_ctx memory is reused.
	 * If we don't NULL these, the completion handler might try to
	 * double-free garbage pointers from a previous I/O.
	 */
	ec_io->data_iovs = NULL;
	ec_io->parity_iovs = NULL;
	ec_io->parity_bufs = NULL;
}

/*
 * ec_child_io_complete
 *
 * Callback function called by SPDK when a read/write to a base bdev completes.
 *
 * \param child_io  The child I/O that just completed.
 * \param success   True if the child I/O succeeded, false otherwise.
 * \param cb_arg    The context (struct ec_bdev_io *) passed during submission.
 */
static void
ec_child_io_complete(struct spdk_bdev_io *child_io, bool success, void *cb_arg)
{
    struct ec_bdev_io *ec_io = cb_arg;
    struct ec_bdev *ec = (struct ec_bdev *)ec_io->bdev_io->bdev->ctxt;
    uint32_t i;

    spdk_bdev_free_io(child_io);

    if (!success) {
        ec_io->status = SPDK_BDEV_IO_STATUS_FAILED;
    }

    ec_io->base_io_remaining--;

    if (ec_io->base_io_remaining == 0) {
        
        /* Free Bounce Buffer */
        if (ec_io->bounce_buf) {
            spdk_dma_free(ec_io->bounce_buf);
            ec_io->bounce_buf = NULL;
        }

        /* Free Parity Resources */
        if (ec_io->parity_bufs) {
            for (i = 0; i < ec->m; i++) {
                if (ec_io->parity_bufs[i]) {
                    spdk_dma_free(ec_io->parity_bufs[i]);
                }
            }
            free(ec_io->parity_bufs);
            ec_io->parity_bufs = NULL;
        }

        if (ec_io->parity_iovs) {
            free(ec_io->parity_iovs);
            ec_io->parity_iovs = NULL;
        }

        if (ec_io->data_iovs) {
            free(ec_io->data_iovs);
            ec_io->data_iovs = NULL;
        }

        spdk_bdev_io_complete(ec_io->bdev_io, ec_io->status);
    }
}

/*
 * ec_submit_read
 *
 * Handles Read Requests.
 * Strategy: Normal Read (Pass-through).
 *
 * We assume the IO has already been split by the generic layer based on
 * 'optimal_io_boundary', so a single read request will never cross
 * a strip (chunk) boundary.
 */
static int
ec_submit_read(struct ec_bdev_io *ec_io)
{
	struct ec_bdev *ec = (struct ec_bdev *)ec_io->bdev_io->bdev->ctxt;

	/*
	 * USE PRIVATE COPY:
	 * We use the parameters stored in our private ec_io structure.
	 * This decouples our logic from the parent bdev_io.
	 */
	uint64_t offset_blocks = ec_io->offset_blocks;
	uint64_t num_blocks = ec_io->num_blocks;

	/* Calculate Geometry */
	/* Which logical stripe are we in? */
	uint64_t stripe_index = offset_blocks / ec->stripe_blocks;

	/* Offset within the stripe (in blocks) */
	uint64_t stripe_offset = offset_blocks % ec->stripe_blocks;

	/* Which Data Disk (0 to k-1) does this fall into? */
	uint32_t chunk_idx = stripe_offset / ec->strip_size;

	/* Offset within that specific chunk (in blocks) */
	uint64_t chunk_offset = stripe_offset % ec->strip_size;

	/* Calculate physical LBA on the underlying base device */
	uint64_t base_lba = (stripe_index * ec->strip_size) + chunk_offset;

	/* Setup Tracking */
	/* We expect exactly 1 child IO completion for a normal read */
	ec_io->base_io_remaining = 1;
	ec_io->status = SPDK_BDEV_IO_STATUS_SUCCESS;

	/* Submit Pass-through Read */
	/* Note: We pass ec_io->iovs and ec_io->iovcnt */
	int rc = spdk_bdev_readv_blocks(ec->descs[chunk_idx],
					ec_io->ch->base_chans[chunk_idx],
					ec_io->iovs,
					ec_io->iovcnt,
					base_lba,
					num_blocks,
					ec_child_io_complete,
					ec_io);

	return rc;
}

/*
 * ec_submit_write
 *
 * Handles Write Requests.
 * Strategy: Full Stripe Write Only.
 */
static int
ec_submit_write(struct ec_bdev_io *ec_io)
{
    struct ec_bdev *ec = (struct ec_bdev *)ec_io->bdev_io->bdev->ctxt;
    uint32_t i;
    int rc = 0;

    uint64_t chunk_blocks = ec->strip_size;
    uint64_t chunk_bytes = chunk_blocks * ec->bdev.blocklen;
    uint64_t total_data_bytes = ec->stripe_blocks * ec->bdev.blocklen;

    if (ec_io->num_blocks != ec->stripe_blocks) {
        SPDK_ERRLOG("Write IO must be full stripe size\n");
        return -EINVAL;
    }

    ec_io->bounce_buf = spdk_dma_zmalloc(total_data_bytes, 4096, NULL);
    if (!ec_io->bounce_buf) {
        return -ENOMEM;
    }

    /* spdk_copy_iovs_to_buf flattens the scatter-gather list into our linear buffer. */
    spdk_copy_iovs_to_buf(ec_io->bounce_buf, total_data_bytes,
                               ec_io->iovs, ec_io->iovcnt);

    ec_io->data_iovs = calloc(ec->k, sizeof(struct iovec));
    if (!ec_io->data_iovs) {
        rc = -ENOMEM;
        goto error;
    }

    uint8_t *data_base = ec_io->bounce_buf;

    for (i = 0; i < ec->k; i++) {
        ec_io->data_iovs[i].iov_base = data_base + (i * chunk_bytes);
        ec_io->data_iovs[i].iov_len = chunk_bytes;
    }

    /* Prepare Parity Buffers & IOVs */
    ec_io->parity_iovs = calloc(ec->m, sizeof(struct iovec));
    ec_io->parity_bufs = calloc(ec->m, sizeof(void *));

    if (!ec_io->parity_iovs || !ec_io->parity_bufs) {
        rc = -ENOMEM;
        goto error;
    }

    for (i = 0; i < ec->m; i++) {
        ec_io->parity_bufs[i] = spdk_dma_zmalloc(chunk_bytes, 4096, NULL);
        if (!ec_io->parity_bufs[i]) {
            rc = -ENOMEM;
            goto error;
        }
        ec_io->parity_iovs[i].iov_base = ec_io->parity_bufs[i];
        ec_io->parity_iovs[i].iov_len = chunk_bytes;
    }


    void *data_ptrs[EC_MAX_BASE_BDEVS];
    void *parity_ptrs[EC_MAX_BASE_BDEVS];

    for (i = 0; i < ec->k; i++) data_ptrs[i] = ec_io->data_iovs[i].iov_base;
    for (i = 0; i < ec->m; i++) parity_ptrs[i] = ec_io->parity_iovs[i].iov_base;

    spdk_ec_encode(parity_ptrs, ec->m, data_ptrs, ec->k, ec->g_tbls, chunk_bytes);

    /* Submit Writes to Disks */
    uint64_t stripe_index = ec_io->offset_blocks / ec->stripe_blocks;
    uint64_t offset_in_disk = stripe_index * ec->strip_size;

    ec_io->base_io_remaining = ec->n;
    ec_io->status = SPDK_BDEV_IO_STATUS_SUCCESS;

    /* Write Data Chunks */
    for (i = 0; i < ec->k; i++) {
        rc = spdk_bdev_writev_blocks(ec->descs[i],
                                     ec_io->ch->base_chans[i],
                                     &ec_io->data_iovs[i], 1,
                                     offset_in_disk,
                                     chunk_blocks,
                                     ec_child_io_complete,
                                     ec_io);
        if (rc != 0) {
            ec_io->base_io_remaining--;
            ec_io->status = SPDK_BDEV_IO_STATUS_FAILED;
        }
    }

    /* Write Parity Chunks */
    for (i = 0; i < ec->m; i++) {
        uint32_t bdev_idx = ec->k + i;
        rc = spdk_bdev_writev_blocks(ec->descs[bdev_idx],
                                     ec_io->ch->base_chans[bdev_idx],
                                     &ec_io->parity_iovs[i], 1,
                                     offset_in_disk,
                                     chunk_blocks,
                                     ec_child_io_complete,
                                     ec_io);
        if (rc != 0) {
            ec_io->base_io_remaining--;
            ec_io->status = SPDK_BDEV_IO_STATUS_FAILED;
        }
    }

    if (ec_io->base_io_remaining == 0) {
        rc = -EIO;
        goto error;
    }

    return 0;

error:
    /* Cleanup on failure */
    if (ec_io->bounce_buf) spdk_dma_free(ec_io->bounce_buf);

    if (ec_io->parity_bufs) {
        for (i = 0; i < ec->m; i++) {
            if (ec_io->parity_bufs[i]) spdk_dma_free(ec_io->parity_bufs[i]);
        }
        free(ec_io->parity_bufs);
    }
    if (ec_io->parity_iovs) free(ec_io->parity_iovs);
    if (ec_io->data_iovs) free(ec_io->data_iovs);

    return rc;
}

static void
ec_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	/* Access the pre-allocated driver_ctx */
	struct ec_bdev_io *ec_io = (struct ec_bdev_io *)bdev_io->driver_ctx;
	struct ec_io_channel *ec_ch = spdk_io_channel_get_ctx(ch);
	int rc = 0;

	/* Initialize the private context (Copy params from bdev_io) */
	ec_bdev_io_init(ec_io, ec_ch, bdev_io);

	/* Dispatch based on IO type */
	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		rc = ec_submit_read(ec_io);
		break;

	case SPDK_BDEV_IO_TYPE_WRITE:
		SPDK_NOTICELOG("Submitting EC Write IO\n");
		rc = ec_submit_write(ec_io);
		break;

	case SPDK_BDEV_IO_TYPE_RESET:
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
		return;

	case SPDK_BDEV_IO_TYPE_FLUSH:
	case SPDK_BDEV_IO_TYPE_UNMAP:
		/* Placeholder: Complete successfully */
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
		return;

	default:
		SPDK_ERRLOG("Invalid IO type %d\n", bdev_io->type);
		rc = -EINVAL;
		break;
	}

	/* Handle Submission Failures
	 * If rc != 0, the IO was NOT submitted to the base device.
	 * We must complete it here to avoid hanging the caller.
	 */
	if (rc != 0) {
		if (rc == -ENOMEM) {
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_NOMEM);
		} else {
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
		/* Note: No need to free(ec_io) because it's part of bdev_io */
	}
}

/* g_ec_fn_table is the function table for ec bdev */
static const struct spdk_bdev_fn_table g_ec_fn_table = {
	.destruct           = ec_destruct,
	.submit_request     = ec_submit_request, /* Implemented in previous steps */
	.io_type_supported  = ec_io_type_supported,
	.get_io_channel     = ec_get_io_channel,
	.dump_info_json     = ec_dump_info_json,
	.write_config_json  = ec_write_config_json,
	/* * Optional callbacks that can be added later:
	 * .get_spin_time   - For QoS
	 * .get_memory_domains - For DMA handling
	 */
};