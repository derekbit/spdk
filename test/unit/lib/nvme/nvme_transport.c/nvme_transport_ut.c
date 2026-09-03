/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2021 Intel Corporation.
 *   All rights reserved.
 *   Copyright (c) 2021, 2022 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

#include "spdk/stdinc.h"
#include "spdk_internal/cunit.h"
#include "nvme/nvme_transport.c"
#include "common/lib/test_env.c"

SPDK_LOG_REGISTER_COMPONENT(nvme)

DEFINE_STUB(nvme_poll_group_connect_qpair, int, (struct spdk_nvme_qpair *qpair), 0);
DEFINE_STUB_V(nvme_qpair_abort_all_queued_reqs, (struct spdk_nvme_qpair *qpair));
DEFINE_STUB(nvme_poll_group_disconnect_qpair, int, (struct spdk_nvme_qpair *qpair), 0);
DEFINE_STUB(spdk_nvme_ctrlr_free_io_qpair, int, (struct spdk_nvme_qpair *qpair), 0);
DEFINE_STUB(spdk_nvme_transport_id_trtype_str, const char *,
	    (enum spdk_nvme_transport_type trtype), NULL);
DEFINE_STUB(spdk_nvme_qpair_process_completions, int32_t, (struct spdk_nvme_qpair *qpair,
		uint32_t max_completions), 0);
DEFINE_STUB(nvme_ctrlr_get_current_process, struct spdk_nvme_ctrlr_process *,
	    (struct spdk_nvme_ctrlr *ctrlr), NULL);
DEFINE_STUB(spdk_nvme_ctrlr_is_fabrics, bool, (struct spdk_nvme_ctrlr *ctrlr), false);
DEFINE_STUB(nvme_qpair_state_string, const char *, (enum nvme_qpair_state state), "UT_STATE");

static struct spdk_poller *g_ut_poller = (struct spdk_poller *)0x1;
static int g_ut_pollers_armed;
static int g_ut_connect_cb_calls;
static struct spdk_nvme_qpair *g_ut_fail_qpair;

struct spdk_poller *
spdk_poller_register(spdk_poller_fn fn, void *arg, uint64_t period_microseconds)
{
	g_ut_pollers_armed++;
	return g_ut_poller;
}

void
spdk_poller_unregister(struct spdk_poller **ppoller)
{
	if (*ppoller != NULL) {
		g_ut_pollers_armed--;
		*ppoller = NULL;
	}
}

/* Reports the qpair as failed once, mimicking a poll group that drops a
 * connecting qpair while nvme_connect_poller() is on the stack.
 */
int64_t
spdk_nvme_poll_group_process_completions(struct spdk_nvme_poll_group *group,
		uint32_t completions_per_qpair,
		spdk_nvme_disconnected_qpair_cb disconnected_qpair_cb)
{
	struct spdk_nvme_qpair *qpair = g_ut_fail_qpair;

	if (qpair != NULL) {
		g_ut_fail_qpair = NULL;
		disconnected_qpair_cb(qpair, NULL);
	}

	return 0;
}

static void
ut_connect_cb(struct spdk_nvme_qpair *qpair, int status, void *cb_arg)
{
	g_ut_connect_cb_calls++;
}

static void
ut_disconnect_qpair(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_qpair *qpair)
{
}

static void
ut_construct_transport(struct spdk_nvme_transport *transport, const char name[])
{
	memcpy(transport->ops.name, name, strlen(name));
	TAILQ_INSERT_TAIL(&g_spdk_nvme_transports, transport, link);
}

static void
test_nvme_get_transport(void)
{
	const struct spdk_nvme_transport *nvme_transport = NULL;
	struct spdk_nvme_transport new_transport = {};

	ut_construct_transport(&new_transport, "new_transport");

	nvme_transport = nvme_get_transport("new_transport");
	CU_ASSERT(nvme_transport == &new_transport);
	TAILQ_REMOVE(&g_spdk_nvme_transports, nvme_transport, link);
	CU_ASSERT(TAILQ_EMPTY(&g_spdk_nvme_transports));

	/* Unavailable transport entry */
	nvme_transport = nvme_get_transport("new_transport");
	SPDK_CU_ASSERT_FATAL(nvme_transport == NULL);
}

static void
test_nvme_transport_poll_group_connect_qpair(void)
{
	int rc;
	struct spdk_nvme_qpair qpair = {};
	struct spdk_nvme_transport_poll_group	tgroup = {};
	struct spdk_nvme_transport transport = {};

	qpair.poll_group = &tgroup;
	tgroup.transport = &transport;
	STAILQ_INIT(&tgroup.connected_qpairs);
	STAILQ_INIT(&tgroup.disconnected_qpairs);

	/* Connected qpairs */
	qpair.poll_group_tailq_head = &tgroup.connected_qpairs;

	rc = nvme_transport_poll_group_connect_qpair(&qpair);
	CU_ASSERT(rc == 0);

	/* Disconnected qpairs */

	qpair.poll_group_tailq_head = &tgroup.disconnected_qpairs;
	STAILQ_INSERT_TAIL(&tgroup.disconnected_qpairs, &qpair, poll_group_stailq);

	rc = nvme_transport_poll_group_connect_qpair(&qpair);
	CU_ASSERT(rc == 0);
	CU_ASSERT(STAILQ_EMPTY(&tgroup.disconnected_qpairs));
	CU_ASSERT(!STAILQ_EMPTY(&tgroup.connected_qpairs));
	STAILQ_REMOVE(&tgroup.connected_qpairs, &qpair, spdk_nvme_qpair, poll_group_stailq);
	CU_ASSERT(STAILQ_EMPTY(&tgroup.connected_qpairs));

	/* None qpairs */
	qpair.poll_group_tailq_head = NULL;

	rc = nvme_transport_poll_group_connect_qpair(&qpair);
	SPDK_CU_ASSERT_FATAL(rc == -EINVAL);
}

static void
test_nvme_transport_poll_group_disconnect_qpair(void)
{
	int rc;
	struct spdk_nvme_qpair qpair = {};
	struct spdk_nvme_transport_poll_group	tgroup = {};
	struct spdk_nvme_transport transport = {};

	qpair.poll_group = &tgroup;
	tgroup.transport = &transport;
	STAILQ_INIT(&tgroup.connected_qpairs);
	STAILQ_INIT(&tgroup.disconnected_qpairs);

	/* Disconnected qpairs */
	qpair.poll_group_tailq_head = &tgroup.disconnected_qpairs;

	rc = nvme_transport_poll_group_disconnect_qpair(&qpair);
	CU_ASSERT(rc == 0);

	/* Connected qpairs */
	qpair.poll_group_tailq_head = &tgroup.connected_qpairs;
	STAILQ_INSERT_TAIL(&tgroup.connected_qpairs, &qpair, poll_group_stailq);
	tgroup.num_connected_qpairs++;

	rc = nvme_transport_poll_group_disconnect_qpair(&qpair);
	CU_ASSERT(rc == 0);
	CU_ASSERT(STAILQ_EMPTY(&tgroup.connected_qpairs));
	CU_ASSERT(tgroup.num_connected_qpairs == 0);
	CU_ASSERT(!STAILQ_EMPTY(&tgroup.disconnected_qpairs));
	STAILQ_REMOVE(&tgroup.disconnected_qpairs, &qpair, spdk_nvme_qpair, poll_group_stailq);
	CU_ASSERT(STAILQ_EMPTY(&tgroup.disconnected_qpairs));

	/* None qpairs */
	qpair.poll_group_tailq_head = NULL;

	rc = nvme_transport_poll_group_disconnect_qpair(&qpair);
	SPDK_CU_ASSERT_FATAL(rc == -EINVAL);
}

static void
test_nvme_transport_poll_group_add_remove(void)
{
	int rc;
	struct spdk_nvme_transport_poll_group tgroup = {};
	struct spdk_nvme_qpair qpair = {};
	const struct spdk_nvme_transport transport = {};

	tgroup.transport = &transport;
	qpair.poll_group = &tgroup;
	qpair.state = NVME_QPAIR_DISCONNECTED;
	STAILQ_INIT(&tgroup.connected_qpairs);
	STAILQ_INIT(&tgroup.disconnected_qpairs);

	/* Add qpair */
	rc = nvme_transport_poll_group_add(&tgroup, &qpair);
	CU_ASSERT(rc == 0);
	CU_ASSERT(qpair.poll_group_tailq_head == &tgroup.disconnected_qpairs);
	CU_ASSERT(STAILQ_FIRST(&tgroup.disconnected_qpairs) == &qpair);

	/*  Remove disconnected_qpairs */
	SPDK_CU_ASSERT_FATAL(!STAILQ_EMPTY(&tgroup.disconnected_qpairs));

	rc = nvme_transport_poll_group_remove(&tgroup, &qpair);
	CU_ASSERT(rc == 0);
	CU_ASSERT(STAILQ_EMPTY(&tgroup.disconnected_qpairs));
	CU_ASSERT(qpair.poll_group == NULL);
	CU_ASSERT(qpair.poll_group_tailq_head == NULL);

	/* Remove connected_qpairs */
	qpair.poll_group_tailq_head = &tgroup.connected_qpairs;
	STAILQ_INSERT_TAIL(&tgroup.connected_qpairs, &qpair, poll_group_stailq);

	rc = nvme_transport_poll_group_remove(&tgroup, &qpair);
	CU_ASSERT(rc == -EINVAL);

	STAILQ_REMOVE(&tgroup.connected_qpairs, &qpair, spdk_nvme_qpair, poll_group_stailq);

	/* Invalid qpair */
	qpair.poll_group_tailq_head = NULL;

	rc = nvme_transport_poll_group_remove(&tgroup, &qpair);
	CU_ASSERT(rc == -ENOENT);
}

static int
g_ut_ctrlr_get_memory_domains(const struct spdk_nvme_ctrlr *ctrlr,
			      struct spdk_memory_domain **domains, int array_size)
{
	return 1;
}

static void
test_ctrlr_get_memory_domains(void)
{
	struct spdk_nvme_ctrlr ctrlr = {
		.trid = {
			.trstring = "new_transport"
		}
	};
	struct spdk_nvme_transport new_transport = {
		.ops = { .ctrlr_get_memory_domains = g_ut_ctrlr_get_memory_domains }
	};

	ut_construct_transport(&new_transport, "new_transport");

	/* transport contains necessary op */
	CU_ASSERT(nvme_transport_ctrlr_get_memory_domains(&ctrlr, NULL, 0) == 1);

	/* transport doesn't contain necessary op */
	new_transport.ops.ctrlr_get_memory_domains = NULL;
	CU_ASSERT(nvme_transport_ctrlr_get_memory_domains(&ctrlr, NULL, 0) == 0);

	TAILQ_REMOVE(&g_spdk_nvme_transports, &new_transport, link);
}

static void
test_nvme_transport_qpair_abort_async_connect(void)
{
	struct spdk_nvme_transport_poll_group tgroup = {};
	struct spdk_nvme_poll_group group = {};
	struct spdk_nvme_ctrlr ctrlr = {};
	struct spdk_nvme_qpair qpair = {};
	int rc;

	qpair.ctrlr = &ctrlr;
	qpair.poll_group = &tgroup;
	tgroup.group = &group;

	g_ut_pollers_armed = 0;
	g_ut_connect_cb_calls = 0;

	/* Aborting with nothing armed is a no-op. */
	nvme_transport_qpair_abort_async_connect(&qpair);
	CU_ASSERT(qpair.async_connect_ctx == NULL);
	CU_ASSERT(g_ut_connect_cb_calls == 0);

	rc = start_async_qpair_connect(&qpair, ut_connect_cb, NULL);
	CU_ASSERT(rc == 0);
	SPDK_CU_ASSERT_FATAL(qpair.async_connect_ctx != NULL);
	CU_ASSERT(g_ut_pollers_armed == 1);

	/* Cancelling outside the poller completes and frees straight away. */
	nvme_transport_qpair_abort_async_connect(&qpair);
	CU_ASSERT(qpair.async_connect_ctx == NULL);
	CU_ASSERT(g_ut_pollers_armed == 0);
	CU_ASSERT(g_ut_connect_cb_calls == 1);

	/* Idempotent. */
	nvme_transport_qpair_abort_async_connect(&qpair);
	CU_ASSERT(g_ut_pollers_armed == 0);
	CU_ASSERT(g_ut_connect_cb_calls == 1);
}

static void
test_nvme_transport_connect_poller_reentrant_disconnect(void)
{
	struct spdk_nvme_transport_poll_group tgroup = {};
	struct spdk_nvme_poll_group group = {};
	struct spdk_nvme_transport transport = {};
	struct spdk_nvme_ctrlr ctrlr = {};
	struct spdk_nvme_qpair qpair = {};
	struct connect_ctx *ctx;
	int rc;

	ut_construct_transport(&transport, "reentrant_transport");
	memcpy(ctrlr.trid.trstring, "reentrant_transport", strlen("reentrant_transport"));
	transport.ops.ctrlr_disconnect_qpair = ut_disconnect_qpair;

	qpair.ctrlr = &ctrlr;
	qpair.poll_group = &tgroup;
	tgroup.group = &group;

	g_ut_pollers_armed = 0;
	g_ut_connect_cb_calls = 0;

	/* Take the fabrics branch of the poller so it drives the poll group. */
	MOCK_SET(spdk_nvme_ctrlr_is_fabrics, true);

	rc = start_async_qpair_connect(&qpair, ut_connect_cb, NULL);
	CU_ASSERT(rc == 0);
	SPDK_CU_ASSERT_FATAL(qpair.async_connect_ctx != NULL);
	CU_ASSERT(g_ut_pollers_armed == 1);
	ctx = qpair.async_connect_ctx;

	nvme_qpair_set_state(&qpair, NVME_QPAIR_CONNECTING);

	/* The poll group reports the qpair failed from inside the poller, which
	 * re-enters nvme_transport_ctrlr_disconnect_qpair() while this same
	 * nvme_connect_poller() frame still holds ctx.
	 */
	g_ut_fail_qpair = &qpair;
	CU_ASSERT(nvme_connect_poller(ctx) == SPDK_POLLER_BUSY);

	/* The connect must be completed exactly once and the ctx freed exactly
	 * once, no matter which side of the re-entry cancels it.
	 */
	CU_ASSERT(g_ut_connect_cb_calls == 1);
	CU_ASSERT(g_ut_pollers_armed == 0);
	CU_ASSERT(qpair.async_connect_ctx == NULL);

	MOCK_CLEAR(spdk_nvme_ctrlr_is_fabrics);
	TAILQ_REMOVE(&g_spdk_nvme_transports, &transport, link);
}

int
main(int argc, char **argv)
{
	CU_pSuite	suite = NULL;
	unsigned int	num_failures;

	CU_initialize_registry();

	suite = CU_add_suite("nvme_transport", NULL, NULL);
	CU_ADD_TEST(suite, test_nvme_get_transport);
	CU_ADD_TEST(suite, test_nvme_transport_poll_group_connect_qpair);
	CU_ADD_TEST(suite, test_nvme_transport_poll_group_disconnect_qpair);
	CU_ADD_TEST(suite, test_nvme_transport_poll_group_add_remove);
	CU_ADD_TEST(suite, test_ctrlr_get_memory_domains);
	CU_ADD_TEST(suite, test_nvme_transport_qpair_abort_async_connect);
	CU_ADD_TEST(suite, test_nvme_transport_connect_poller_reentrant_disconnect);

	num_failures = spdk_ut_run_tests(argc, argv, NULL);
	CU_cleanup_registry();
	return num_failures;
}
