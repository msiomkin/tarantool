/*
 * Copyright 2010-2019, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "memory.h"
#include "fiber.h"
#include "uuid/tt_uuid.h"
#include "unit.h"
#include "swim/swim.h"
#include "swim_test_transport.h"
#include "swim_test_ev.h"
#include "swim_test_utils.h"
#include <fcntl.h>

/**
 * Test result is a real returned value of main_f. Fiber_join can
 * not be used, because it expects if a returned value < 0 then
 * diag is not empty. But in unit tests it can be violated -
 * check_plan() does not set diag.
 */
static int test_result;

static void
swim_test_one_link(void)
{
	swim_start_test(6);
	/*
	 * Run a simple cluster of two elements. One of them
	 * learns about another explicitly. Another should add the
	 * former into his table of members.
	 */
	struct swim_cluster *cluster = swim_cluster_new(2, -1);
	fail_if(swim_cluster_add_link(cluster, 0, 1) != 0);
	is(swim_cluster_wait_fullmesh(cluster, 0.9), -1,
	   "no rounds - no fullmesh");
	is(swim_cluster_wait_fullmesh(cluster, 0.1), 0, "one link");

	is(swim_cluster_member_status(cluster, 0, 0), MEMBER_ALIVE,
	   "self 0 is alive");
	is(swim_cluster_member_status(cluster, 1, 1), MEMBER_ALIVE,
	   "self 1 is alive");
	is(swim_cluster_member_status(cluster, 0, 1), MEMBER_ALIVE,
	   "0 sees 1 as alive");
	is(swim_cluster_member_status(cluster, 1, 0), MEMBER_ALIVE,
	   "1 sees 0 as alive");
	swim_cluster_delete(cluster);

	swim_finish_test();
}

static void
swim_test_sequence(void)
{
	swim_start_test(1);
	/*
	 * Run a simple cluster of several elements. Build a
	 * 'forward list' from them. It should turn into fullmesh
	 * in O(N) time. Time is not fixed because of randomness,
	 * so here just in case 2N is used - it should be enough.
	 */
	struct swim_cluster *cluster = swim_cluster_new(5, -1);
	for (int i = 0; i < 4; ++i)
		swim_cluster_add_link(cluster, i, i + 1);
	is(swim_cluster_wait_fullmesh(cluster, 10), 0, "sequence");
	swim_cluster_delete(cluster);

	swim_finish_test();
}

static void
swim_test_uuid_update(void)
{
	swim_start_test(4);

	struct swim_cluster *cluster = swim_cluster_new(2, -1);
	swim_cluster_add_link(cluster, 0, 1);
	fail_if(swim_cluster_wait_fullmesh(cluster, 1) != 0);
	struct swim *s = swim_cluster_node(cluster, 0);
	struct tt_uuid new_uuid = uuid_nil;
	new_uuid.time_low = 1000;
	is(swim_cfg(s, NULL, -1, -1, -1, &new_uuid), 0, "UUID update");
	is(swim_cluster_wait_fullmesh(cluster, 1), 0,
	   "old UUID is returned back as a 'ghost' member");
	new_uuid.time_low = 2;
	is(swim_cfg(s, NULL, -1, -1, -1, &new_uuid), -1,
	   "can not update to an existing UUID - swim_cfg fails");
	ok(swim_error_check_match("exists"), "diag says 'exists'");
	swim_cluster_delete(cluster);

	swim_finish_test();
}

static void
swim_test_cfg(void)
{
	swim_start_test(15);

	struct swim *s = swim_new();
	assert(s != NULL);
	is(swim_cfg(s, NULL, -1, -1, -1, NULL), -1, "first cfg failed - no URI");
	ok(swim_error_check_match("mandatory"), "diag says 'mandatory'");
	const char *uri = "127.0.0.1:1";
	is(swim_cfg(s, uri, -1, -1, -1, NULL), -1, "first cfg failed - no UUID");
	ok(swim_error_check_match("mandatory"), "diag says 'mandatory'");
	struct tt_uuid uuid = uuid_nil;
	uuid.time_low = 1;
	is(swim_cfg(s, uri, -1, -1, -1, &uuid), 0, "configured first time");
	is(swim_cfg(s, NULL, -1, -1, -1, NULL), 0, "second time can omit URI, UUID");
	is(swim_cfg(s, NULL, 2, 2, -1, NULL), 0, "hearbeat is dynamic");

	struct swim *s2 = swim_new();
	assert(s2 != NULL);
	const char *bad_uri1 = "127.1.1.1.1.1.1:1";
	const char *bad_uri2 = "google.com:1";
	const char *bad_uri3 = "unix/:/home/gerold103/any/dir";
	struct tt_uuid uuid2 = uuid_nil;
	uuid2.time_low = 2;
	is(swim_cfg(s2, bad_uri1, -1, -1, -1, &uuid2), -1,
	   "can not use invalid URI");
	ok(swim_error_check_match("invalid uri"), "diag says 'invalid uri'");
	is(swim_cfg(s2, bad_uri2, -1, -1, -1, &uuid2), -1,
	   "can not use domain names");
	ok(swim_error_check_match("invalid uri"), "diag says 'invalid uri'");
	is(swim_cfg(s2, bad_uri3, -1, -1, -1, &uuid2), -1,
		    "UNIX sockets are not supported");
	ok(swim_error_check_match("only IP"), "diag says 'only IP'");
	is(swim_cfg(s2, uri, -1, -1, -1, &uuid2), -1,
		    "can not bind to an occupied port");
	ok(swim_error_check_match("bind"), "diag says 'bind'");
	swim_delete(s2);
	swim_delete(s);

	swim_finish_test();
}

static void
swim_test_add_remove(void)
{
	swim_start_test(13);

	struct swim_cluster *cluster = swim_cluster_new(2, -1);
	swim_cluster_add_link(cluster, 0, 1);
	fail_if(swim_cluster_wait_fullmesh(cluster, 1) != 0);
	struct swim *s1 = swim_cluster_node(cluster, 0);
	struct swim *s2 = swim_cluster_node(cluster, 1);
	const struct swim_member *s2_self = swim_self(s2);

	is(swim_add_member(s1, swim_member_uri(s2_self),
			   swim_member_uuid(s2_self)), -1,
	   "can not add an existing member");
	ok(swim_error_check_match("already exists"),
	   "diag says 'already exists'");

	const char *bad_uri = "127.0.0101010101";
	struct tt_uuid uuid = uuid_nil;
	uuid.time_low = 1000;
	is(swim_add_member(s1, bad_uri, &uuid), -1,
	   "can not add a invalid uri");
	ok(swim_error_check_match("invalid uri"), "diag says 'invalid uri'");

	is(swim_remove_member(s2, swim_member_uuid(s2_self)), -1,
	   "can not remove self");
	ok(swim_error_check_match("can not remove self"),
	   "diag says the same");

	isnt(swim_member_by_uuid(s1, swim_member_uuid(s2_self)), NULL,
	     "find by UUID works");
	is(swim_remove_member(s1, swim_member_uuid(s2_self)), 0,
	   "now remove one element");
	is(swim_member_by_uuid(s1, swim_member_uuid(s2_self)), NULL,
	   "and it can not be found anymore");

	is(swim_remove_member(s1, &uuid), 0, "remove of a not existing member");

	is(swim_cluster_is_fullmesh(cluster), false,
	   "after removal the cluster is not in fullmesh");
	is(swim_cluster_wait_fullmesh(cluster, 1), 0,
	   "but it is back in 1 step");

	/*
	 * On each step s1 sends itself to s2. However s2 can be
	 * removed from s1 after the message is scheduled but
	 * before its completion.
	 */
	swim_cluster_block_io(cluster, 0);
	swim_run_for(1);
	/*
	 * Now the message from s1 is in 'fly', round step is not
	 * finished.
	 */
	swim_remove_member(s1, swim_member_uuid(s2_self));
	swim_cluster_unblock_io(cluster, 0);
	is(swim_cluster_wait_fullmesh(cluster, 1), 0,
	   "back in fullmesh after a member removal in the middle of a step");

	swim_cluster_delete(cluster);

	swim_finish_test();
}

static void
swim_test_basic_failure_detection(void)
{
	swim_start_test(7);
	struct swim_cluster *cluster = swim_cluster_new(2, 0.5);

	swim_cluster_add_link(cluster, 0, 1);
	is(swim_cluster_member_status(cluster, 0, 1), MEMBER_ALIVE,
	   "node is added as alive");
	swim_cluster_block_io(cluster, 1);
	is(swim_cluster_wait_member_status(cluster, 0, 1, MEMBER_DEAD, 2.4), -1,
	   "member still is not dead after 2 noacks");
	is(swim_cluster_wait_member_status(cluster, 0, 1, MEMBER_DEAD, 0.1), 0,
	   "but it is dead after one more");

	is(swim_cluster_wait_member_status(cluster, 0, 1,
					   swim_member_status_MAX, 0.9), -1,
	   "after 1 more unack the member still is not deleted");
	is(swim_cluster_wait_member_status(cluster, 0, 1,
					   swim_member_status_MAX, 0.1), 0,
	   "but it is dropped after 1 more");

	/*
	 * After IO unblock pending messages will be processed all
	 * at once. S2 will learn about S1. After one more round
	 * step it should be fullmesh.
	 */
	swim_cluster_unblock_io(cluster, 1);
	is(swim_cluster_wait_fullmesh(cluster, 1), 0, "fullmesh is restored");

	/* A member can be removed during an ACK wait. */
	swim_cluster_block_io(cluster, 1);
	/* Next round after 1 sec + let ping hang for 0.25 sec. */
	swim_run_for(1.25);
	struct swim *s1 = swim_cluster_node(cluster, 0);
	struct swim *s2 = swim_cluster_node(cluster, 1);
	const struct swim_member *s2_self = swim_self(s2);
	swim_remove_member(s1, swim_member_uuid(s2_self));
	swim_cluster_unblock_io(cluster, 1);
	swim_run_for(0.1);
	is(swim_cluster_member_status(cluster, 0, 1), MEMBER_ALIVE,
	   "a member is added back on an ACK");

	swim_cluster_delete(cluster);
	swim_finish_test();
}

static void
swim_test_probe(void)
{
	swim_start_test(2);
	struct swim_cluster *cluster = swim_cluster_new(2, -1);

	struct swim *s1 = swim_cluster_node(cluster, 0);
	struct swim *s2 = swim_cluster_node(cluster, 1);
	const char *s2_uri = swim_member_uri(swim_self(s2));
	is(swim_probe_member(s1, s2_uri), 0, "send probe");
	is(swim_cluster_wait_fullmesh(cluster, 0.1), 0,
	   "receive ACK on probe and get fullmesh")

	swim_cluster_delete(cluster);
	swim_finish_test();
}

static int
main_f(va_list ap)
{
	swim_start_test(7);

	(void) ap;
	swim_test_ev_init();
	swim_test_transport_init();

	swim_test_one_link();
	swim_test_sequence();
	swim_test_uuid_update();
	swim_test_cfg();
	swim_test_add_remove();
	swim_test_basic_failure_detection();
	swim_test_probe();

	swim_test_transport_free();
	swim_test_ev_free();

	test_result = check_plan();
	footer();
	return 0;
}

int
main()
{
	memory_init();
	fiber_init(fiber_c_invoke);
	int fd = open("log.txt", O_TRUNC);
	if (fd != -1)
		close(fd);
	say_logger_init("log.txt", 6, 1, "plain", 0);

	struct fiber *main_fiber = fiber_new("main", main_f);
	fiber_set_joinable(main_fiber, true);
	assert(main_fiber != NULL);
	fiber_wakeup(main_fiber);
	ev_run(loop(), 0);
	fiber_join(main_fiber);

	say_logger_free();
	fiber_free();
	memory_free();

	return test_result;
}