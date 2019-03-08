--
-- Check that replication applier invokes on_replace triggers
--

env = require('test_run')
test_run = env.new()
fiber = require('fiber')

_ = box.schema.space.create('test')
_ = box.space.test:create_index('primary')
box.schema.user.grant('guest', 'replication')

test_run:cmd("create server replica with rpl_master=default, script='replication/replica.lua'")
test_run:cmd("start server replica")
test_run:cmd("switch replica")
session_type = nil
--
-- gh-2642: box.session.type() in replication applier
--
_ = box.space.test:on_replace(function() session_type = box.session.type() end)
box.space.test:insert{1}
--
-- console
--
session_type
test_run:cmd("switch default")
box.space.test:insert{2}
test_run:cmd("switch replica")
fiber = require('fiber')
while box.space.test:count() < 2 do fiber.sleep(0.01) end
--
-- applier
--
session_type
test_run:cmd("switch default")
--
-- cleanup
--
test_run:cmd("stop server replica")
test_run:cmd("cleanup server replica")
test_run:cmd("delete server replica")
test_run:cleanup_cluster()
box.space.test:drop()
box.schema.user.revoke('guest', 'replication')


-- gh-2682 on_replace on slave server with data change

SERVERS = { 'on_replace1', 'on_replace2' }
test_run:create_cluster(SERVERS, "replication", {args="20 50"})
test_run:wait_fullmesh(SERVERS)

test_run:cmd('switch on_replace1')
fiber = require'fiber'
s1 = box.schema.space.create('s1')
_ = s1:create_index('pk')
s2 = box.schema.space.create('s2')
_ = s2:create_index('pk')

test_run:cmd('switch on_replace2')
fiber = require'fiber'
while box.space.s2 == nil do fiber.sleep(0.00001) end
_ = box.space.s1:on_replace(function (old, new) box.space.s2:replace(new) end)

test_run:cmd('switch on_replace1')
box.space.s1:replace({1, 2, 3, 4})
while #(box.space.s2:select()) == 0 do fiber.sleep(0.00001) end

test_run:cmd('switch on_replace2')
box.space.s1:select()
box.space.s2:select()

test_run:cmd('switch on_replace1')
box.space.s1:select()
box.space.s2:select()

_ = test_run:cmd('switch default')
test_run:drop_cluster(SERVERS)
test_run:cleanup_cluster()
