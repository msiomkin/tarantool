#!/usr/bin/env tarantool

-- get instance name from filename (before_replace1.lua => before_replace1)
local INSTANCE_ID = string.match(arg[0], "%d")
local USER = 'cluster'
local PASSWORD = 'somepassword'
local SOCKET_DIR = require('fio').cwd()
local TIMEOUT = tonumber(arg[1])
local CON_TIMEOUT = arg[2] and tonumber(arg[2]) or 30.0

local function instance_uri(instance_id)
    --return 'localhost:'..(3310 + instance_id)
    return SOCKET_DIR..'/before_replace'..instance_id..'.sock';
end

-- start console first
require('console').listen(os.getenv('ADMIN'))

box.cfg({
    listen = instance_uri(INSTANCE_ID);
--    log_level = 7;
    replication = {
        USER..':'..PASSWORD..'@'..instance_uri(1);
        USER..':'..PASSWORD..'@'..instance_uri(2);
        USER..':'..PASSWORD..'@'..instance_uri(3);
    };
    replication_timeout = TIMEOUT;
    replication_connect_timeout = CON_TIMEOUT;
})

box.once("bootstrap", function()
    local test_run = require('test_run').new()
    box.schema.user.create(USER, { password = PASSWORD })
    box.schema.user.grant(USER, 'replication')
    box.schema.space.create('test', {engine = test_run:get_cfg('engine')})
    box.space.test:create_index('primary')
end)
