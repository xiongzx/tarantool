--
-- gh-1233: JOIN/SUBSCRIBE must fail if master has wal_mode = "none"
--

env = require('test_run')
test_run = env.new()
test_run:cmd('switch default')
fiber = require('fiber')
box.schema.user.grant('guest', 'replication')
test_run:cmd("create server wal_off with rpl_master=default, script='replication/wal_off.lua'")
test_run:cmd("start server wal_off")
test_run:cmd('switch default')
wal_off_uri = test_run:eval('wal_off', 'return box.cfg.listen')[1]
wal_off_uri ~= nil
wal_off_id = test_run:eval('wal_off', 'return box.info.id')[1]

box.cfg { replication = wal_off_uri }
check = "Replication does not support wal_mode = 'none'"
while box.info.replication[wal_off_id].upstream.message ~= check do fiber.sleep(0) end
box.info.replication[wal_off_id].upstream ~= nil
box.info.replication[wal_off_id].downstream ~= nil
box.info.replication[wal_off_id].upstream.status
box.info.replication[wal_off_id].upstream.message
box.cfg { replication = "" }

test_run:cmd('switch wal_off')
box.schema.user.revoke('guest', 'replication')
test_run:cmd('switch default')

replication_sync_timeout = box.cfg.replication_sync_timeout
box.cfg { replication_sync_timeout = 0.01 }
box.cfg { replication = wal_off_uri }
box.cfg { replication_sync_timeout = replication_sync_timeout }
check = "Read access to universe"
while string.find(box.info.replication[wal_off_id].upstream.message, check) == nil do fiber.sleep(0.01) end
box.cfg { replication = "" }

test_run:cmd("stop server wal_off")
test_run:cmd("cleanup server wal_off")
test_run:cmd("delete server wal_off")
test_run:cleanup_cluster()

box.schema.user.revoke('guest', 'replication')
