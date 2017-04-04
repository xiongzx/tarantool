fiber = require('fiber')
fio = require('fio')

s = box.schema.space.create('test', {engine='vinyl'})
_ = s:create_index('pk', {run_count_per_level=1})

path = fio.pathjoin(box.cfg.vinyl_dir, tostring(s.id), tostring(s.index.pk.id))

function run_count() return box.info.vinyl().db[s.id..'/'..s.index.pk.id].run_count end
function file_count() return #fio.glob(fio.pathjoin(path, '*')) end
function snapshot() box.snapshot() box.internal.gc.run(box.info.cluster.signature) end

-- Check that run files are deleted by gc.
s:insert{1} snapshot() -- dump
s:insert{2} snapshot() -- dump + compaction
while run_count() > 1 do fiber.sleep(0.01) end -- wait for compaction
file_count()
s:insert{3} snapshot() -- dump
file_count()

-- Check that files left from dropped indexes are deleted by gc.
s:drop() snapshot()
file_count()