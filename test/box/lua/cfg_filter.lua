local box_cfg_module = {}

local _hide = {
    pid_file=1, log=1, listen=1, vinyl_dir=1,
    memtx_dir=1, wal_dir=1,
    memtx_max_tuple_size=1, memtx_min_tuple_size=1
}

function box_cfg_module.cfg_filter(data)
    if type(data)~='table' then return data end
    local keys,k,_ = {}
    for k in pairs(data) do
        table.insert(keys, k)
    end
    table.sort(keys)
    local result = {}
    for _,k in pairs(keys) do
        table.insert(result, {k, _hide[k] and '<hidden>' or box_cfg_module.cfg_filter(data[k])})
    end
    return result
end

return box_cfg_module
