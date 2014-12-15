local disabled = ngx.shared.disabled_nodes
balance_req(math.random(0xFFFFFFF), cfg["clusters"][ngx.var.dest_group], disabled)

local last_run = disabled:get("sentinel_run_at")
if last_run == nil or (ngx.time() - last_run) > cfg["check_interval"] then
    query_http()
end
