yaml = require "lyaml"
local file = "/etc/nginx/redro.yaml"
local f, err = io.open(file, "r")
if not f then return nil, err end
cfg = yaml.load(f:read("*all"))
cluster_total = {}
cluster_keys = {}
checklist = {}
nodenames = {}
for cluster, nodes in pairs(cfg["clusters"]) do
    cluster_total[cluster] = 0
    cluster_keys[cluster] = 0
    for address, attribs in pairs(nodes) do
        cluster_total[cluster] = cluster_total[cluster] + attribs["weight"]
        cluster_keys[cluster] = cluster_keys[cluster] + 1
        table.insert(checklist, { "/query_one", { vars = {qhost = address, quri = cfg["check_uri"]}}})
        table.insert(nodenames, address)
    end
    ngx.log(ngx.INFO, ("cluster %s: total weight=%s"):format( cluster, cluster_total[cluster] ))
end

function md5_last48bits(data)
    local binhash = ngx.md5_bin(data)
    local hash = 0
    for i=11, 16 do hash = 256 * hash + binhash:byte(i) end
    return hash
end

function node_vector(seed, nodes)
    local result = {}
    local used = {}
    local len = 0
    local tw = 0
    for address, attribs in pairs(nodes) do
        tw = tw + attribs["weight"]
        len = len + 1
    end

    local cs = seed
    for i=1, len do
        local rem = math.fmod(cs, tw)
        cs = math.floor(cs / tw)
        for address, attribs in pairs(nodes) do
            if used[address] == nil then
                if rem < attribs["weight"] then
                    table.insert(result, address)
                    used[address] = true
                    break
                end
                rem = rem - attribs["weight"]
            end
        end

        tw = tw - nodes[result[#result]]["weight"]
    end

    return result
end

function balance_req(seed, nodes, disabled_nodes)
    local nv = node_vector(seed, nodes)
    for k, v in pairs(nv) do
        if disabled_nodes:get(v) == null then
            if ngx.var.cr_classic == "0" then
                ngx.status = ngx.HTTP_MOVED_TEMPORARILY
                ngx.header["Location"] = ("%s://%s%s"):format(ngx.var.scheme, v, ngx.var.request_uri)
            else
                ngx.status = ngx.HTTP_OK
                ngx.print(v)
            end
            ngx.eof()
        end
    end
end

function query_http()
    local resps = { ngx.location.capture_multi(checklist) }
    local disabled = ngx.shared.disabled_nodes
    disabled:set("sentinel_run_at", ngx.time())
    for i, resp in ipairs(resps) do
        if resp.status >= 500 and resp.status <= 599 then
            disabled:add(nodenames[i],1)
            ngx.log(ngx.WARN, "node is down: ", nodenames[i], " response=", tostring(resp.status))
        else
            disabled:delete(nodenames[i])
            ngx.log(ngx.DEBUG, "node is alive: ", nodenames[i])
        end
    end
end

