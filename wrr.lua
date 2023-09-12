local meta = KEYS[1]
local servers = KEYS[2]

local serverNames = redis.call("ZRANGE", servers, 0, -1)
local serverLen = #serverNames

if serverLen == 0 then
    return ""
end

local total = 0
local bestMember = serverNames[1]

for i = 1, serverLen do
    local member = serverNames[i]
    local weight = redis.call("HGET", meta, member .. "_weight")
    local currentWeight = redis.call("ZINCRBY", servers, weight, member)
    total = total + weight
    local bestCurrentWeight = redis.call("ZSCORE", servers, bestMember)
    if tonumber(currentWeight) > tonumber(bestCurrentWeight) then
        bestMember = member
    end
end

redis.call("ZINCRBY", servers, -total, bestMember)
local addr = redis.call("HGET", meta, bestMember .. "_addr")

return addr
