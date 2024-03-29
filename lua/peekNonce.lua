--[[
Input:
KEYS[N] nonce keys
ARGV[1] number of keys
Output:
-1 ERROR (none of keys worked)
N OK (chosen key index)
]]
local keysSize = tonumber(ARGV[1])
local expireInSeconds = tonumber(ARGV[2])
local rcall = redis.call

local i = 1
repeat
    local nextKey = KEYS[i]
    if rcall("EXISTS", nextKey) ==  0 then
        return i
    end
    i = i + 1
until( i > keysSize)
return -1
