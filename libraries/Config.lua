-- 读取Config.json配置文件并返回

local fopen = io.open
local getinfo = debug.getinfo
local sub = string.sub

local info = getinfo(1, "S")
local dir = sub(info.source, 2, -11)

local fp, err = fopen(dir .. "Config.json", "r")

if not fp  then
	error("cannot open file Config.json: " .. err)
end

local Config = fp:read("*all")

fp:close()

return Config
