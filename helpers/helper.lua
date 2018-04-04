local ngx = ngx
local ngx_log = ngx.log
local null = ngx.null
local strlen = string.len
local strfind = string.find
local strsub = string.sub
local gmatch = string.gmatch
local time = os.time
local gmatch = string.gmatch
local error = error
local setmetatable = setmetatable

local config = require(APP_ROOT .. "/config/config")

local helper = {}

-- 让self “继承” base
function helper.Extends(self, base)
	setmetatable(self, {__index=base})
end

-- 获取配置项
function helper.Conf(item, default)
	if config[item] ~= nil then
		return config[item]
	end
	return default
end

-- 记录 debug 信息
function helper.Debug(...)
	if helper.Conf("debug") then
		ngx_log(ngx.CRIT, "[## DEBUG_INFO ##]", ...)
	end
end

-- 检查必传参数
function helper.CheckMust(params, must)
	for i=1, #must do
		if not params[must[i]] or params[must[i]] == ""  then
			return i;
		end
	end
	return false
end

-- 判断值是否为空
-- 包括 空数组,0,空字符串,nil,false,ngx.null 等值
function helper.Empty(param)
	if type(param) == "table" and not next(param)  then
		return true
	end
	if type(param) == "userdata" and param == null then
		return true
	end
	if not param or param == 0 or param == "" then
		return true
	end
	return false	
end

-- 指定字符切割字符串, delimiter为单个字符
function helper.Explode(str, delimiter)
	local res = {}

	if strfind(str, delimiter) ~= 1 then
		str = delimiter .. str
	end
	for match in gmatch(str, delimiter .. "[^" .. delimiter  .. "]*") do
		res[#res + 1] = strsub(match, 2, -1)
	end
	return res
end

-- 检查某值是否存在于数组中
function helper.InArray(val, arr)
	if type(arr) ~= "table" then
		return false
	end
	
	for k,v in pairs(arr) do
		if val == v then
			return k
		end
	end
	return false
end

-- 日期字符串转换为时间戳
function helper.StrToTime(datetime)
	if type(datetime) ~= "string" and type(datetime) ~= "number" then
		error("日期格式无法转换为时间戳")
	end
	local dts = {}
	for dt in gmatch(datetime, "%d+") do
		dts[#dts+1] = dt
	end
	local params = {}
	params.year = dts[1] or 0
	params.month = dts[2] or 0
	params.day = dts[3] or 0
	params.hour = dts[4] or 0
	params.min = dts[5] or 0
	params.sec = dts[6] or 0
	return time(params)
end

-- 生成返回值json table
function helper.MakeJson(code, msg, info)
	local json = {code=code, msg=msg}
	if info ~= nil then
		json.info = info
	end
	return json
end

-- 构造Error json
function helper.Error(code, msg)
	return helper.MakeJson(code, msg)
end

-- 输出信息并结束请求
function helper.Exit(code, msg, info)
	ngx.print(helper.encode(helper.MakeJson(code, msg, info)))
	ngx.exit(ngx.HTTP_OK)
end

return helper
