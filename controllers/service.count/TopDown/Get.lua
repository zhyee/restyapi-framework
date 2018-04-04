-- 获取点赞/顶踩数

local ngx = ngx
local type = type
local tonumber = tonumber

local Cjson = require "cjson"
local Utils = require "Utils"

local result = {code='0', msg='OK'}

local Config = require "Config"

if type(Config) == 'string' then
	Config = Cjson.decode(Config)
end

local redisCli = require "redis"

local redis = redisCli:new()
local redis_is_connected = false

local args = ngx.req.get_uri_args()
local pid = args.pid

repeat
	if type(Config) ~= 'table' then
		result.code = 1
		result.msg = '加载配置文件失败'
		break	
	end
	
	local must = {'pid'}
	local index = Utils.checkMust(args, must)

	if index then
		result.code = 2
		result.msg = '参数' .. must[index] .. '不能为空'
		break
	end
	
	redis:set_timeout(Config.REDIS_TIMEOUT)
	local ok, err = redis:connect(Config.REDIS_HOST, Config.REDIS_PORT)
	
	if not ok then
		result.code = 3
		result.msg = '无法连接redis服务器：' .. err
		break
	end
	redis_is_connected = true
	
	local pidArr = Utils.explode(pid, ',')

	local info = {}
	
	if type(pidArr) == 'table' then
		for i = 1, #pidArr do
			local topcount_key, downcount_key = Config.TOPDOWN_TOP_KEY_PREFIX .. pidArr[i], Config.TOPDOWN_DOWN_KEY_PREFIX .. pidArr[i]
			local res, err = redis:mget(topcount_key, downcount_key)
			local topcount,downcount = tonumber(res[1]) or 0, tonumber(res[2]) or 0
			info[pidArr[i]] = {topcount = topcount, downcount = downcount}
		end
	end
	
	result.info = info

until true

if redis_is_connected then
	redis:set_keepalive(Config.REDIS_KEEPALIVE_TIME, Config.REDIS_KEEPALIVE_COUNT)
end

ngx.say(Cjson.encode(result))
