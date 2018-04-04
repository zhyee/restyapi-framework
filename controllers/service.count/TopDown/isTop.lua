-- 判断是否已经点赞接口
-- http://192.168.23.149:8092/TopDown/isTop?pid=C39808305&userId=104300078093352170325

local ngx = ngx
local type = type

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
local userId = args.userId

repeat
	if type(Config) ~= 'table' then
		result.code = 1
		result.msg = '加载配置文件失败'
		break	
	end
	
	local must = {'pid', 'userId'}
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

	local topdown_zset_key = Config.TOPDOWN_ZSET_KEY_PREFIX .. userId
	local res, err = redis:zscore(topdown_zset_key, pid)

	if not res then
		result.code = 4
		result.msg = 'redis zsocre 失败'
		break
	else
		if not Utils.empty(res) then
			result.info = 1
		else
			result.info = 0
		end
	end

until true

if redis_is_connected then
	redis:set_keepalive(Config.REDIS_KEEPALIVE_TIME, Config.REDIS_KEEPALIVE_COUNT)
end

ngx.say(Cjson.encode(result))