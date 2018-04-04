-- 获取订阅数
-- created by $Joy Zhang$ <1054948153@qq.com>
-- http://192.168.23.149:8091/service/UserSubscribe/Count?authID=104300078093352170325&contentType=3&contentIDs=C8000000000000000001427180449323,C800000000000000000142718044938883&devID=000001&appID=115020310221
-- @user ZY
-- @date 2017-04-12
-- @file Del.lua

local ngx = ngx
local type = type
local tonumber = tonumber
local tostring = tostring
local ipairs = ipairs
local unpack = table.unpack or unpack
local remove = table.remove
local timestamp = os.time()
local result = {code=0, msg="OK"}
local redis = nil                       --redis handler
local redis_is_connected = false
local db = nil                          --mysql handler
local mysql_is_connected = false
local ok, Cjson, Config

repeat

        ok, Cjson = pcall(require, "cjson")
        if not ok then
                result.code = 1
                result.msg = "无法加载 cjson.so库"
                break
        end

        local ok, Utils = pcall(require, "Utils")
        if not ok then
                result.code = 1
                result.msg = "无法加载 Utils.lua库"
                break
        end

        ok, Config = pcall(require, "Config")
        if not ok then
                result.code = 2
                result.msg = "无法加载 Config.lua库"
                break
        end
		if type(Config) == "string" then	
			Config = Cjson.decode(Config)
		end			

        local ok, RedisCli = pcall(require, "redis")
        if not ok then
                result.code = 3
                result.msg = "无法加载redis.lua库"
                break
        end

        local args = ngx.req.get_uri_args()
        local must = {"devID", "appID", "contentIDs"}
        local index = Utils.checkMust(args, must);

        if index then
                result.code = 1
                result.msg = "缺少参数 " .. must[index]
                break
        end
		
		local authID = args.authID
		local devID = args.devID
		local appID = args.appID
		local contentIDs = args.contentIDs
	
		--local count_key_prefix = Config.SUBSCRIBE_COUNT_KEY_PREFIX .. appID .. "_"
		local count_key_prefix = Config.SUBSCRIBE_COUNT_KEY_PREFIX

		redis = RedisCli:new()
		redis:set_timeout(Config.REDIS_TIMEOUT)
		local ok,err = redis:connect(Config.REDIS_HOST, Config.REDIS_PORT)
		if not ok then
				result.code = 4
				result.msg = "无法连接redis: " .. err
				break
		end
		redis_is_connected = true


		
		-- 获取指定内容订阅数
		local contentIDArr = Utils.explode(contentIDs, ",")
		
		local count_keys = {}
		
		if type(contentIDArr) == "table" then
			for i,v in ipairs(contentIDArr) do
				count_keys[#count_keys + 1] = count_key_prefix  .. v
			end
		end
		
		local counts = redis:mget(unpack(count_keys))
		
		local countArr = {}
		
		for i,v in ipairs(counts) do
		
			if Utils.empty(v) then
				v = 0
			end
			countArr[#countArr + 1] = {contentId=contentIDArr[i], count=tonumber(v) * 5 + 14724}
		end
		
		result.info = countArr
		
until true

if mysql_is_connected then
	db:set_keepalive(Config.MYSQL_KEEPALIVE_TIME, Config.MYSQL_KEEPALIVE_COUNT)
end

if redis_is_connected then
    redis:set_keepalive(Config.REDIS_KEEPALIVE_TIME, Config.REDIS_KEEPALIVE_COUNT)
end

ngx.say(Cjson.encode(result))