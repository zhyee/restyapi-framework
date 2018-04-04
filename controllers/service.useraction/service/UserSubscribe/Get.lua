-- 判断是否已经订阅
-- created by $Joy Zhang$ <zhangy@tv189.com>
-- http://192.168.23.149:8091/service/UserSubscribe/Get?authID=104300078093352170325&contentIDs=C8000000000000000001427180449323,C8000000000000000001427180449444&devID=000001&appID=115020310221
-- @user ZY
-- @date 2017-04-12
-- @file Get.lua

local ngx = ngx
local type = type
local ipairs = ipairs
local tostring = tostring
local unpack = table.unpack or unpack
local timestamp = os.time()
local result = {code=0, msg="OK"}
local redis = nil			--redis handler
local redis_is_connected = false
local db = nil				--mysql handler
local mysql_is_connected = false
local ok, Cjson, Config

repeat

	ok, Cjson = pcall(require, "cjson")
	if not ok then
		result.code = 1
		result.msg = "无法加载 cjson库"
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
		result.msg = "无法加载redis库"
		break
	end

	local args = ngx.req.get_uri_args()

	local must = {"authID", "contentIDs", "devID", "appID"}

	local index = Utils.checkMust(args, must);

	if index then
		result.code = 1
		result.msg = "缺少参数 " .. must[index]
		break
	end
	
	local authID = args.authID
	local contentIDs = args.contentIDs
	local devID = args.devID
	local appID = args.appID
	
	redis = RedisCli:new()
	redis:set_timeout(Config.REDIS_TIMEOUT)
	local ok,err = redis:connect(Config.REDIS_HOST, Config.REDIS_PORT)
	if not ok then
		result.code = 4
		result.msg = "无法连接redis: " .. err
		break
	end
	redis_is_connected = true

	
	--local hash_key = Config.SUBSCRIBE_HASH_KEY_PREFIX .. appID .. "_" .. authID
	--local zset_key = Config.SUBSCRIBE_ZSET_KEY_PREFIX .. appID .. "_" .. authID

	local hash_key = Config.SUBSCRIBE_HASH_KEY_PREFIX .. authID
	local zset_key = Config.SUBSCRIBE_ZSET_KEY_PREFIX .. authID
	
	local hash_fields = Utils.explode(contentIDs, ",")
	
	-- 判断 hash_key是否存在缓存中
	local res, err = redis:exists(hash_key)
	
	if not res then
		result.code = 5
		result.msg = "redis exists查询失败"
		break
	elseif res == 0 then
		-- 不存在键值，从数据库中提取数据	
		
		------------- mysql操作开始 ------------

		local ok, mysql = pcall(require, "mysql")
		if not ok then
			result.code = 4
			result.msg = "无法加载 mysql.lua库"
			break
		end
		db, err = mysql:new()
		db:set_timeout(Config.MYSQL_TIMEOUT)
		local ok, err, errcode, sqlstate = db:connect({
			host = Config.MYSQL_HOST,
			port = Config.MYSQL_PORT,
			database = Config.MYSQL_DBNAME,
			user = Config.MYSQL_USER,
			password = Config.MYSQL_PASS,
			max_packet_size = Config.MYSQL_MAX_PACKET_SIZE
		})
		if not ok then
			result.code = 5
			result.msg = "无法连接mysql: " .. err
			break
		end
		mysql_is_connected = true

		local res, err, errcode, sqlstate = db:query("set names utf8")
		if not res then
			result.code = 6
			result.msg = "mysql set names 失败"
			break
		end
		
		res, err, errcode, sqlstate = db:query("SELECT * FROM " .. Config.SUBSCRIBE_TABLE .. " WHERE authID = '" .. authID .. "' ORDER BY id DESC")
		if not res then
			result.code = 7
			result.msg = "mysql查询失败"
			break
		else
			local hash_field = nil
			for _,v in ipairs(res)  do
				if type(v) == "table" then
					hash_field = v.contentID
					redis:hset(hash_key, hash_field, Cjson.encode(v))
					redis:zadd(zset_key, v.createTime, v.contentID)
				end
			end
		end
		------------- mysql操作结束 ------------
		
	end
	
	redis:hset(hash_key, "-1", tostring(timestamp))  -- 占位并记录最新的修改时间

	-- 查找hash中是否已有该条内容的收藏记录
	local res, err = redis:hmget(hash_key, unpack(hash_fields))
	local info = {}
	if res and type(res) == "table" then
		for i,v in ipairs(res) do
			if not Utils.empty(v) then
				info[#info + 1] = {contentId=hash_fields[i], state=1}
			else
				info[#info + 1] = {contentId=hash_fields[i], state=0}
			end
		end
	else
		-- error
		result.code = 10
		result.msg = 'redis hmget error'
	end
	
	result.info = info
	
	-- 更新redis ttl
	if Config.SUBSCRIBE_IS_EXPIRE then
		redis:init_pipeline()
		redis:expire(zset_key, Config.SUBSCRIBE_KEEPALIVE_TIME)
		redis:expire(hash_key, Config.SUBSCRIBE_KEEPALIVE_TIME)
		redis:commit_pipeline()
	end
	
until true

if mysql_is_connected then
	db:set_keepalive(Config.MYSQL_KEEPALIVE_TIME, Config.MYSQL_KEEPALIVE_COUNT)
end

if redis_is_connected then
	redis:set_keepalive(Config.REDIS_KEEPALIVE_TIME, Config.REDIS_KEEPALIVE_COUNT)
end

ngx.say(Cjson.encode(result))
