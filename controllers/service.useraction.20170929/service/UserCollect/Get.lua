-- 添加收藏记录
-- created by $Joy Zhang$ <1054948153@qq.com>
-- http://192.168.23.149:8091/service/UserCollect/Get?authID=104300078093352170325&contentID=C39984414&devID=000001&appID=115020310221&productID=1000000228
-- @user ZY
-- @date 2017-04-12
-- @file Add.lua

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

	local must = {"authID", "contentID", "devID", "appID"}

	local index = Utils.checkMust(args, must);

	if index then
		result.code = 1
		result.msg = "缺少参数 " .. must[index]
		break
	end
	
	local authID = args.authID
	local contentID = args.contentID
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
	
	local hash_key = Config.COLLECT_HASH_KEY_PREFIX .. appID .. "_" .. authID
	local zset_key = Config.COLLECT_ZSET_KEY_PREFIX .. appID .. "_" .. authID
	
	local hash_field = contentID
	
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
		
		res, err, errcode, sqlstate = db:query("SELECT * FROM ua_user_collect WHERE authID = '" .. authID .. "' AND appID = '" .. appID .. "' ORDER BY id DESC", Config.COLLECT_ZSET_MAX_LENGTH)
		if not res then
			result.code = 7
			result.msg = "mysql查询失败"
			break
		else
			for _,v in ipairs(res)  do
				if type(v) == "table" then
					redis:hset(hash_key, v.contentID, Cjson.encode(v))
					redis:zadd(zset_key, v.createTime, v.contentID)
				end
			end
		end
		------------- mysql操作结束 ------------
		
	end
	
	redis:hset(hash_key, "-1", tostring(timestamp))  -- 占位并记录最新的修改时间

	-- 查找hash中是否已有该条内容的收藏记录
	local res, err = redis:hget(hash_key, hash_field)
	if res and type(res) == "string" then
		-- local score1, err = redis:zscore(zset_key, res)  -- 判断是否在zset中
		-- local score2, err = redis:zscore(zset_key, contentID)
		-- if (score1 and score1 ~= ngx.null) or (score2 and score2 ~= ngx.null) then
			-- res = Cjson.decode(res)
		-- else
			-- res = {}
		-- end
		res = Cjson.decode(res)
	else
		res = {}
	end
	
	result.info = res
	
	-- 更新redis ttl
	if Config.COLLECT_IS_EXPIRE then
		redis:init_pipeline()
		redis:expire(zset_key, Config.COLLECT_KEEPALIVE_TIME)
		redis:expire(hash_key, Config.COLLECT_KEEPALIVE_TIME)
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
