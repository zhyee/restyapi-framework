-- 获取观看记录
-- created by $Joy Zhang$ <1054948153@qq.com>
-- http://192.168.23.149:8091/service/UserPlayHistory/GetByContentIDs?authID=104318907191667130930&contentIDs=C39808305,C39778377,C39778370&devID=000001&appID=115020310221&contentType=1&productIDs=1000000228,1000000218,1000000100,1000000228,1000000432,1000000442
-- @user ZY
-- @date 2017-03-30
-- @file List.lua

local ngx = ngx 
local type = type
local ipairs = ipairs
local tonumber = tonumber
local timestamp = os.time()
local tostring = tostring
local unpack = table.unpack or unpack
local remove = table.remove
local result = {code=0, msg="OK"}
local redis = nil           --redis handler
local redis_is_connected = false
local db = nil				--mysql handler
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
	local must = {"authID", "devID", "appID", "contentType", "contentIDs", "productIDs"}
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
	local contentType = args.contentType
	local productIDs = args.productIDs

	local contentIDsArr = Utils.explode(contentIDs, ",")
	local productIDsArr = Utils.explode(productIDs, ",")
	
	if type(contentIDsArr) == 'table' then
		for i,v in ipairs(contentIDsArr) do
			if Utils.empty(v) then
				remove(contentIDsArr, i)
			end
		end
	end

	local zset_key = Config.PLAY_HISTORY_ZSET_KEY_PREFIX .. authID
	local hash_key = Config.PLAY_HISTORY_HASH_KEY_PREFIX .. authID

	redis = RedisCli:new()
	redis:set_timeout(Config.REDIS_TIMEOUT)
	local ok,err = redis:connect(Config.REDIS_HOST, Config.REDIS_PORT)
	if not ok then
			result.code = 4
			result.msg = "无法连接redis: " .. err
			break
	end
	redis_is_connected = true

	--[[
	-- 修改 播放记录不进入数据库
	-- 判断缓存中是否存在该键值
	local res, err = redis:exists(hash_key)
	if not res then
		result.code = 2
		result.msg = "redis exists查询失败" .. err
		break
	elseif res == 0 then
		--缓存中不存在则从数据库初始化

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
			result.msg = "mysql查询失败"
			break
		end
		
		res, err, errcode, sqlstate = db:query("SELECT * FROM ua_user_play_history WHERE authID = '" .. authID .. "' AND appID = '" .. appID .. "' ORDER BY id DESC", Config.PLAY_HISTORY_ZSET_MAX_LENGTH)
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
					redis:zadd(zset_key, v.createTime, Cjson.encode(v))
				end
			end
		end
		------------- mysql操作结束 ------------
	end
	]]
	
	redis:hset(hash_key, "-1", tostring(timestamp))  -- 占位并记录最新的修改时间
	
	local res, err = {}, nil
	if #contentIDsArr > 0 then
		res, err = redis:hmget(hash_key, unpack(contentIDsArr))
	end
	
	local playLists = {}
	if not res then
		result.code = 1;
		result.msg = "获取缓存数据失败: " .. err
		break
	else

		if type(res) == "table" then
			for _,v in pairs(res) do
				if v ~= ngx.null then
					v = Cjson.decode(v)
					if type(v) == "table" then
						if Utils.in_array(v.productID, productIDsArr) then
							if v.createTime then
								v.time = v.createTime
								v.createTime = nil
							end
							playLists[#playLists + 1] = v
						end
					end
				end
			end
		end
	end

	result.info = playLists
	
	-- 更新redis ttl
	if Config.PLAY_HISTORY_IS_EXPIRE then
		redis:init_pipeline()
		redis:expire(hash_key, Config.PLAY_HISTORY_KEEPALIVE_TIME)
		redis:expire(zset_key, Config.PLAY_HISTORY_KEEPALIVE_TIME)
		redis:commit_pipeline()
	end

until true

if mysql_is_connected then
	db:set_keepalive(Config.MYSQL_KEEPALIVE_TIME, Config.MYSQL_KEEPALIVE_size)
end

if redis_is_connected then
        redis:set_keepalive(Config.REDIS_KEEPALIVE_TIME, Config.REDIS_KEEPALIVE_size)
end

ngx.say(Cjson.encode(result))
