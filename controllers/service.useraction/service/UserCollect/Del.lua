-- 删除收藏
-- created by $Joy Zhang$ <1054948153@qq.com>
-- http://192.168.23.149:8091/service/UserCollect/Del?authID=104300078093352170325&contentType=3&contentIDs=C39984414,C39984469&devID=000001&appID=115020310221&productIDs=1000000218,1000000100,1000000228,1000000432,1000000442
-- @user ZY
-- @date 2017-09-29
-- @file Del.lua

local ngx = ngx
local type = type
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
        local must = {"authID", "devID", "appID"}
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
	
		local hash_key = Config.COLLECT_HASH_KEY_PREFIX .. appID .. "_" .. authID	
		local zset_key = Config.COLLECT_ZSET_KEY_PREFIX .. appID .. "_" .. authID

		redis = RedisCli:new()
		redis:set_timeout(Config.REDIS_TIMEOUT)
		local ok,err = redis:connect(Config.REDIS_HOST, Config.REDIS_PORT)
		if not ok then
				result.code = 4
				result.msg = "无法连接redis: " .. err
				break
		end
		redis_is_connected = true
		redis:zadd(Config.COLLECT_APPIDS_ZSET_KEY, timestamp, appID) 
		redis:lpush(Config.COLLECT_AUTHIDS_LIST_KEY, appID .. '_' .. authID)  --加入队列异步入数据库
		
		--if contentIDs == "ALL" then
		if Utils.empty(contentIDs) then
			--删除所有
			redis:init_pipeline()
			redis:del(zset_key)
			redis:del(hash_key)
			redis:commit_pipeline()
			
			redis:hset(hash_key, "-1", tostring(timestamp))  -- 占位并记录最新的修改时间
			-- 更新redis ttl
			if Config.COLLECT_IS_EXPIRE then
				redis:init_pipeline()
				redis:expire(hash_key, Config.COLLECT_KEEPALIVE_TIME)
				redis:expire(zset_key, Config.COLLECT_KEEPALIVE_TIME)
				redis:commit_pipeline()
			end
			break
		end
		
		local res, err = redis:exists(hash_key)
		
		if not res then
			result.code = 5
			result.msg = "redis exists查询失败"
			break
		elseif res == 0 then
			--  从数据库拉取数据	
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
		
		-- 删除指定内容
		local contentIDArr = Utils.explode(contentIDs, ",")
		
		local hash_fields = {}
		if type(contentIDArr) == "table" then
			for i = 1, #contentIDArr do
				hash_fields[#hash_fields + 1] = contentIDArr[i]
			end
		end
		local res, err = redis:hmget(hash_key, unpack(hash_fields))
		local hashdels = {}
		local zsetdels = {}
		if type(res) == "table" then
			for i, v in ipairs(res) do
				if not Utils.empty(v) then
					hashdels[#hashdels + 1] = hash_fields[i]
					zsetdels[#zsetdels + 1] = v
				end
			end
		end
		
		if #hashdels > 0 then 
			res, err = redis:hdel(hash_key, unpack(hash_fields))
			if not res then
				result.code = 8
				result.msg = "redis hdel 失败"
				break
			end
		end
		
		if #zsetdels > 0 then
			res, err = redis:zrem(zset_key, unpack(zsetdels))
			if not res then
				result.code = 9
				result.msg = "redis zrem 失败"
				break
			end
		end
		
		if #hashdels > 0 then 
			res, err = redis:zrem(zset_key, unpack(hash_fields))
			if not res then
				result.code = 9
				result.msg = "redis zrem 失败"
				break
			end
		end
		
		-- 更新redis ttl
		if Config.COLLECT_IS_EXPIRE then
			redis:init_pipeline()
			redis:expire(hash_key, Config.COLLECT_KEEPALIVE_TIME)
			redis:expire(zset_key, Config.COLLECT_KEEPALIVE_TIME)
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