-- 删除订阅
-- created by $Joy Zhang$ <zhangy@tv189.com>
-- http://192.168.23.149:8091/service/UserSubscribe/Del?authID=104300078093352170325&contentType=3&contentIDs=C8000000000000000001427180449323,C800000000000000000142718044938883&devID=000001&appID=115020310221
-- @user ZY
-- @date 2017-04-12
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
	

		--local count_key_prefix = Config.SUBSCRIBE_COUNT_KEY_PREFIX .. appID .. "_"
		--local hash_key = Config.SUBSCRIBE_HASH_KEY_PREFIX .. appID .. "_" .. authID	
		--local zset_key = Config.SUBSCRIBE_ZSET_KEY_PREFIX .. appID .. "_" .. authID

		local count_key_prefix = Config.SUBSCRIBE_COUNT_KEY_PREFIX
		local hash_key = Config.SUBSCRIBE_HASH_KEY_PREFIX .. authID	
		local zset_key = Config.SUBSCRIBE_ZSET_KEY_PREFIX .. authID
		
		redis = RedisCli:new()
		redis:set_timeout(Config.REDIS_TIMEOUT)
		local ok,err = redis:connect(Config.REDIS_HOST, Config.REDIS_PORT)
		if not ok then
				result.code = 4
				result.msg = "无法连接redis: " .. err
				break
		end
		redis_is_connected = true
		redis:zadd(Config.SUBSCRIBE_APPIDS_ZSET_KEY, timestamp, appID)
		
		if Utils.empty(contentIDs) then
			--删除所有
			local hash_keys = redis:hkeys(hash_key)
			if type(hash_keys) == 'table' then
				for i,v in ipairs(hash_keys) do
					if v ~= -1 then
						local count_key = count_key_prefix .. v
						redis:decr(count_key)
					end
				end
			end
			
			redis:init_pipeline()
			redis:del(zset_key)
			redis:del(hash_key)
			redis:hset(hash_key, "-1", tostring(timestamp))  -- 占位并记录最新的修改时间
			redis:lpush(Config.SUBSCRIBE_AUTHIDS_LIST_KEY, appID .. '_' .. authID)
			redis:commit_pipeline()
			
			-- 更新redis ttl
			if Config.SUBSCRIBE_IS_EXPIRE then
				redis:init_pipeline()
				redis:expire(hash_key, Config.SUBSCRIBE_KEEPALIVE_TIME)
				redis:expire(zset_key, Config.SUBSCRIBE_KEEPALIVE_TIME)
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
		redis:lpush(Config.SUBSCRIBE_AUTHIDS_LIST_KEY, appID .. '_' .. authID) -- 加入队列异步入数据库
		
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
					local count_key = count_key_prefix  .. contentIDArr[i]
					redis:decr(count_key)
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
		if Config.SUBSCRIBE_IS_EXPIRE then
			redis:init_pipeline()
			redis:expire(hash_key, Config.SUBSCRIBE_KEEPALIVE_TIME)
			redis:expire(zset_key, Config.SUBSCRIBE_KEEPALIVE_TIME)
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