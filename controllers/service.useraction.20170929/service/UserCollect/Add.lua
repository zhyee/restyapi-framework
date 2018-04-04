-- 添加收藏
-- created by $Joy Zhang$ <zhangy@tv189.com>

--http://192.168.23.149:8091/service/UserCollect/Add?authID=104300078093352170325&contentID=C39984414&index=1&contentType=3&categoryID=&devID=000001&appID=115020310221&productID=1000000228&ext={%22countryName%22:%22u6b27u7f8e%22,%22siteFolderId%22:11111122,%22seriescount%22:0,%22categoryId%22:%222%22,%22imgM8%22:%22http://pic01.v.vnet.mobi/mcover/2017/5/39984414/131072_7337988005.jpg%22,%22contentType%22:%223%22,%22himgM7%22:%22http://pic01.v.vnet.mobi/mcover/2017/5/39984414/200_7337988004.jpg%22,%22contentId%22:%22C39984414%22,%22himgM8%22:%22http://pic01.v.vnet.mobi/mcover/2017/5/39984414/131072_7337988004.jpg%22,%22productId%22:%221000000228%22,%22title%22:%22\u4e00\u6761\u72d7\u7684\u4f7f\u547d%22,%22ppvStyle%22:0,%22length%22:6000,%22plats%22:32,%22nowseriescount%22:0}


-- @user ZY
-- @date 2017-09-13
-- @file Add.lua
-- @ver 0.2

local ngx = ngx
local type = type
local ipairs = ipairs
local tostring = tostring
local unpack = table.unpack or unpack
local find = string.find
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

	local must = {"authID", "contentID", "contentType", "devID", "appID"}

	local index = Utils.checkMust(args, must);

	if index then
		result.code = 1
		result.msg = "缺少参数 " .. must[index]
		break
	end
	
	local authID = args.authID
	local contentID = args.contentID
	local contentType = args.contentType
	local index = args.index
	local devID = args.devID
	local appID = args.appID
	local ext = args.ext
	args.createTime = timestamp
	
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

	local collect_value = Cjson.encode(args)

	local res, err = redis:hget(hash_key, hash_field)
	if res and type(res) == "string" then
		-- 已收藏该节目
		result.code = 141
		result.msg = '该节目已收藏'
		break
	end
	
	redis:hset(hash_key, "-1", tostring(timestamp))  -- 占位并记录最新的修改时间
	redis:lpush(Config.COLLECT_AUTHIDS_LIST_KEY, appID .. '_' .. authID) -- 加入队列异步入数据库
	
	-- 添加新的收藏记录
	redis:hset(hash_key, hash_field, collect_value)
	redis:zadd(zset_key, timestamp, contentID)

	local count = redis:zcount(zset_key, "-inf", "+inf")
	local need_del_count = count - Config.COLLECT_ZSET_MAX_LENGTH
	if need_del_count > 0 then
		-- 获取最老的数据
		local res, err = redis:zrange(zset_key, 0, need_del_count - 1)
		if not res then
			result.code = 8
			result.msg = "redis zrange失败"
			break
		
		else
			local del_hashkeys = {}
			if type(res) == 'table' then
				for _,v in pairs(res) do
					if not find(v, "{", 1, true) then
						del_hashkeys[#del_hashkeys + 1] = v
					else
						v = Cjson.decode(v)
						if type(v) == "table" then
							del_hashkeys[#del_hashkeys + 1] = v.contentID
						end
					end
				end
			end
			
			if #del_hashkeys > 0 then
				local res, err = redis:hdel(hash_key, unpack(del_hashkeys))
				if not res then
					result.code = 9
					result.msg = "redis hdel 失败"
					break
				end
			end
		end	
	
		redis:zremrangebyrank(zset_key, 0, need_del_count - 1)
	end
	
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
