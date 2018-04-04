-- 获取观看记录
-- created by $Joy Zhang$ <1054948153@qq.com>
-- http://192.168.23.149:8091/service/UserPlayHistory/List?authID=104318907191667130930&devID=000001&appID=111010310225&contentType=1&page=1&size=20&productIDs=1000000218,1000000100,1000000228,1000000432,1000000442
-- @user ZY
-- @date 2017-03-30
-- @file List.lua

local ngx = ngx 
local type = type
local ipairs = ipairs
local tonumber = tonumber
local unpack = table.unpack or unpack
local ceil = math.ceil
local sort = table.sort
local timestamp = os.time()
local tostring = tostring
local find = string.find
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
	local must = {"authID", "devID", "appID", "contentType", "productIDs", "page", "size"}
	local index = Utils.checkMust(args, must);

	if index then
			result.code = 1
			result.msg = "缺少参数 " .. must[index]
			break
	end

	local authID = args.authID
	local devID = args.devID
	local appID = args.appID
	local contentType = args.contentType
	local productIDs = args.productIDs
	local page = tonumber(args.page)
	local size = tonumber(args.size)
	
	local offset = (page - 1) * size
	local productIDsArr = Utils.explode(productIDs, ",")

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
	
	--local res, err = redis:zrevrange(zset_key, offset, offset + size - 1)
	
	local allKeys, err = redis:hkeys(hash_key)
	if not allKeys then
		result.code = 4
		result.msg = "hkeys 命令执行失败"
		break
	end
	
	local tmpSetKey = 'ua_pl_tmp_set_key_' .. authID
	local ok, err = redis:sadd(tmpSetKey, unpack(allKeys))
	
	local diffKeys, err = redis:sdiff(tmpSetKey, Config.DOWNLINE_CONTENTIDS_SET)
	
	if not diffKeys then
		result.code = 7
		result.msg = "sdiff 命令执行失败"
		break
	end
	
	local ok, err  = redis:del(tmpSetKey)
	
	local filterAll = {}
	
	if #diffKeys > 0 then
		local res, err = redis:hmget(hash_key, unpack(diffKeys))
		if not res then
			result.code = 1;
			result.msg = "hmget失败: " .. err
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
								
								if type(v.ext) == 'string' and not Utils.empty(v.ext) then
									v.ext = Cjson.decode(v.ext)
								else
									v.ext = {}
								end
								
								filterAll[#filterAll + 1] = v
							end
						end
					end
				end
			end
		end		
	end
	
	local function comp(item1, item2)
		if tonumber(item1.time) > tonumber(item2.time) then
			return 1
		end
		return false
	end
	
	sort(filterAll, comp)
	
	local total = #filterAll
	local pagetotal = ceil(total / size)

	local playLists = {}
	
	for i = offset + 1, offset + size do
		playLists[#playLists + 1] = filterAll[i]
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
