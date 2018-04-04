-- 获取收藏记录
-- created by $Joy Zhang$ <1054948153@qq.com>
-- http://192.168.23.149:8091/service/UserCollect/List?authID=104300078093352170325&contentType=3&page=1&size=30&categoryID=&devID=000001&appID=115020310221&productIDs=1000000456,1000000085,1000000064,1000000198,1000000218,1000000207,1000000228,1000000432,1000000442
-- @user ZY
-- @date 2017-04-12
-- @file List.lua

local ngx = ngx 
local type = type
local pairs = pairs
local tonumber = tonumber
local tostring = tostring
local sort = table.sort
local ceil = math.ceil
local timestamp = os.time()
local result = {code=0, msg="OK"}
local redis = nil   --redis handler
local redis_is_connected = false
local db = nil	    --mysql handler
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
        local must = {"authID", "devID", "appID", "contentType", "page", "size"}
        local index = Utils.checkMust(args, must);

        if index then
                result.code = 1
                result.msg = "缺少参数 " .. must[index]
                break
        end

	local authID = args.authID
	local devID = args.devID
	local appID = args.appID
	local contentType = tonumber(args.contentType)
	local page = tonumber(args.page)
	local size = tonumber(args.size)
	local productIDs = args.productIDs
	
	local offset =  (page - 1) * size
	
	local hash_key = Config.COLLECT_HASH_KEY_PREFIX .. appID .. "_" .. authID
	local zset_key = Config.COLLECT_ZSET_KEY_PREFIX .. appID .. "_" .. authID

	local productIDArr = {}
	if not Utils.empty(productIDs) then
		productIDArr = Utils.explode(productIDs, ',')
	end

	redis = RedisCli:new()
	redis:set_timeout(Config.REDIS_TIMEOUT)
	local ok,err = redis:connect(Config.REDIS_HOST, Config.REDIS_PORT)
	if not ok then
			result.code = 4
			result.msg = "无法连接redis: " .. err
			break
	end
	redis_is_connected = true
	

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

	
	--local all, err = redis:zrevrangebyscore(zset_key, timestamp, 0)
	local all, err = redis:hgetall(hash_key);
	
	if not all then
		result.code = 3
		result.msg = "zrevrangebyscore 命令执行失败"
		break
	end
	
	local filterAll = {}
	
	if type(all) == 'table' then
		all['-1'] = nil
		for _,row in pairs(all) do
			ok, row = pcall(Cjson.decode, row)
			if ok then
				if type(row) == "table" then
					if row.createTime then
						row.time = row.createTime
						row.createTime = nil
					end
					
					if type(row.ext) == 'string' and not Utils.empty(row.ext) then
						row.ext = Cjson.decode(row.ext)
					else
						row.ext = {}
					end

					if not Utils.empty(row.contentType) and contentType == tonumber(row.contentType) then
						filterAll[#filterAll + 1] = row
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

	result.info = {
		data = playLists,
		page = page,
		pagetotal = pagetotal,
		size = size,
		total = total
	}
	
	-- 更新redis ttl
	if Config.COLLECT_IS_EXPIRE then
		redis:init_pipeline()
		redis:expire(hash_key, Config.COLLECT_KEEPALIVE_TIME)
		redis:expire(zset_key, Config.COLLECT_KEEPALIVE_TIME)
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
