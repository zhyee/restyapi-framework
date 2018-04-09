-- created by $Joy Zhang$ <zhangy@tv189.com>
-- @author ZY
-- @date 2018-04-08
-- @file SubscribeController.lua
-- @ver 0.3

local ngx = ngx
local ngx_null = ngx.null
local ngx_req_get_uri_args = ngx.req.get_uri_args
local require = require
local type = type
local error = error
local ipairs = ipairs
local pairs = pairs
local tostring = tostring
local tonumber = tonumber
local unpack = table.unpack or unpack
local string_find = string.find
local table_concat = table.concat
local table_sort = table.sort
local math_ceil = math.ceil
local APP_ROOT = APP_ROOT
local extends = extends

local BaseController = require(APP_ROOT .. "/controllers/BaseController")

local SubscribeController = {}

extends(SubscribeController, BaseController)

-- 添加订阅
-- http://192.168.23.149:8091/service/UserSubscribe/Add?authID=104300078093352170325&contentID=C39984414&devID=000001&appID=115020310221&ext={%22countryName%22:%22u6b27u7f8e%22,%22siteFolderId%22:11111122,%22seriescount%22:0,%22categoryId%22:%222%22,%22imgM8%22:%22http://pic01.v.vnet.mobi/mcover/2017/5/39984414/131072_7337988005.jpg%22,%22contentType%22:%223%22,%22himgM7%22:%22http://pic01.v.vnet.mobi/mcover/2017/5/39984414/200_7337988004.jpg%22,%22contentId%22:%22C39984414%22,%22himgM8%22:%22http://pic01.v.vnet.mobi/mcover/2017/5/39984414/131072_7337988004.jpg%22,%22productId%22:%221000000228%22,%22title%22:%22u4e00u6761u72d7u7684u4f7fu547d%22,%22ppvStyle%22:0,%22length%22:6000,%22plats%22:32,%22nowseriescount%22:0}

function SubscribeController:Add()
	local timestamp = self.time()
	local helper = self.helper
	local getconf = helper.Conf
	
	local args = ngx_req_get_uri_args()
	local must = {"authID", "contentID", "devID", "appID"}

	local i = helper.CheckMust(args, must);

	if i then
		error(helper.Error(21, "缺少参数 " .. must[i]))
	end
	
	local authID = args.authID
	local contentID = args.contentID
	local index = args.index
	local devID = args.devID
	local appID = args.appID
	local ext = args.ext
	args.createTime = timestamp
	
	local count_key = getconf("SUBSCRIBE_COUNT_KEY_PREFIX") .. contentID
	local hash_key = getconf("SUBSCRIBE_HASH_KEY_PREFIX") .. authID
	local zset_key = getconf("SUBSCRIBE_ZSET_KEY_PREFIX") .. authID
	local hash_field = contentID
	
	local redis = self:redis()
	-- 判断 hash_key是否存在缓存中
	local res, err = redis:exists(hash_key)
	
	if not res then
		error(helper.Error(22, "redis exists fail"))
	end
	
	if res == 0 then
		local db = self:mysql()
		local res, err, errcode, sqlstate = db:query("SELECT * FROM " .. getconf("SUBSCRIBE_TABLE") .. " WHERE authID = '" .. authID .. "' ORDER BY id DESC")
		if not res then
			error(helper.Error(23, "db query fail"))
		end
		
		local hash_field
		for _,v in ipairs(res)  do
			if type(v) == "table" then
				hash_field = v.contentID
				redis:hset(hash_key, hash_field, helper.encode(v))
				redis:zadd(zset_key, v.createTime, v.contentID)
			end
		end
	end
	
	redis:hset(hash_key, "-1", tostring(timestamp))  -- 占位并记录最新的修改时间
	redis:lpush(getconf("SUBSCRIBE_AUTHIDS_LIST_KEY"), appID .. '_' .. authID) -- 加入队列异步入数据库

	local subscribe_value = helper.encode(args)

	local res, err = redis:hget(hash_key, hash_field)
	if res and type(res) == "string" then
		-- 从zset中删除已有内容收藏记录
		redis:zrem(zset_key, res)
	else
		redis:incr(count_key)
	end
	-- 添加新的订阅
	redis:hset(hash_key, hash_field, subscribe_value)
	redis:zadd(zset_key, timestamp, contentID)

	local count = redis:zcount(zset_key, "-inf", "+inf")
	local need_del_count = count - getconf("SUBSCRIBE_ZSET_MAX_LENGTH")
	
	if need_del_count > 0 then
		-- 获取最老的数据
		local res, err = redis:zrange(zset_key, 0, need_del_count - 1)
		if not res then
			error(helper.Error(24, "redis zrange fail"))
		else
			local del_hashkeys = {}
			if type(res) == 'table' then
				for _,v in pairs(res) do
					if not string_find(v, "{", 1, true) then
						del_hashkeys[#del_hashkeys + 1] = v
					else
						v = helper.decode(v)
						if type(v) == "table" then
							del_hashkeys[#del_hashkeys + 1] = v.contentID
						end
					end
				end
			end
			
			if #del_hashkeys > 0 then
				local res, err = redis:hdel(hash_key, unpack(del_hashkeys))
				if not res then
					error(helper.Error(25, "redis hdel fail"))
				end
			end
		end	
	
		redis:zremrangebyrank(zset_key, 0, need_del_count - 1)
	end
	
	-- 更新redis ttl
	if getconf("SUBSCRIBE_IS_EXPIRE") then
		redis:init_pipeline()
		redis:expire(zset_key, getconf("SUBSCRIBE_KEEPALIVE_TIME"))
		redis:expire(hash_key, getconf("SUBSCRIBE_KEEPALIVE_TIME"))
		redis:commit_pipeline()
	end
	
	return nil
end

-- 订阅列表
-- http://192.168.23.149:8091/service/UserSubscribe/List?authID=104300078093352170325&page=1&size=30&categoryID=&devID=000001&appID=115020310221
function SubscribeController:List()
	local timestamp = self.time()
	local helper = self.helper
	local getconf = helper.Conf

	local args = ngx_req_get_uri_args()
	local must = {"authID", "devID", "appID", "page", "size"}
	local i= helper.CheckMust(args, must)

	if i then
		error(helper.Error(31, "缺少参数 " .. must[i]))
	end
	
	local authID = args.authID
	local devID = args.devID
	local appID = args.appID
	local page = tonumber(args.page)
	local size = tonumber(args.size)
	local productIDs = args.productIDs
	
	local offset =  (page - 1) * size

	local hash_key = getconf("SUBSCRIBE_HASH_KEY_PREFIX") .. authID
	local zset_key = getconf("SUBSCRIBE_ZSET_KEY_PREFIX") .. authID

	local productIDArr = {}
	if not helper.Empty(productIDs) then
		productIDArr = helper.Explode(productIDs, ',')
	end
	
	local redis = self:redis()
	
	-- 判断缓存中是否存在该键值
	local res, err = redis:exists(hash_key)
	
	if not res then
		error(helper.Error(32, "redis exists query fail"))
	end
	
	if res == 0 then
		local db = self:mysql()
		
		local res, err, errcode, sqlstate = db:query("SELECT * FROM " .. getconf("SUBSCRIBE_TABLE") .. " WHERE authID = '" .. authID .. "' ORDER BY id DESC")
		if not res then
			error(helper.Error(33, "mysql db query"))
		else
			local hash_field
			for _,v in ipairs(res)  do
				if type(v) == "table" then
					hash_field = v.contentID
					redis:hset(hash_key, hash_field, helper.encode(v))
					redis:zadd(zset_key, v.createTime, v.contentID)
				end
			end
		end
	end
	
	redis:hset(hash_key, "-1", tostring(timestamp))  -- 占位并记录最新的修改时间
	
	local allKeys, err = redis:hkeys(hash_key)
	if not allKeys then
		error(helper.Error(34, "redis hkeys query fail"))
	end
	
	local tmpSetKey = 'ua_ss_tmp_set_key_' .. authID
	local ok, err = redis:sadd(tmpSetKey, unpack(allKeys))
	
	local diffKeys, err = redis:sdiff(tmpSetKey, getconf("DOWNLINE_CONTENTIDS_SET"))
	
	if not diffKeys then
		error(helper.Error(35, "redis sdiff query fail"))
	end
	
	local ok, err  = redis:del(tmpSetKey)
	
	local all, err = redis:hmget(hash_key, unpack(diffKeys))
	if not all then
		error(helper.Error(36, "redis hmget query fail"))
	end
	
	local filterAll = {}
	
	if type(all) == 'table' then
		all['-1'] = nil
		for _,row in pairs(all) do
			row = helper.decode(row)

			if type(row) == "table" then
				if row.createTime then
					row.time = row.createTime
					row.createTime = nil
				end
				
				if type(row.ext) == 'string' and not helper.Empty(row.ext) then
					row.ext = helper.decode(row.ext)
				else
					row.ext = {}
				end

				filterAll[#filterAll + 1] = row
			end

		end
	end
	
	local function comp(item1, item2)
		if tonumber(item1.time) > tonumber(item2.time) then
			return 1
		end
		return false
	end
	
	table_sort(filterAll, comp)
	
	
	local total = #filterAll
	local pagetotal = math_ceil(total / size)

	local playLists = {}
	
	for i = offset + 1, offset + size do
		playLists[#playLists + 1] = filterAll[i]
	end
	
	-- 更新redis ttl
	if getconf("SUBSCRIBE_IS_EXPIRE") then
		redis:init_pipeline()
		redis:expire(hash_key, getconf("SUBSCRIBE_KEEPALIVE_TIME"))
		redis:expire(zset_key, getconf("SUBSCRIBE_KEEPALIVE_TIME"))
		redis:commit_pipeline()
	end
	
	return 	{
		data = playLists,
		page = page,
		pagetotal = pagetotal,
		size = size,
		total = total
	}
end

return SubscribeController
















