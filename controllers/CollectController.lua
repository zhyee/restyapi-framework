-- created by $Joy Zhang$ <zhangy@tv189.com>
-- @author ZY
-- @date 2018-04-08
-- @file CollectController.lua
-- @ver 0.3

local ngx = ngx
local null = ngx.null
local ngx_req_get_uri_args = ngx.req.get_uri_args
local require
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

local CollectController = {}

extends(CollectController, BaseController)


-- 添加收藏
--http://192.168.23.149:8091/service/UserCollect/Add?authID=104300078093352170325&contentID=C39984414&index=1&contentType=3&categoryID=&devID=000001&appID=115020310221&productID=1000000228&ext={%22countryName%22:%22u6b27u7f8e%22,%22siteFolderId%22:11111122,%22seriescount%22:0,%22categoryId%22:%222%22,%22imgM8%22:%22http://pic01.v.vnet.mobi/mcover/2017/5/39984414/131072_7337988005.jpg%22,%22contentType%22:%223%22,%22himgM7%22:%22http://pic01.v.vnet.mobi/mcover/2017/5/39984414/200_7337988004.jpg%22,%22contentId%22:%22C39984414%22,%22himgM8%22:%22http://pic01.v.vnet.mobi/mcover/2017/5/39984414/131072_7337988004.jpg%22,%22productId%22:%221000000228%22,%22title%22:%22\u4e00\u6761\u72d7\u7684\u4f7f\u547d%22,%22ppvStyle%22:0,%22length%22:6000,%22plats%22:32,%22nowseriescount%22:0}

function CollectController:Add()

	local timestamp = self.time()
	local helper = self.helper
	local getconf = helper.Conf

	local args = ngx_req_get_uri_args()
	local must = {"authID", "contentID", "contentType", "devID", "appID"}
	local i = helper.CheckMust(args, must);
	
	if i then
		error(helper.Error(11, "缺少必传参数: " .. must[i]))  -- 抛出异常
	end
	
	local authID = args.authID
	local contentID = args.contentID
	local contentType = args.contentType
	local index = args.index
	local devID = args.devID
	local appID = args.appID
	local ext = args.ext
	args.createTime = timestamp
	
	local redis = self:redis()
	
	local hash_key = getconf("COLLECT_HASH_KEY_PREFIX") .. appID .. "_" .. authID
	local zset_key = getconf("COLLECT_ZSET_KEY_PREFIX") .. appID .. "_" .. authID
	local content_key = getconf("CONTENT_DETAIL_PREFIX") .. contentID
	local hash_field = contentID
	
	-- 判断 hash_key是否存在缓存中
	local res, err = redis:exists(hash_key)
	
	if not res then
		error(helper.Error(13, "redis exists fail"))
	end
	
	if res == 0 then
		local db = self:mysql()
		
		local res, err, errcode, sqlstate = db:query("SELECT * FROM " . getconf("COLLECT_TABLE") . " WHERE authID = '" .. authID .. "' AND appID = '" .. appID .. "' ORDER BY id DESC", getconf("COLLECT_ZSET_MAX_LENGTH"))
		
		if not res then
			helper.Debug("mysql query fail:", err)
			error(helper.Error(15, "mysql query fail"))
		end
		
		for _,v in ipairs(res)  do
			if type(v) == "table" then
				redis:init_pipeline(2)
				redis:hset(hash_key, v.contentID, helper.encode(v))
				redis:zadd(zset_key, v.createTime, v.contentID)
				redis:commit_pipeline()
			end
		end
	end
	
	args.ext = nil
	local collect_value = helper.encode(args)

	local res, err = redis:hget(hash_key, hash_field)
	if res and type(res) == "string" then
		-- 已收藏该节目
		error(helper.Error(141, "该节目已收藏"))
	end
	
	redis:init_pipeline(6)
	
	redis:hset(hash_key, "-1", tostring(timestamp))  -- 占位并记录最新的修改时间
	redis:lpush(getconf("COLLECT_AUTHIDS_LIST_KEY"), appID .. '_' .. authID) -- 加入队列异步导入数据库
	
	-- 添加新的收藏记录
	redis:hset(hash_key, hash_field, collect_value)
	redis:zadd(zset_key, timestamp, contentID)
	redis:set(content_key, ext)
	redis:expire(content_key, getconf("CONTENT_DETAIL_TTL"))
	
	redis:commit_pipeline()
	
	local count = redis:zcount(zset_key, "-inf", "+inf")
	local need_del_count = count - getconf("COLLECT_ZSET_MAX_LENGTH")
	if need_del_count > 0 then
		-- 获取最老的数据
		local res, err = redis:zrange(zset_key, 0, need_del_count - 1)
		if not res then
			error(helper.Error(16, "redis zrange fail"))
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
					error(helper.Error(17, "redis hdel fail"))
				end
			end
		end	
	
		redis:zremrangebyrank(zset_key, 0, need_del_count - 1)
	end
	
	-- 更新redis ttl
	if getconf("COLLECT_IS_EXPIRE") then
		redis:init_pipeline(3)
		redis:expire(zset_key, getconf("COLLECT_KEEPALIVE_TIME"))
		redis:expire(hash_key, getconf("COLLECT_KEEPALIVE_TIME"))
		redis:expire(content_key, getconf("CONTENT_DETAIL_TTL"))
		redis:commit_pipeline()
	end
	
	return nil
end


-- 收藏列表
-- http://192.168.23.149:8091/service/UserCollect/List?authID=104300078093352170325&contentType=3&page=1&size=30&categoryID=&devID=000001&appID=115020310221&productIDs=1000000456,1000000085,1000000064,1000000198,1000000218,1000000207,1000000228,1000000432,1000000442

function CollectController:List()
		local timestamp = self.time()
		local helper = self.helper
		local getconf = helper.Conf

        local args = ngx_req_get_uri_args()
        local must = {"authID", "devID", "appID", "contentType", "page", "size"}
        local i = helper.CheckMust(args, must);

        if i then
				error(helper.Error(21, "缺少参数 " .. must[i]))
        end
		
		local authID = args.authID
		local devID = args.devID
		local appID = args.appID
		local contentType = tonumber(args.contentType)
		local page = tonumber(args.page)
		local size = tonumber(args.size)
		local productIDs = args.productIDs

		local offset =  (page - 1) * size
		
		local hash_key = getconf("COLLECT_HASH_KEY_PREFIX") .. appID .. "_" .. authID
		local zset_key = getconf("COLLECT_ZSET_KEY_PREFIX") .. appID .. "_" .. authID
		
		local productIDArr = {}
		if not helper.Empty(productIDs) then
			productIDArr = helper.Explode(productIDs, ',')
		end
		
		local redis = self:redis()
		
		-- 判断缓存中是否存在该键值
		local res, err = redis:exists(hash_key)
		
		if not res then
			error(helper.Error(22, "redis exists query fail"))
		end
		
		if res == 0 then
			local db = self:mysql()
			
			local res, err, errcode, sqlstate = db:query("SELECT * FROM " . getconf("COLLECT_TABLE") . " WHERE authID = '" .. authID .. "' AND appID = '" .. appID .. "' ORDER BY id DESC", getconf("COLLECT_ZSET_MAX_LENGTH"))
			if not res then
				error(helper.Error(23, "mysql query fail"))
			else
				for _,v in ipairs(res)  do
					if type(v) == "table" then
						redis:init_pipeline()
						redis:hset(hash_key, v.contentID, helper.encode(v))
						redis:zadd(zset_key, v.createTime, v.contentID)
						redis:commit_pipeline()
					end
				end
			end
		end
		
		redis:hset(hash_key, "-1", tostring(timestamp))  -- 占位并记录最新的修改时间
		
		local allCollects, err = redis:hgetall(hash_key)
		
		if not allCollects then
			error(helper.Error(24, "redis hgetall query fail"))
		end
		
		allCollects = redis:array_to_hash(allCollects)
		allCollects["-1"] = nil

		if helper.Empty(allCollects) then
			return {}
		end
		
		local mdetailKeys = {}
		local rdetailKeys = {}
		local content_detail_prefix = getconf("CONTENT_DETAIL_PREFIX")
		
		local filterAll = {}
		
		for k,v in pairs(allCollects) do
			v = helper.decode(v)
			if tonumber(v.contentType) == contentType then
				if v.createTime then
					v.time = v.createTime
					v.createTime = nil
				end
				filterAll[#filterAll+1] = v
				mdetailKeys[#mdetailKeys+1] = k
				rdetailKeys[#rdetailKeys+1] = content_detail_prefix .. k
			end
		end
		local Details, err = redis:mget(unpack(rdetailKeys))
		if not Details then
			error(helper.Error(25, "redis mget query fail"))
		end
		
		local redisDtls = helper.ArrayCombine(mdetailKeys, Details)
		
		local nullKeys = {}
		for i, det in ipairs(Details) do
			if det == null then
				nullKeys[#nullKeys+1] = mdetailKeys[i]
			end
		end
		
		local mysqlDtls = {}
		-- 映射数据库详情数据
		if not helper.Empty(nullKeys) then
			local db = self:mysql()
			
			local commaNullKeys = helper.ArrayMap(nullKeys, function (val) return "'" .. val .. "'" end)
			local res, err, errcode, sqlstate = db:query("SELECT * FROM " . getconf("CONTENT_DETAIL_TABLE") . " WHERE contentID IN (" .. table_concat(commaNullKeys, ",") .. ")", #commaNullKeys)
			
			if not res then
				helper.Debug("mysql query fail: ", err)
				error(helper.Error(26, "mysql query fail"))
			end

			local rddtailkey
			local content_detail_ttl = getconf("CONTENT_DETAIL_TTL")
			
			for _, row in ipairs(res) do
				if type(row) == "table" then
					mysqlDtls[row.contentID] = row.ext
					rddtailkey = content_detail_prefix .. row.contentID
					redis:set(rddtailkey, row.ext)
				end
			end
			
			for _, CID in ipairs(nullKeys) do
				if not mysqlDtls[CID] then
					rddtailkey = content_detail_prefix .. CID
					redis:set(rddtailkey, "{}")
				end
			end			
		end
		

		redis:init_pipeline()
		local CID
		for i,v in pairs(filterAll) do
			CID = v.contentID
			rddtailkey = content_detail_prefix .. CID
			redis:expire(rddtailkey, content_detail_ttl)
			v.ext = mysqlDtls[CID] and helper.decode(mysqlDtls[CID]) or (redisDtls[CID] ~= null and helper.decode(redisDtls[CID]) or {})
			filterAll[i] = v
		end
		redis:commit_pipeline()
		
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
		if getconf("COLLECT_IS_EXPIRE") then
			redis:init_pipeline()
			redis:expire(hash_key, getconf("COLLECT_KEEPALIVE_TIME"))
			redis:expire(zset_key, getconf("COLLECT_KEEPALIVE_TIME"))
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

-- 判断用户是否已收藏指定节目
-- http://192.168.23.149:8091/service/UserCollect/Get?authID=104300078093352170325&contentID=C39984414&devID=000001&appID=115020310221&productID=1000000228

function CollectController:Get()
	local helper = self.helper
	local getconf = helper.Conf
	
	local args = ngx_req_get_uri_args()
	local must = {"authID", "contentID", "devID", "appID"}
	local i = helper.CheckMust(args, must);

	if i then
		error(helper.Error(31, "缺少参数 " .. must[1]))
	end
	
	local authID = args.authID
	local contentID = args.contentID
	local devID = args.devID
	local appID = args.appID
	
	local hash_key = getconf("COLLECT_HASH_KEY_PREFIX") .. appID .. "_" .. authID
	local zset_key = getconf("COLLECT_ZSET_KEY_PREFIX") .. appID .. "_" .. authID
	local hash_field = contentID
	
	local redis = self:redis()
	
	local hash_key_exists, err = redis:exists(hash_key)
	
	if not hash_key_exists then
		error(helper.Error(32, "redis exists query fail"))
	end
	
	-- 缓存不存在， 映射数据库数据
	if hash_key_exists == 0 then
		local db = self:mysql()
		
		local res, err, errcode, sqlstate = db:query("SELECT * FROM " . getconf("COLLECT_TABLE") . " WHERE authID = '" .. authID .. "' AND appID = '" .. appID .. "' ORDER BY id DESC", getconf("COLLECT_ZSET_MAX_LENGTH"))
		if not res then
			helper.Debug("mysql query fail:", err)
			error(helper.Error(33, "mysql query fail"))
		else
			for _,v in ipairs(res)  do
				if type(v) == "table" then
					redis:hset(hash_key, v.contentID, helper.encode(v))
					redis:zadd(zset_key, v.createTime, v.contentID)
				end
			end
		end
	end
	
	local res, err = redis:hget(hash_key, hash_field)
	if res and type(res) == "string" then
		res = helper.decode(res)
	else
		res = {}
	end
	
	-- 更新redis ttl
	if getconf(COLLECT_IS_EXPIRE) then
		redis:init_pipeline()
		redis:expire(zset_key, getconf("COLLECT_KEEPALIVE_TIME"))
		redis:expire(hash_key, getconf("COLLECT_KEEPALIVE_TIME"))
		redis:commit_pipeline()
	end
	return res
end

-- 删除收藏
-- http://192.168.23.149:8091/service/UserCollect/Del?authID=104300078093352170325&contentType=3&contentIDs=C39984414,C39984469&devID=000001&appID=115020310221&productIDs=1000000218,1000000100,1000000228,1000000432,1000000442

function CollectController:Del()
	local timestamp = self.time()
	local helper = self.helper
	local getconf = helper.Conf
	
	local args = ngx_req_get_uri_args()
	local must = {"authID", "devID", "appID"}
	local i = helper.CheckMust(args, must);

	if i then
		error(helper.Error(41, "缺少参数 " .. must[index]))
	end
	
	local authID = args.authID
	local devID = args.devID
	local appID = args.appID
	local contentIDs = args.contentIDs

	local hash_key = getconf("COLLECT_HASH_KEY_PREFIX") .. appID .. "_" .. authID	
	local zset_key = getconf("COLLECT_ZSET_KEY_PREFIX") .. appID .. "_" .. authID
	
	local redis = self:redis()
	
	redis:lpush(getconf("COLLECT_AUTHIDS_LIST_KEY"), appID .. '_' .. authID)  --加入队列异步入数据库
	
	if helper.Empty(contentIDs) then
		--删除所有
		redis:init_pipeline()
		redis:del(zset_key)
		redis:del(hash_key)
		redis:commit_pipeline()
		
		redis:hset(hash_key, "-1", tostring(timestamp))  -- 占位并记录最新的修改时间
		-- 更新redis ttl
		if getconf("COLLECT_IS_EXPIRE") then
			redis:init_pipeline()
			redis:expire(hash_key, getconf("COLLECT_KEEPALIVE_TIME"))
			redis:expire(zset_key, getconf("COLLECT_KEEPALIVE_TIME"))
			redis:commit_pipeline()
		end
		return nil
	end
	
	local res, err = redis:exists(hash_key)
	
	if not res then
		error(helper.Error(42, "redis exists query fail"))
	end
	
	if res == 0 then
		local db = self:mysql()		

		local res, err, errcode, sqlstate = db:query("SELECT * FROM " . getconf("COLLECT_TABLE") . " WHERE authID = '" .. authID .. "' AND appID = '" .. appID .. "' ORDER BY id DESC", getconf("COLLECT_ZSET_MAX_LENGTH"))
		
		if not res then
			error(helper.Error(43, "mysql query fail"))
		else
			for _,v in ipairs(res)  do
				if type(v) == "table" then
					redis:hset(hash_key, v.contentID, helper.encode(v))
					redis:zadd(zset_key, v.createTime, v.contentID)
				end
			end
		end
	end
	
	redis:hset(hash_key, "-1", tostring(timestamp))  -- 占位并记录最新的修改时间
	
	-- 删除指定内容
	local contentIDArr = helper.Explode(contentIDs, ",")
	
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
			if not helper.Empty(v) then
				hashdels[#hashdels + 1] = hash_fields[i]
				zsetdels[#zsetdels + 1] = v
			end
		end
	end
	
	if #hashdels > 0 then 
		res, err = redis:hdel(hash_key, unpack(hash_fields))
		if not res then
			error(helper.Error(44, "redis hdel fail"))
		end
	end
	
	if #zsetdels > 0 then
		res, err = redis:zrem(zset_key, unpack(zsetdels))
		if not res then
			error(helper.Error(45, "redis zrem fail"))
		end
	end
	
	if #hashdels > 0 then 
		res, err = redis:zrem(zset_key, unpack(hash_fields))
		if not res then
			error(helper.Error(46, "redis zrem fail"))
		end
	end
	
	-- 更新redis ttl
	if getconf("COLLECT_IS_EXPIRE") then
		redis:init_pipeline()
		redis:expire(hash_key, getconf("COLLECT_KEEPALIVE_TIME"))
		redis:expire(zset_key, getconf("COLLECT_KEEPALIVE_TIME"))
		redis:commit_pipeline()
	end
	return nil
end


return CollectController
