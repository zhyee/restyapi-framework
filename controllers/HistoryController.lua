-- created by $Joy Zhang$ <zhangy@tv189.com>
-- @author ZY
-- @date 2018-04-08
-- @file HistoryController.lua
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
local HistoryController = {}

extends(HistoryController, BaseController)

-- 新增播放记录
-- http://192.168.23.149:8091/service/UserPlayHistory/Add?authID=104318907191667130930&contentID=C39778370&contentType=1&length=15&index=1&devID=000001&appID=111010310225&plat=32&productID=1000000432&ext={%22countryName%22:%22u6b27u7f8e%22,%22siteFolderId%22:11111122,%22seriescount%22:0,%22categoryId%22:%222%22,%22imgM8%22:%22http://pic01.v.vnet.mobi/mcover/2017/5/39984414/131072_7337988005.jpg%22,%22contentType%22:%223%22,%22himgM7%22:%22http://pic01.v.vnet.mobi/mcover/2017/5/39984414/200_7337988004.jpg%22,%22contentId%22:%22C39984414%22,%22himgM8%22:%22http://pic01.v.vnet.mobi/mcover/2017/5/39984414/131072_7337988004.jpg%22,%22productId%22:%221000000228%22,%22title%22:%22\u4e00\u6761\u72d7\u7684\u4f7f\u547d%22,%22ppvStyle%22:0,%22length%22:6000,%22plats%22:32,%22nowseriescount%22:0}
function HistoryController:Add()
	local timestamp = self.time()
	local helper = self.helper
	local getconf = helper.Conf

	local args = ngx_req_get_uri_args()

	local must = {"authID", "contentID", "contentType", "length", "devID", "appID", "plat", "productID"}

	local i = helper.CheckMust(args, must);

	if i then
		error(helper.Error(21, "缺少参数 " .. must[i]))
	end
	
	local authID = args.authID
	local contentID = args.contentID
	local contentType = args.contentType
	local length = args.length
	local index = args.index
	local devID = args.devID
	local appID = args.appID
	local plat = args.plat
	local productID = args.productID
	local parentID = args.parentID
	args.createTime = timestamp
	args.status = 1
	
	local redis = self:redis()
	
	local hash_key = getconf("PLAY_HISTORY_HASH_KEY_PREFIX") .. authID
	local zset_key = getconf("PLAY_HISTORY_ZSET_KEY_PREFIX") .. authID
	local hash_field = contentID
	
	redis:hset(hash_key, "-1", tostring(timestamp))  -- 占位并记录最新的修改时间
	--redis:lpush(Config.PLAY_HISTORY_AUTHIDS_LIST_KEY, appID .. '_' .. authID)  -- 加入队列异步入数据库 暂时用不到

	local play_value = helper.encode(args)

	local res, err = redis:hget(hash_key, hash_field)
	if res and type(res) == "string" then
		-- 从zset中删除已有内容播放记录
		redis:zrem(zset_key, res)
	end
	-- 添加新的播放记录
	redis:hset(hash_key, hash_field, play_value)
	redis:zadd(zset_key, timestamp, contentID)

	local count = redis:zcount(zset_key, "-inf", "+inf")
	local need_del_count = count - getconf("PLAY_HISTORY_ZSET_MAX_LENGTH")
	if need_del_count > 0 then
		-- 获取最老的数据
		local res, err = redis:zrange(zset_key, 0, need_del_count - 1)
		if not res then
			error(helper.Error(22, "redis zrange fail"))
		else
			local del_hashkeys = {}
			if type(res) == 'table' then
				for _,v in pairs(res) do
					if not string_find(v, "{", 1, true) then
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
					error(helper.Error(23, "redis hdel fail"))
				end
			end
		end
		
		redis:zremrangebyrank(zset_key, 0, need_del_count - 1)
	end
	
	-- 更新redis ttl
	if getconf("PLAY_HISTORY_IS_EXPIRE") then
		redis:init_pipeline()
		redis:expire(zset_key, getconf("PLAY_HISTORY_KEEPALIVE_TIME"))
		redis:expire(hash_key, getconf("PLAY_HISTORY_KEEPALIVE_TIME"))
		redis:commit_pipeline()
	end
	
	return nil
end

-- 历史播放记录列表
-- http://192.168.23.149:8091/service/UserPlayHistory/List?authID=104318907191667130930&devID=000001&appID=111010310225&contentType=1&page=1&size=20&productIDs=1000000218,1000000100,1000000228,1000000432,1000000442

function HistoryController:List()
	local timestamp = self.time()
	local helper = self.helper
	local getconf = helper.Conf

	local args = ngx_req_get_uri_args()
	local must = {"authID", "devID", "appID", "contentType", "productIDs", "page", "size"}
	local i = helper.CheckMust(args, must);

	if i then
			error(helper.Error(31, "缺少参数 " .. must[i]))
	end
	
	local authID = args.authID
	local devID = args.devID
	local appID = args.appID
	local contentType = args.contentType
	local productIDs = args.productIDs
	local page = tonumber(args.page)
	local size = tonumber(args.size)
	
	local offset = (page - 1) * size
	local productIDsArr = helper.Explode(productIDs, ",")

	local zset_key = getconf("PLAY_HISTORY_ZSET_KEY_PREFIX") .. authID
	local hash_key = getconf("PLAY_HISTORY_HASH_KEY_PREFIX") .. authID
	
	local redis = self:redis()
	
	redis:hset(hash_key, "-1", tostring(timestamp))  -- 占位并记录最新的修改时间
	
	--local res, err = redis:zrevrange(zset_key, offset, offset + size - 1)
	
	local allKeys, err = redis:hkeys(hash_key)
	if not allKeys then
		error(helper.Error(32, "redis hkeys query fail"))
	end
	
	local tmpSetKey = 'ua_pl_tmp_set_key_' .. authID
	local ok, err = redis:sadd(tmpSetKey, unpack(allKeys))
	
	local diffKeys, err = redis:sdiff(tmpSetKey, getconf("DOWNLINE_CONTENTIDS_SET"))
	
	if not diffKeys then
		error(helper.Error(33, "redis sdiff query fail"))
	end
	
	local ok, err  = redis:del(tmpSetKey)
	
	local filterAll = {}
	
	if #diffKeys > 0 then
		local res, err = redis:hmget(hash_key, unpack(diffKeys))
		if not res then
			error(helper.Error(34, "hmget query fail"))
		else
			if type(res) == "table" then
				for _,v in pairs(res) do
					if v ~= ngx_null then
						v = helper.decode(v)
						if type(v) == "table" then
							if helper.InArray(v.productID, productIDsArr) then
								if v.createTime then
									v.time = v.createTime
									v.createTime = nil
								end
								
								if type(v.ext) == 'string' and not helper.Empty(v.ext) then
									v.ext = helper.decode(v.ext)
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
	
	table_sort(filterAll, comp)
	
	local total = #filterAll
	local pagetotal = math_ceil(total / size)

	local playLists = {}
	
	for i = offset + 1, offset + size do
		playLists[#playLists + 1] = filterAll[i]
	end
	
	-- 更新redis ttl
	if getconf("PLAY_HISTORY_IS_EXPIRE") then
		redis:init_pipeline()
		redis:expire(hash_key, getconf("PLAY_HISTORY_KEEPALIVE_TIME"))
		redis:expire(zset_key, getconf("PLAY_HISTORY_KEEPALIVE_TIME"))
		redis:commit_pipeline()
	end

	return playLists

end

-- 删除播放历史
-- http://192.168.23.149:8091/service/UserPlayHistory/Del?authID=104318907191667130930&devID=000001&appID=111010310225&contentIDs=C39778370&contentType=2&productIDs=1000000218,1000000100,1000000228,1000000432,1000000442

function HistoryController:Del()
	local timestamp = self.time()
	local helper = self.helper
	local getconf = helper.Conf
	
	local args = ngx_req_get_uri_args()
	local must = {"authID", "devID", "appID", "contentType"}
	local i = helper.CheckMust(args, must)

	if i then
			error(helper.Error(41, "缺少参数 " .. must[i]))
	end
	
	local authID = args.authID
	local devID = args.devID
	local appID = args.appID
	local contentIDs = args.contentIDs
	local contentType = args.contentType
	
	local zset_key = getconf("PLAY_HISTORY_ZSET_KEY_PREFIX") .. authID
	local hash_key = getconf("PLAY_HISTORY_HASH_KEY_PREFIX") .. authID
	
	
	local redis = self:redis()
	--if contentIDs == "ALL" then
	if helper.Empty(contentIDs) then
		--删除所有
		redis:init_pipeline()
		redis:del(zset_key)
		redis:del(hash_key)
		redis:hset(hash_key, "-1", tostring(timestamp))  -- 占位并记录最新的修改时间
		redis:commit_pipeline()
		
		-- 更新redis ttl
		if getconf("PLAY_HISTORY_IS_EXPIRE") then
			redis:init_pipeline()
			redis:expire(hash_key, Config.PLAY_HISTORY_KEEPALIVE_TIME)
			redis:expire(zset_key, Config.PLAY_HISTORY_KEEPALIVE_TIME)
			redis:commit_pipeline()
		end
		
		return
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
				error(helper.Error(42, "redis hdel fail"))
			end
		end
		
		if #zsetdels > 0 then
			res, err = redis:zrem(zset_key, unpack(zsetdels))
			if not res then
				error(helper.Error(43, "redis zrem fail"))
			end
		end
		
		if #hashdels > 0 then 
			res, err = redis:zrem(zset_key, unpack(hash_fields))
			if not res then
				error(helper.Error(44, "redis zrem fail"))
			end
		end
		
		-- 更新redis ttl
		if getconf("PLAY_HISTORY_IS_EXPIRE") then
			redis:init_pipeline()
			redis:expire(hash_key, getconf("PLAY_HISTORY_KEEPALIVE_TIME"))
			redis:expire(zset_key, getconf("PLAY_HISTORY_KEEPALIVE_TIME"))
			redis:commit_pipeline()
		end
		return
end

-- 获取指定内容的播放记录
-- http://192.168.23.149:8091/service/UserPlayHistory/GetByContentIDs?authID=104318907191667130930&contentIDs=C39808305,C39778377,C39778370&devID=000001&appID=115020310221&contentType=1&productIDs=1000000228,1000000218,1000000100,1000000228,1000000432,1000000442

function HistoryController:GetByContentIDs()
	local timestamp = self.time()
	local helper = self.helper
	local getconf = helper.Conf
	local args = ngx_req_get_uri_args()
	local must = {"authID", "devID", "appID", "contentType", "contentIDs", "productIDs"}
	local i = helper.CheckMust(args, must);

	if i then
			error(helper.Error(51, "缺少参数 " .. must[i]))
	end
	
	local authID = args.authID
	local contentIDs = args.contentIDs
	local devID = args.devID
	local appID = args.appID
	local contentType = args.contentType
	local productIDs = args.productIDs

	local contentIDsArr = helper.Explode(contentIDs, ",")
	local productIDsArr = helper.Explode(productIDs, ",")
	
	local newContentIDsArr = {}
	if type(contentIDsArr) == 'table' then
		for _,v in ipairs(contentIDsArr) do
			if not helper.Empty(v) then
				newContentIDsArr[#newContentIDsArr+1] = v
			end
		end
	end
	
	contentIDArr = newContentIDsArr
	newContentIDsArr = nil
	
	local zset_key = getconf("PLAY_HISTORY_ZSET_KEY_PREFIX") .. authID
	local hash_key = getconf("PLAY_HISTORY_HASH_KEY_PREFIX") .. authID
	
	local redis = self:redis()
	
	redis:hset(hash_key, "-1", tostring(timestamp))  -- 占位并记录最新的修改时间
	
	local res, err = {}, nil
	if #contentIDsArr > 0 then
		res, err = redis:hmget(hash_key, unpack(contentIDsArr))
	end
	
	local playLists = {}
	if not res then
		error(helper.Error(52, "hmget query fail"))
	else
		if type(res) == "table" then
			for _,v in pairs(res) do
				if v ~= ngx_null then
					v = helper.decode(v)
					if type(v) == "table" then
						if helper.InArray(v.productID, productIDsArr) then
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
	
	-- 更新redis ttl
	if getconf("PLAY_HISTORY_IS_EXPIRE") then
		redis:init_pipeline()
		redis:expire(hash_key, getconf("PLAY_HISTORY_KEEPALIVE_TIME"))
		redis:expire(zset_key, getconf("PLAY_HISTORY_KEEPALIVE_TIME"))
		redis:commit_pipeline()
	end

	return playLists

end



return HistoryController



