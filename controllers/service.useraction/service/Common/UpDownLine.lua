-- 同步视频上下线状态
-- created by $Joy Zhang$ <1054948153@qq.com>
-- http://192.168.23.149:8091/service/Common/UpDownLine?act=down&contentIDs=C39778370,C39778371
-- act=down 视频下线  act=up 视频重新上线
-- @user ZY
-- @date 2017-10-19
-- @file UpDownLine.lua

local ngx = ngx
local type = type
local ipairs = ipairs
local tonumber = tonumber
local unpack = table.unpack or unpack
local match = string.match
local gsub = string.gsub
local result = {code=0, msg="OK"}
local redis = nil			--redis handler
local redis_is_connected = false
local db = nil	--mysql handler
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
		result.code = 2
		result.msg = "无法加载 Utils.lua库"
		break
	end

	ok, Config = pcall(require, "Config")
	if not ok then
		result.code = 3
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
	
	local ok, mysql = pcall(require, "mysql")
	if not ok then
		result.code = 4
		result.msg = "无法加载 mysql.lua库"
		break
	end
	
	local args = ngx.req.get_uri_args()

	local must = {"act", "contentIDs"}
	local index = Utils.checkMust(args, must);
	if index then
		result.code = 4
		result.msg = "缺少参数 " .. must[index]
		break
	end
	
	local contentIDs = args.contentIDs
	local contentIDArr = Utils.explode(contentIDs, ",")
	local act = args.act
	
	if act ~= 'up' and act ~= 'down' then
		result.code = 5
		result.msg = "act参数值只能为up或down"
		break
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
	
	
	local ok, err
	if act == 'up' then
		ok, err = redis:srem(Config.DOWNLINE_CONTENTIDS_SET, unpack(contentIDArr))
	else
		ok, err = redis:sadd(Config.DOWNLINE_CONTENTIDS_SET, unpack(contentIDArr))
	end
	
	if not ok then
		result.code = 5
		result.msg = "redis sadd 命令执行失败"
		break
	end
	
	-- db, err = mysql:new()
	-- db:set_timeout(Config.MYSQL_TIMEOUT)
	-- local ok, err, errcode, sqlstate = db:connect({
		-- host = Config.MYSQL_HOST,
		-- port = Config.MYSQL_PORT,
		-- database = Config.MYSQL_DBNAME,
		-- user = Config.MYSQL_USER,
		-- password = Config.MYSQL_PASS,
		-- max_packet_size = Config.MYSQL_MAX_PACKET_SIZE
	-- })
	-- if not ok then
		-- result.code = 5
		-- result.msg = "无法连接mysql: " .. err
		-- break
	-- end
	-- mysql_is_connected = true

	-- local res, err, errcode, sqlstate = db:query("set names utf8")
	-- if not res then
		-- result.code = 6
		-- result.msg = "mysql查询失败"
		-- break
	-- end
	
	-- res, err, errcode, sqlstate = db:query("UPDATE ua_user_collect SET status = " .. status .. " WHERE contentID IN " .. contentIDs)
	-- if not res then
		-- result.code = 7
		-- result.msg = "数据库更新失败"
		-- break
	-- end

until true

if redis_is_connected then
	redis:set_keepalive(Config.REDIS_KEEPALIVE_TIME, Config.REDIS_KEEPALIVE_COUNT)
end

if mysql_is_connected then
	db:set_keepalive(Config.MYSQL_KEEPALIVE_TIME, Config.MYSQL_KEEPALIVE_COUNT)
end

ngx.say(Cjson.encode(result))
