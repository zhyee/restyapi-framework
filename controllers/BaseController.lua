-- 基础控制器
local require = require
local type = type
local error = error
local unpack = table.unpack or unpack
local ngx_now = ngx.now
local math_floor = math.floor
local APP_ROOT = APP_ROOT

local BaseController = {}

function BaseController:new(helper)
	local new = {helper=helper, cleanups={}}
	helper.Extends(new, self)
	return new
end

function BaseController.time()
	return math_floor(ngx_now())
end

function BaseController:addCleanup(callback, params)
	self.cleanups[#self.cleanups + 1] = {callback = callback, params = params}
end


function BaseController:mysql()
	if not self.mysqlconn then
		local getconf = self.helper.Conf
		local Error = self.helper.Error
		local Debug = self.helper.Debug
		
		local mysql = require(APP_ROOT .. "/libraries/mysql")
		local db, err = mysql:new()
		if not db then
			Debug("init mysql fail: ", err)
			error(Error(212, "init mysql fail"))
		end
		local _, err = db:set_timeout(self.helper.Conf(MYSQL_TIMEOUT, 0))
		
		if err then
			Debug("mysql set_timeout fail:", err)
			error(Error(213, "mysql set_timeout fail"))
		end
		
		local ok, err, errcode, sqlstate = db:connect({
			host = getconf("MYSQL_HOST"),
			port = getconf("MYSQL_PORT"),
			database = getconf("MYSQL_DBNAME"),
			user = getconf("MYSQL_USER"),
			password = getconf("MYSQL_PASS"),
			max_packet_size = getconf("MYSQL_MAX_PACKET_SIZE")
		})
		
		if not ok then
			Debug("mysql connect fail", err)
			error(Error(214, "mysql connect fail"))
		end
		
		-- 加入析构
		self:addCleanup(function(db) db:set_keepalive(getconf("MYSQL_KEEPALIVE_TIME"), getconf("MYSQL_KEEPALIVE_COUNT")) end, {db})
		
		local res, err, errcode, sqlstate = db:query("SET NAMES " .. getconf("MYSQL_CHARSET"))
		
		if not res then
			Debug("set char set fail", err)
			error(Error(215, "set charset fail"))
		end
		self.mysqlconn = db
	end
	return self.mysqlconn

end

function BaseController:redis()
	if not self.redisconn then
		local getconf = self.helper.Conf
		local Debug = self.helper.Debug
		local Error = self.helper.Error
		
		local RedisClass = require(APP_ROOT .. "/libraries/redis")
		local redis = RedisClass:new()
		redis:set_timeout(getconf("REDIS_TIMEOUT"))
		local ok, err = redis:connect(getconf("REDIS_HOST"), getconf("REDIS_PORT"))
		if not ok then
			Debug("redis connect fail", err)
			error(Error(216, "redis connect server fail"))
		end
		
		self:addCleanup(function(redis) redis:set_keepalive(getconf("REDIS_KEEPALIVE_TIME"), getconf("REDIS_KEEPALIVE_COUNT")) end, {redis})
		self.redisconn = redis
	end
	return self.redisconn
end

-- 析构函数
function BaseController:__destruct()
	local cleanup
	for i = 1, #self.cleanups do
		cleanup = self.cleanups[i]
		cleanup["callback"](unpack(cleanup["params"]))
	end	
end


return BaseController
