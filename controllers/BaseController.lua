-- 基础控制器
local type = type
local unpack = table.unpack or unpack

local AutoLoader = {}

function AutoLoader:setHelper(helper)
	self.helper = helper
end

function AutoLoader:auto_mysql(base)
	local getconf = self.helper.Conf
	local Error = self.helper.Error
	local Debug = self.helper.Debug
	
	local mysql = require(APP_ROOT .. "/libraries/mysql")
	local db, err = mysql:new()
	if not db then
		Debug("init mysql fail: ", err)
		return nil, Error(212, "init mysql fail")
	end
	local _, err = db:set_timeout(self.helper.Conf(MYSQL_TIMEOUT, 0))
	
	if err then
		Debug("mysql set_timeout fail:", err)
		return nil, Error(213, "mysql set_timeout fail")
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
		return nil, Error(214, "mysql connect fail")
	end
	
	base:addCleanup(function(db) db:close() end, {db})
	
	local res, err, errcode, sqlstate = db:query("SET NAMES utf8")
	
	if not res then
		Debug("set char set fail", err)
		return nil, Error(215, "set char set fail")
	end
	
	return db
end

function AutoLoader:auto_redis(base)
	return "redis instance"
end

function AutoLoader:load(base, property)
	property = "auto_" .. property
	if self[property] and type(self[property]) == "function" then
		return self[property](self, base)
	else
		self.helper.Debug("access not exists property: ", property)
		return nil, self.helper.Error(211, "property not found")
	end
end

function AutoLoader.autoload(base, property)
	return AutoLoader:load(base, property)
end

local BaseController = {}
extends(BaseController, AutoLoader.autoload)  -- 注入自动加载方法

function BaseController:new(helper)
	AutoLoader:setHelper(helper)
	local new = {helper = helper}
	ngx.ctx.cleanups = {}
	helper.Extends(new, self)
	return new
end

function BaseController:Conf(item, default)
	return self.helper.Conf(item, default)
end

function BaseController:Error(code, msg)
	return self.helper.Error(code, msg)
end

function BaseController:addCleanup(func, params)
	ngx.ctx.cleanups[#ngx.ctx.cleanups + 1] = {func = func, params = params}
end

function BaseController:__destruct()
	local cleanup
	for i = 1, #ngx.ctx.cleanups do
		cleanup = ngx.ctx.cleanups[i]
		cleanup["func"](unpack(cleanup["params"]))
	end	
end


return BaseController
