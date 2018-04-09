local pcall = pcall
local type = type
local ngx = ngx
local ngx_re_match = ngx.re.match

local request_filename = ngx.var.request_filename
local match, _ = ngx_re_match(ngx.var.request_filename, "^(.*)/")  
if not match then
	ngx.print("bad request")
	ngx.exit(ngx.HTTP_BAD_REQUEST)
end

APP_ROOT = match[1] --  全局变量 项目根目录
local RequestURI = ngx_re_match(ngx.var.request_uri, "^[^?]+")[0]  -- request_uri

local helper = require(APP_ROOT .. "/helpers/helper")
local dispatcher = require(APP_ROOT .. "/libraries/dispatcher")


extends = helper.Extends
helper:Extends(require("cjson"))

local patcher = dispatcher:new(helper)
local Controller, Method, Error = patcher:dispatch(RequestURI)

if Error then
	helper.Exit(Error.code, Error.msg)
end

ngx.header["Content-Type"] = "text/html;charset=utf-8"

local ok, ret = pcall(Method, Controller)

Controller:__destruct()  -- 调用析构函数

if not ok then
	if type(ret) == "table" then
		helper.Exit(ret.code, ret.msg)
	else
		helper.Exit(444, ret)
	end
end

helper.Exit(0, "ok", ret)


