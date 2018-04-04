local type = type
local error = error
local ngx_re_match = ngx.re.match

local routers = require(APP_ROOT .. "/routers/router")

local dispatcher = {}

function dispatcher:new(helper)
	self.helper = helper
	return self
end

function dispatcher:dispatch(RequestURI)
	local ControllerName = nil
	local MethodName = nil

   if RequestURI == "/" then
                ControllerName = "IndexController"
                MethodName = "index"
   else
	
	if RequestURI:sub(1, 1) == "/" then
		RequestURI = RequestURI:sub(2)
	end
	if RequestURI:sub(-1) == "/" then
		RequestURI = RequestURI .. "index"
	end
	
	-- 自定义路由规则
	for k, v in pairs(routers) do
		if k == RequestURI then
			local match,_ = ngx_re_match(v, "^([^@]+)@?(?:@([^@]+))?$")
			if not match then
				error("router setting error, please config like controller@method", 2)
			end
			ControllerName = match[1]
			MethodName = match[2] or "index"
		end
	end

	-- 默认路由规则  
	-- user =>           controller : UserController  method : index()
	-- user/login        controller : UserController  method : login()
	-- user/foo/bar  =>  controller : user/FooController method :  bar()
     	if not ControllerName then

		local pos, _ = RequestURI:reverse():find("/", 1, true)

		if pos then
			ControllerName = RequestURI:sub(1, 0 - pos - 1)
			MethodName = RequestURI:sub(0 - pos + 1)
		else
			ControllerName = RequestURI
			MethodName = "index"
		end

		pos, _ = ControllerName:reverse():find("/", 1, true)

		if pos then
			ControllerName = ControllerName:sub(1, 0 - pos) .. ControllerName:sub(0 - pos + 1):upper() .. ControllerName:sub(0 - pos + 2) .. "Controller"
		else
			ControllerName = ControllerName:sub(1,1):upper() .. ControllerName:sub(2) .. "Controller"
		end

	end
   end

	local ok, ControllerClass = pcall(require, APP_ROOT .. "/controllers/" .. ControllerName)

	if not ok then
			self.helper.Debug(ControllerClass)
        	return nil, nil, self.helper.Error(111, "controller Not Found")
	end


	local Controller = ControllerClass:new(self.helper)
	local Method = Controller[MethodName]

	if type(Method) ~= "function" then
		return nil, nil, self.helper.Error(112, "method Not Found")
	end

	return Controller, Method, nil
end

return dispatcher
