local ngx = ngx
local error = error


local BaseController = require(APP_ROOT .. "/controllers/BaseController")

local IndexController = {}


extends(IndexController, BaseController)


function IndexController:index()
	local args = ngx.req.get_uri_args()

	if not args["user"] then
		error(self:Error(21, "user is empty"))
	end
	
	return "default_page"
end

function IndexController:customerule()
	return "custome_rule"
end


function IndexController:defaultrule()
	return "default_rule"
end


function IndexController:Add()
	return "IndexController/Add"
end


return IndexController
