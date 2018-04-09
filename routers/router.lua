return {
	["index/index"] = "IndexController",
	["index/test"] = "IndexController@test",
	
	["service/UserCollect/Add"] = "CollectController@Add",
	["service/UserCollect/Get"] = "CollectController@Get",
	["service/UserCollect/List"] = "CollectController@List",
	["service/UserCollect/Del"] = "CollectController@Del",

	["service/UserPlayHistory/Add"] = "HistoryController@Add",
	["service/UserPlayHistory/List"] = "HistoryController@List",
	["service/UserPlayHistory/Del"] = "HistoryController@Del",
	["service/UserPlayHistory/GetByContentIDs"] = "HistoryController@GetByContentIDs",
	
	["service/UserSubscribe/Add"] = "SubscribeController@Add",
	["service/UserSubscribe/Count"] = "SubscribeController@Count",
	["service/UserSubscribe/Get"] = "SubscribeController@Get",
	["service/UserSubscribe/List"] = "SubscribeController@List",
	["service/UserSubscribe/Del"] = "SubscribeController@Del"
}
