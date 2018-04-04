<?php
/**
 * 用户行为服务
 * 定期将redis数据更新到mysql中
 * @author $Joy Zhang$ 1054948153@qq.com
 * @date 2017-04-18
 * @file ua_redis2mysql.php
 * php ua_redis2mysql.php
 *
 */

error_reporting(E_ERROR);
set_time_limit(0);
date_default_timezone_set("Asia/shanghai");

$err_msg = false;
$redis = NULL;
$db = NULL;
$redis_is_connect = false;
$mysql_is_connect = false;
$timestamp = time();
$COLLECT_TABLE = 'ua_user_collect';
$PLAY_HISTORY_TABLE = 'ua_user_play_history';
$step = 20;

$logFile = '/data/log/useraction/collect_' . date('Y-m-d') . '.log';

/* 打印日志 */
function mylog($info)
{
	global $logFile;
    $info = date("[Y-m-d H:i:s]") . "  " . $info . PHP_EOL . PHP_EOL;
    //echo $info;
    file_put_contents($logFile, $info, FILE_APPEND);
}

/* 判断字段是否是数字类型 */
function isMysqlNumber($fieldType)
{
    if (stripos($fieldType, 'int') !== FALSE)
    {
        return TRUE;
    }

    if (stripos($fieldType, 'decimal') !== FALSE)
    {
        return TRUE;
    }

    if (stripos($fieldType, 'float') !== FALSE)
    {
        return TRUE;
    }

    if (stripos($fieldType, 'double') !== FALSE)
    {
        return TRUE;
    }
    return FALSE;
}

/* 获取表结构 */
function getTableFields($table)
{
    static $fields = array();

    if (!isset($fields[$table]))
    {
        global $db;
        $sql = "SHOW COLUMNS FROM `$table`";
        $res = $db->query($sql);
        $tableFields = array();
        if ($res instanceof mysqli_result)
        {
            while (($row = $res->fetch_assoc()) != FALSE)
            {
                $field = $row['Field'];
                $tableFields[$field]['Type'] = $row['Type'];
                $tableFields[$field]['IsNumber'] = isMysqlNumber($row['Type']);
                $tableFields[$field]['CanBeNull'] = $row['Null'] === 'YES';
                $tableFields[$field]['Default'] = $row['Default'];
            }
        }
        $fields[$table] = $tableFields;
    }

    return $fields[$table];
}

/* 对一行记录中的空值进行默认值设置 */
function setDefaultVal($rowData, $table)
{
    global $db;
    $fields = getTableFields($table);
    foreach ($fields as $field => $prop)
    {
        if ($prop['IsNumber'])
        {
            if (!isset($rowData[$field]) || $rowData[$field] === '')
            {
                if (strlen($prop['Default']))
                {
                    $rowData[$field] = $prop['Default'];
                }
                elseif($prop['CanBeNull'])
                {
                    $rowData[$field] = 'NULL';
                }
            }
        }
        else
        {
            if (!isset($rowData[$field]))
            {
                if (isset($prop['Default']))
                {
                    $rowData = "'{$prop['Default']}'";
                }
                elseif ($prop['CanBeNull'])
                {
                    $rowData[$field] = 'NULL';
                }
            }
            else
            {
                $rowData[$field] = "'" . $db->real_escape_string($rowData[$field]) ."'";
                mylog($rowData[$field]);
            }
        }
    }

    return $rowData;
}


do
{
    mylog("---------------start------------------");

    $confFile = __DIR__ . "/../inc/Config.json";

    if (!file_exists($confFile))
    {
        $err_msg = "配置文件不存在";
        break;
    }

    $confJson = file_get_contents($confFile);
    $conf = json_decode($confJson);


    /* redis */
    $redis = new Redis();
    if (!$redis->connect($conf->REDIS_HOST, $conf->REDIS_PORT))
    {
        $err_msg = "redis连接失败";
        break;
    }

    $redis_is_connect = true;

    /* 数据库 */
    $db = new mysqli($conf->MYSQL_HOST, $conf->MYSQL_USER, $conf->MYSQL_PASS, $conf->MYSQL_DBNAME, $conf->MYSQL_PORT);

    if ($db->connect_errno)
    {
        $err_msg = "mysql连接失败: " . $db->connect_error;
        break;
    }
    $mysql_is_connect = true;

/*
 * 播放记录不进入数据库
 *
 *     // ---------------------播放记录开始-------------------
    $startScore = $timestamp - $PL_UPDATE_INTERVAL_TIME;
    $endScore = $timestamp;

    $appids = $redis->zRangeByScore($conf->PLAY_HISTORY_APPIDS_ZSET_KEY, $startScore, $endScore);

    // 根据appids分批获取播放记录keys
    foreach ($appids as $appid)
    {
        $play_hash_prefix = $conf->PLAY_HISTORY_HASH_KEY_PREFIX . $appid . '_*';
        $keys = $redis->keys($play_hash_prefix);

        foreach ($keys as $key)
        {

            $hash = $redis->hGetAll($key);

            // 如果最近的一个时间段内数据有增删， 则更新到数据库
            if ($timestamp - $hash['-1'] <= $conf->PLAY_HISTORY_KEEPALIVE_TIME)
            {
                $authID = str_replace($conf->PLAY_HISTORY_HASH_KEY_PREFIX . $appid . '_', '', $key);

                //先删除所有
                $sql = "DELETE FROM $PLAY_HISTORY_TABLE WHERE authID = '$authID' AND appID = '$appid'";
                mylog($sql);
                $db->query($sql);

                $index = 0;
                foreach ($hash as $contentID => $fieldVal)
                {
                    if ($index % $step == 0)
                    {
                        $insert_query = "INSERT INTO $PLAY_HISTORY_TABLE (`authID`,`devID`,`appID`,`productID`,`contentID`,`categoryID`,`contentType`,`createTime`,`parentID`,`length`,`status`, `cover`, `index`) VALUES ";
                        $has_data = FALSE;
                    }

                    $fieldVal = json_decode($fieldVal, TRUE);

                    if (is_array($fieldVal) && $contentID == $fieldVal['contentID'])
                    {
                        $fieldVal = setDefaultVal($fieldVal, $PLAY_HISTORY_TABLE);
                        $insert_query .= "({$fieldVal['authID']},{$fieldVal['devID']},{$fieldVal['appID']},{$fieldVal['productID']},{$fieldVal['contentID']},{$fieldVal['categoryID']},{$fieldVal['contentType']},{$fieldVal['createTime']},{$fieldVal['parentID']},{$fieldVal['length']}, {$fieldVal['status']}, {$fieldVal['cover']}, {$fieldVal['index']}),";
                        $has_data = TRUE;
                    }

                    if ($index % $step == ($step - 1) || $index + 1 == count($hash))
                    {
                        $insert_query = substr($insert_query, 0, -1);

                        if ($has_data)
                        {
                            mylog($insert_query);
                            $db->query($insert_query);
                        }
                    }

                    $index ++;
                }
            }
        }
    }
    // -------------------播放记录结束-----------------*/


    /* -------------------------用户收藏开始-------------------------- */
    $len = $redis->lLen($conf->COLLECT_AUTHIDS_LIST_KEY);

    if ($len > 0)
    {
        for($i = 0; $i < $len; $i++)
        {
            $appid_authid = $redis->rPop($conf->COLLECT_AUTHIDS_LIST_KEY);

            if ($appid_authid)
            {
                list($appid, $authID) = explode('_', $appid_authid);
                $key = $conf->COLLECT_HASH_KEY_PREFIX . $appid_authid;
				
				$hash = $redis->hGetAll($key);

                    //先删除所有
                    $sql = "DELETE FROM $COLLECT_TABLE WHERE authID = '$authID' AND appID = '$appid'";
                    mylog($sql);
                    $db->query($sql);

                    if ($db->errno)
                    {
                        mylog("QUERY　ERROR: " . $db->error);
                    }

                    $index = 0;
                    foreach ($hash as $contentID => $fieldVal)
                    {
                        if ($index % $step == 0)
                        {
                            $insert_query = "INSERT INTO $COLLECT_TABLE (`authID`,`devID`,`appID`,`productID`,`contentID`,`categoryID`,`contentType`,`creater`,`createTime`,`parentID`,`ext`) VALUES ";
                            $has_data = FALSE;
                        }

                        $fieldVal = json_decode($fieldVal, TRUE);

                        if (is_array($fieldVal) && $contentID == $fieldVal['contentID'])
                        {
                            $fieldVal = setDefaultVal($fieldVal, $COLLECT_TABLE);
                            $insert_query .= "({$fieldVal['authID']},{$fieldVal['devID']},{$fieldVal['appID']},{$fieldVal['productID']},{$fieldVal['contentID']},{$fieldVal['categoryID']},{$fieldVal['contentType']},{$fieldVal['creater']},{$fieldVal['createTime']},{$fieldVal['parentID']},{$fieldVal['ext']}),";
                            $has_data = TRUE;
                        }

                        if ($index % $step == ($step - 1) || $index + 1 == count($hash))
                        {
                            $insert_query = substr($insert_query, 0, -1);

                            if ($has_data)
                            {
                                mylog($insert_query);
                                $db->query($insert_query);
                                if ($db->errno)
                                {
                                    mylog("QUERY　ERROR: " . $db->error);
                                }
                            }
                        }

                        $index ++;
                    }

            }
        }
    }

    /* -------------------------用户收藏结束-------------------------- */

} while (FALSE);



if ($redis_is_connect)
{
    @$redis->close();
}
if ($mysql_is_connect)
{
    @$db->close();
}

if ($err_msg)
{
    mylog("-------END ERROR: " . $err_msg);
}
else
{
    mylog("-------END SUCCESS------");
}
