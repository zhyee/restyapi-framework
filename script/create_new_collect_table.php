<?php

class DbConf
{
    const host = '192.168.187.215';
    const port = 3306;
    const user = 'useraction';
    const password = 'tLmTUjlDEQI';
    const dbname = 'useraction';
    const table = 'ua_user_collect';
}

// 点播cms接口
define('ON_DEMAND_CMS_INTERFACE', 'http://192.168.45.66:30001/cps/service/cms/getContentInfos');

// 直播cms接口
define('LIVE_CMS_INTERFACE', 'http://192.168.45.63:10001/cps/service/cms/getLiveChannels');

if (!function_exists('array_column'))
{
    function array_column( array $input, $column_key, $index_key = NULL)
    {
        $res = array();

        foreach ($input as $row)
        {
            if ($index_key)
            {
                $res[$row[$index_key]] = $row[$column_key];
            }
            else
            {
                $res[] = $row[$column_key];
            }
        }

        return $res;
    }
}


$db = new mysqli(DbConf::host, DbConf::user, DbConf::password, DbConf::dbname, DbConf::port);

if ($db->connect_errno)
{
    die("连接数据库失败" . $db->connect_error);
}

$limit = 20;
$offset = 0;

$ch = curl_init();
curl_setopt($ch, CURLOPT_HEADER, 0);
curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);

do
{
    $hasData = FALSE;
    $onDemandIds = array();
    $liveIds = array();

    $sql = "SELECT DISTINCT contentID FROM " . DbConf::table . " LIMIT $offset,$limit";
    echo $sql . PHP_EOL;

    $res = $db->query($sql);

    if ($res instanceof mysqli_result)
    {
        while (($row = $res->fetch_assoc()) != FALSE)
        {
            if (strlen($row['contentID']) > 12)
            {
                $hasData = TRUE;
                $liveIds[] = $row['contentID'];
            }
            else
            {
                $hasData = TRUE;
                $onDemandIds[] = $row['contentID'];
            }
        }

        $res->free();

        if ($onDemandIds)
        {
            $ids = implode(',', $onDemandIds);
            $url = ON_DEMAND_CMS_INTERFACE . '?ids=' . $ids . '&plats=32&attrs=description,himgM8,himgM7,categoryId,contentId,productid,ppvid,length,contentType,parentId&showOnNewTysx=1';

            echo "点播request url: " . $url . PHP_EOL;

            curl_setopt($ch, CURLOPT_URL, $url);

            $response = curl_exec($ch);
            echo "response: " . $response . PHP_EOL;

            if ($response)
            {
                $response = json_decode($response, true);
                if ($response['status'] == 0) {
                    $info = $response['info'];
                    foreach ($info as $val)
                    {
                        $sql = "UPDATE " . DbConf::table . " SET `ext` = '" . $db->real_escape_string(json_encode($val)) . "' WHERE contentID = '{$val['contentId']}'";
                        echo $sql . PHP_EOL;
                        $db->query($sql);
                        if ($db->errno)
                        {
                            echo "ERROR: " . $db->error . PHP_EOL;
                        }
                    }
                }
            }
        }

        if ($liveIds)
        {
            $ids = implode(',', $liveIds);
            $url = LIVE_CMS_INTERFACE . '?liveids=' . $ids;

            echo '直播request url: ' . $url . PHP_EOL;

            curl_setopt($ch, CURLOPT_URL, $url);

            $response = curl_exec($ch);

            echo "response: " . $response . PHP_EOL;

            if ($response)
            {
                $response = json_decode($response, true);

                if ($response['status'] == 0)
                {
                    $info = $response['info'];

                    foreach ($info as $val)
                    {
                        $sql = "UPDATE " . DbConf::table . " SET `ext` = '" . $db->real_escape_string(json_encode($val)) . "' WHERE contentID = '{$val['liveid']}'";
                        echo $sql . PHP_EOL;
                        $db->query($sql);
                        if ($db->errno)
                        {
                            echo "ERROR: " . $db->error . PHP_EOL;
                        }
                    }

                }
            }

        }

    }

    $offset += $limit;


} while ($hasData);

@curl_close($ch);
@$db->close();