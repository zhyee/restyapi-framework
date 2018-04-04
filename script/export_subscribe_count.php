<?php

error_reporting(E_ERROR);
set_time_limit(0);
date_default_timezone_set('Asia/shanghai');

$redis_is_connect = FALSE;

$dirpath = '/opt/scripts/upload_subscribe_count/tmp/';
$err_msg = FALSE;

do {

    if (!is_writeable($dirpath))
    {
        $err_msg = "导出文件夹不可写: " .realpath($dirpath);
        break;
    }

    $export = $dirpath . 'subscribe_stock_' . date('Ymd', strtotime("-1 day")) . '.txt';

    $confFile = __DIR__ . "/../inc/Config.json";

    if (!file_exists($confFile)) {
        $err_msg = "配置文件不存在";
        break;
    }

    $confJson = file_get_contents($confFile);
    $conf = json_decode($confJson);


    /* redis */
    $redis = new Redis();
    if (!$redis->connect($conf->REDIS_HOST, $conf->REDIS_PORT)) {
        $err_msg = "redis连接失败";
        break;
    }

    $redis_is_connect = true;

    $pattern = $conf->SUBSCRIBE_COUNT_KEY_PREFIX . '*';

    $keys = $redis->keys($pattern);

    if ($keys)
    {
        $fp = @fopen($export, "w");

        if (!$fp)
        {
            $err_msg = "无法创建文件";
            break;
        }

        $values = $redis->mget($keys);

        foreach ($keys as $k => $key)
        {
            $key = str_replace($conf->SUBSCRIBE_COUNT_KEY_PREFIX, '', $key);

            fwrite($fp, $key . "|" . (int)$values[$k] . "\n");
        }

        @fclose($fp);
    }

} while (FALSE);


if ($redis_is_connect)
{
    $redis->close();
}

if ($err_msg)
{
    echo $err_msg . PHP_EOL;
}
else
{
    echo "export to file: " . realpath($export) . PHP_EOL;
}
