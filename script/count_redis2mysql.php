<?php
/**
 * 计数服务数据持久化脚本
 * Created by PhpStorm.
 * User: zy
 * Datetime: 2016/9/12 9:12
 * File: redis2mysql.20160919.php
 *
 * 定期把redis缓存数据更新到mysql数据库，可以使用crontab实现周期任务
 * eg: 0 * * * * /usr/local/php/bin/php /project/Redis2Mysql/redis2mysql.php
 * {$logpath} 配置日志文件存放路径，默认放在 /var/log/redis2mysql.log
 */

/**
 * 配置
 * Class Config
 */
class Config
{
    /**
     * mysql服务器地址
     */
    const MYSQL_HOST = '172.16.37.54';

    /**
     * mysql数据库用户名
     */
    const MYSQL_USER = 'countService';

    /**
     * mysql密码
     */
    const MYSQL_PASSWORD = 'o2RVyHRl#0t26r-U';

    /**
     * mysql数据库名称
     */
    const MYSQL_DBNAME = 'countService';

    /**
     * mysql服务器端口
     */
    const MYSQL_PORT = '3306';

    /**
     * redis服务器地址
     */
    const REDIS_HOST = '172.16.42.88';

    /**
     * redis端口
     */
    const REDIS_PORT = '6379';

    /**
     * 单次处理数据条数
     */
    const STEP = 100;

    /**
     * 日志文件存放路径
     */
    const logPath = '/var/log/redis2mysql.log';

    /**
     * 需要处理的redis key前缀 以及 对应的表、字段以及查询主键
     * @var array
     */
    public static $redisKeyPrefixs = [
        'topcount_' => ['table' => 'CountProgram', 'field' => 'TopCount', 'primaryKey' => 'ContentId'],
        'downcount_' => ['table' => 'CountProgram', 'field' => 'DownCount', 'primaryKey' => 'ContentId'],
        'playcount_' => ['table' => 'PlayCount', 'field' => 'Count', 'primaryKey' => 'ContentId']
    ];
}

/**
 * 日志类
 * Class Logger
 */
class Logger
{

    private $logFile = '';

    public function __construct()
    {
		$dir = dirname(Config::logPath);
        if (!is_dir($dir))
        {
            if (!mkdir($dir, 0755, TRUE))
            {
                throw new Exception('创建文件夹失败: ' . $dir);
            }
        }
        $this->logFile = Config::logPath;
    }

    /**
     * 记录日志
     * @param $text
     * @return int
     */
    public function log($text)
    {
        $text = '[' . date('Y-m-d H:i:s') . ']   ' . $text . PHP_EOL;
        return file_put_contents($this->logFile, $text, FILE_APPEND);
    }

    /**
     * 换行
     */
    public function newLine()
    {
        return file_put_contents($this->logFile, PHP_EOL, FILE_APPEND);
    }
}


$logger = new Logger();

$logger->log('redis2mysql start ...');

try
{

    $redisHandler = new Redis();
    if (!$redisHandler->connect(Config::REDIS_HOST, Config::REDIS_PORT))
    {
        throw new Exception("连接redis服务器失败 HOST: " . Config::REDIS_HOST . " PORT: " . Config::REDIS_PORT);
    }

    $logger->log('redis 连接成功 ...');

    $mysqlHandler = new mysqli(Config::MYSQL_HOST, Config::MYSQL_USER, Config::MYSQL_PASSWORD, Config::MYSQL_DBNAME, Config::MYSQL_PORT);

    if ($mysqlHandler->connect_errno)
    {
        throw new Exception("连接mysql服务器失败: " . $mysqlHandler->connect_error);
    }

    $logger->log('mysql 连接成功 ...');

    $sum = 0; //计数

    foreach (Config::$redisKeyPrefixs as $key => $val)
    {
        $table = $val['table'];
        $field = $val['field'];
        $primaryKey = $val['primaryKey'];

        $redisKeys = $redisHandler->keys($key . '*');
        $redisKeysArr = array_chunk($redisKeys, Config::STEP);

        foreach ($redisKeysArr as $redisKeysSubArr)
        {
            $redisValues = $redisHandler->mget($redisKeysSubArr);
            $redisKeysValues = array_combine($redisKeysSubArr, $redisValues);

            $primaryKeys = array_map(function($val) use($key) {
                return "'" . trim(str_replace($key, '', $val)) . "'";
            }, $redisKeysSubArr);

            /* 数据库已经存在的pid */
            $existsPrimaryKeys = [];

            /* 数据库当前的计数 */
            $existsCounts = [];

            if ($primaryKeys)
            {
                $sql = "SELECT {$primaryKey},{$field} FROM {$table} WHERE {$primaryKey} IN (" . implode(',', $primaryKeys) . ")";

		$logger->log($sql);

                $mysqlResult = $mysqlHandler->query($sql);
                if ($mysqlResult instanceof mysqli_result)
                {
                    while ($row = $mysqlResult->fetch_assoc())
                    {
			$pk = trim($row[$primaryKey]);
                        $existsPrimaryKeys[] = $pk;
                        $existsCounts[$pk] = (int)$row[$field];
                    }
                    $mysqlResult->free();
                }
                elseif (FALSE === $mysqlResult)
                {
                    throw new Exception("数据库查询失败，sql：" . $sql . " error：" . $mysqlHandler->error);
                }

            }


            foreach ($redisKeysValues as $redisKey => $redisValue)
            {
                $redisValue = (int)$redisValue;
                $primaryKeyValue = trim(str_replace($key, '', $redisKey));  // 主键pid

                if (in_array($primaryKeyValue, $existsPrimaryKeys))
                {
                    /* 只有当缓存的数值大于数据库中的值才做更新 */
                    if ($redisValue > $existsCounts[$primaryKeyValue])
                    {
                        $sql = "UPDATE {$table} SET {$field} = {$redisValue} WHERE {$primaryKey} = '{$primaryKeyValue}'";
                        if ($mysqlHandler->query($sql) === FALSE)
                        {
                            throw new Exception("更新数据库失败，sql：" . $sql . ' error：' . $mysqlHandler->error);
                        }
                        else
                        {
                            $logger->log("更新数据库，{$table}[{$primaryKey}={$primaryKeyValue}].{$field}: {$existsCounts[$primaryKeyValue]} -> {$redisValue}");
                        }
                    }
                }
                else
                {
                    /* 没有数据则尝试插入新数据 */
                    if ($table == 'CountProgram')
                    {
                        $TopCount = $DownCount = 0;
                        $$field = $redisValue;
                        $sql = "INSERT INTO CountProgram (ContentId, mbContentId, TopCount, DownCount, Plat, UpdateTime, random) VALUES ('{$primaryKeyValue}', NULL, {$TopCount}, {$DownCount}, NULL, now(), NULL)";
                    }
                    elseif ($table == 'PlayCount')
                    {
                        $sql = "INSERT INTO PlayCount (ContentId, mbContentId, `Count`, Plat, UpdateTime) VALUES ('{$primaryKeyValue}', NULL, $redisValue, 1, now())";
                    }

                    if ($mysqlHandler->query($sql) === FALSE)
                    {
                        throw new Exception("插入新纪录失败，sql：" . $sql . ' error：' . $mysqlHandler->error);
                    }
                    else
                    {
                        $logger->log("添加新纪录，sql: " . $sql);
                    }
                }
            }

            $sum += count($redisKeysSubArr);
        }
        $logger->log('当前已处理数据：' . $sum . '条...');
    }

    $logger->log('数据更新完毕．．．');

}
catch (Exception $e)
{
    $logger->log('数据更新过程中出错: ' . $e->getMessage());
    throw $e;
}







