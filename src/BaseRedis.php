<?php

declare(strict_types=1);
/**
 * This file is part of Simps.
 *
 * @link     https://simps.io
 * @document https://doc.simps.io
 * @license  https://github.com/simple-swoole/simps/blob/master/LICENSE
 */
namespace Simps\DB;

class BaseRedis
{
    protected $pool;

    protected $connection;

    public function __construct($config = null)
    {
        if (! empty($config)) {
            $this->pool = Redis::getInstance($config);
        } else {
            $this->pool = Redis::getInstance();
        }
    }

    public function __call($name, $arguments)
    {
        $this->connection = $this->pool->getConnection();

        $data = $this->connection->{$name}(...$arguments);

        $this->pool->close($this->connection);

        return $data;
    }

    public function brPop($keys, $timeout)
    {
        $this->connection = $this->pool->getConnection();

        if ($timeout === 0) {
            // TODO Need to optimize...
            $timeout = 99999999999;
        }

        $this->connection->setOption(\Redis::OPT_READ_TIMEOUT, $timeout);

        try {
            $start = time();
            $data = $this->connection->blPop($keys, $timeout);
        } catch (\RedisException $e) {
            $end = time();
            if ($end - $start < $timeout) {
                throw $e;
            }
            return [];
        }

        $this->connection->setOption(\Redis::OPT_READ_TIMEOUT, $this->pool->getConfig()['time_out']);

        $this->pool->close($this->connection);

        return $data;
    }

    public function blPop($keys, $timeout)
    {
        $this->connection = $this->pool->getConnection();

        if ($timeout === 0) {
            // TODO Need to optimize...
            $timeout = 99999999999;
        }

        $this->connection->setOption(\Redis::OPT_READ_TIMEOUT, $timeout);

        try {
            $start = time();
            $data = $this->connection->blPop($keys, $timeout);
        } catch (\RedisException $e) {
            $end = time();
            if ($end - $start < $timeout) {
                throw $e;
            }
            return [];
        }

        $this->connection->setOption(\Redis::OPT_READ_TIMEOUT, $this->pool->getConfig()['time_out']);

        $this->pool->close($this->connection);

        return $data;
    }
}
