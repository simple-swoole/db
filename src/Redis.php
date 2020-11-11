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

use RuntimeException;
use Swoole\Database\RedisConfig;
use Swoole\Database\RedisPool;

class Redis
{
    protected $pools;

    protected $config = [
        'host' => 'localhost',
        'port' => 6379,
        'auth' => '',
        'db_index' => 0,
        'time_out' => 1,
        'size' => 64,
    ];

    private static $instance;

    private function __construct(array $config)
    {
        if (empty($this->pools)) {
            $this->config = array_replace_recursive($this->config, $config);
            $this->pools = new RedisPool(
                (new RedisConfig())
                    ->withHost($this->config['host'])
                    ->withPort($this->config['port'])
                    ->withAuth($this->config['auth'])
                    ->withDbIndex($this->config['db_index'])
                    ->withTimeout($this->config['time_out']),
                $this->config['size']
            );
        }
    }

    public static function getInstance($config = null)
    {
        if (empty(self::$instance)) {
            if (empty($config)) {
                throw new RuntimeException('redis config empty');
            }
            if (empty($config['size'])) {
                throw new RuntimeException('the size of redis connection pools cannot be empty');
            }
            self::$instance = new static($config);

            try {
                $ping = self::$instance->getConnection()->ping();
            } catch (\ErrorException $e) {
                throw new RuntimeException('redis Not working, please check redis service');
            }
            if ($ping != '+PONG') {
                throw new RuntimeException('redis Not working, please check redis service');
            }
        }else{
            $config = self::$instance->getConfig();
            try {
                $ping = self::$instance->getConnection()->ping();
            } catch (\ErrorException $e) {
                self::$instance = null;
                self::$instance = self::getInstance($config);
                $ping = self::$instance->getConnection()->ping();
            }
            if ($ping != '+PONG') {
                self::$instance = null;
                self::$instance = self::getInstance($config);
            }
        }

        return self::$instance;
    }

    public function getConnection()
    {
        return $this->pools->get();
    }

    public function close($connection = null)
    {
        $this->pools->put($connection);
    }

    public function getConfig(): array
    {
        return $this->config;
    }
}
