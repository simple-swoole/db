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

use PDO;
use RuntimeException;
use Swoole\Coroutine;
use Swoole\Database\PDOStatementProxy;

class DB
{
    protected $pool;

    /** @var PDO */
    protected $pdo;

    private $in_transaction = false;

    public function __construct($config = null)
    {
        if (! empty($config)) {
            $this->pool = \Simps\DB\PDO::getInstance($config);
        } else {
            $this->pool = \Simps\DB\PDO::getInstance();
        }
    }

    public function quote(string $string, int $parameter_type = PDO::PARAM_STR)
    {
        $this->realGetConn();
        $ret = $this->pdo->quote($string, $parameter_type);
        $this->release($this->pdo);
        return $ret;
    }

    public function beginTransaction(): void
    {
        if ($this->in_transaction) { //嵌套事务
            throw new RuntimeException('do not support nested transaction now');
        }
        $this->realGetConn();
        $this->pdo->beginTransaction();
        $this->in_transaction = true;
        Coroutine::defer(function () {
            if ($this->in_transaction) {
                $this->rollBack();
            }
        });
    }

    public function commit(): void
    {
        $this->pdo->commit();
        $this->in_transaction = false;
        $this->release($this->pdo);
    }

    public function rollBack(): void
    {
        $this->pdo->rollBack();
        $this->in_transaction = false;
        $this->release($this->pdo);
    }

    public function query(string $query, array $bindings = []): array
    {
        $this->realGetConn();

        $statement = $this->pdo->prepare($query);

        $this->bindValues($statement, $bindings);

        $statement->execute();

        $ret = $statement->fetchAll();

        $this->release($this->pdo);

        return $ret;
    }

    public function fetch(string $query, array $bindings = [])
    {
        $records = $this->query($query, $bindings);

        return array_shift($records);
    }

    public function execute(string $query, array $bindings = []): int
    {
        $this->realGetConn();

        $statement = $this->pdo->prepare($query);

        $this->bindValues($statement, $bindings);

        $statement->execute();

        $ret = $statement->rowCount();

        $this->release($this->pdo);

        return $ret;
    }

    public function exec(string $sql): int
    {
        $this->realGetConn();

        $ret = $this->pdo->exec($sql);

        $this->release($this->pdo);

        return $ret;
    }

    public function insert(string $query, array $bindings = []): int
    {
        $this->realGetConn();

        $statement = $this->pdo->prepare($query);

        $this->bindValues($statement, $bindings);

        $statement->execute();

        $ret = (int) $this->pdo->lastInsertId();

        $this->release($this->pdo);

        return $ret;
    }

    public function release($connection = null)
    {
        if ($connection === null) {
            $this->in_transaction = false;
        }

        if (! $this->in_transaction) {
            $this->pool->close($connection);
            return true;
        }

        return false;
    }

    protected function bindValues(PDOStatementProxy $statement, array $bindings): void
    {
        foreach ($bindings as $key => $value) {
            $statement->bindValue(
                is_string($key) ? $key : $key + 1,
                $value,
                is_int($value) ? PDO::PARAM_INT : PDO::PARAM_STR
            );
        }
    }

    private function realGetConn()
    {
        if (! $this->in_transaction) {
            $this->pdo = $this->pool->getConnection();
        }
    }
}
