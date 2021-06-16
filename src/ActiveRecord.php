<?php

declare(strict_types=1);

namespace Simps\DB;

/**
 * Class ActiveRecord
 * @package App\Model
 */
abstract class ActiveRecord
{
    /**
     * @var bool
     */
    private $_isNewRecord = true;

    /**
     * @var array
     */
    private $_columns;

    /**
     * @var array|null
     */
    private $_oldAttributes;

    /**
     * ActiveRecord constructor.
     */
    public function __construct()
    {
        $this->findTableColumns();
    }

    /**
     * getIsNewRecord
     * @return bool
     */
    public function getIsNewRecord()
    {
        return $this->_isNewRecord;
    }

    /**
     * getColumns
     * @return array
     */
    public function getColumns()
    {
        return $this->_columns;
    }

    /**
     * getOldAttributes
     * @param null $column
     * @return array|mixed|null
     */
    public function getOldAttributes($column=null)
    {
        return $column?($this->_oldAttributes[$column]??null):$this->_oldAttributes;
    }

    /**
     * findTableColumns
     */
    protected function findTableColumns()
    {
        foreach (static::getTableSchema() as $column) {

            $field = $column['Field'];

            $this->{$field} = $column['Default'];

            $this->_columns[$field] = $column;
        }
    }

    /**
     * tableName
     * @return string
     */
    abstract public static function tableName(): string;

    /**
     * getDb
     * @return BaseModel
     */
    public static function getDb()
    {
        return new BaseModel();
    }

    /**
     * getTableSchema
     * @return array
     */
    public static function getTableSchema()
    {
        static $_columns;
        if(empty($_columns)){
            $_columns = static::getDb()->query('SHOW FULL COLUMNS FROM ' . static::tableName())->fetchAll();
        }
        return $_columns;
    }

    /**
     * 主键
     * @var null
     */
    private static $_primaryKey = null;

    /**
     * 主键必须设置
     * getPrimaryKey
     * @return array
     */
    public static function getPrimaryKey()
    {
        if(empty(self::$_primaryKey)){
            foreach (static::getTableSchema() as $column) {
                if ($column['Key'] == 'PRI')
                    self::$_primaryKey[] = $column['Field'];
            }
        }
        return self::$_primaryKey;
    }

    /**
     * toArray
     * @return array
     */
    public function toArray()
    {
        $data = [];

        foreach ($this->getColumns() as $field => $column) {
            $data[$field] = $this->{$field};
        }

        return $data;
    }

    /**
     * createModelsByDataBase
     * @param array $rows
     * @return $this
     */
    protected function createModelsByDataBase(array $rows)
    {

        foreach ($rows as $column => $value) {
            $this->{$column}               = $value;
            $this->_oldAttributes[$column] = $value;
        }

        $this->_isNewRecord = false;

        return $this;
    }

    /**
     * getChangeAttributes
     * @return array
     */
    public function getChangeAttributes()
    {
        $changeAttributes = [];
        foreach ($this->getOldAttributes() as $column => $oldValue) {
            if ($this->{$column} !== $oldValue) {
                $changeAttributes[$column] = $this->{$column};
            }
        }
        return $changeAttributes;
    }

    /**
     * getPrimaryKeyCondition
     * @return array
     */
    protected function getPrimaryKeyCondition()
    {
        $condition = [];

        foreach (self::getPrimaryKey() as $primaryKey) {
            $condition[$primaryKey] = $this->getOldAttributes($primaryKey);
        }

        return $condition;
    }

    /**
     * save
     * @return bool
     */
    public function save()
    {
        return $this->getIsNewRecord() ? $this->insert() : $this->update();
    }

    /**
     * update
     * @return bool
     */
    public function update()
    {
        $changeAttributes = $this->getChangeAttributes();
        if(!empty($changeAttributes)){
            return static::getDb()->update(static::tableName(), $changeAttributes, $this->getPrimaryKeyCondition())->rowCount();
        }
        return true;
    }

    /**
     * insert
     * @return bool
     */
    public function insert()
    {
        $insertId = static::getDb()->insert(static::tableName(), $this->toArray());

        $primaryKey = self::getPrimaryKey()[0]??null;

        if($primaryKey && isset($this->{$primaryKey}))
            $this->{$primaryKey} = $insertId;

        return $insertId?true:false;
    }

    /**
     * delete
     * @return bool
     */
    public function delete()
    {
        return static::getDb()->delete(static::tableName(), $this->getPrimaryKeyCondition())->rowCount()?true:false;
    }

    /**
     * formatWhere
     * @param array|string $where
     * @return array
     */
    protected static function formatWhere($where)
    {
        if(is_string($where)){
            return [self::getPrimaryKey()[0]=>$where];
        }
        return $where;
    }

    /**
     * findOrCreate
     * @param array|string $where
     * @return ActiveRecord|static
     */
    public static function findOrCreate($where)
    {
        $where = self::formatWhere($where);

        $data = null;

        if(!empty($where))
            $data  = static::getDb()->get(static::tableName(), '*', self::formatWhere($where));

        $model = (new static());
        if (!empty($data)) {
            return $model->createModelsByDataBase($data);
        }

        return $model;
    }

    /**
     * findOne
     * @param array|string $where
     * @return static
     */
    public static function findOne($where)
    {
        $where = self::formatWhere($where);

        if(empty($where))
            return null;

        $data = static::getDb()->get(static::tableName(), '*', $where);
        return $data ? (new static())->createModelsByDataBase($data) : null;
    }

    /**
     * findAll
     * @param array $where
     * @return static[]|array
     */
    public static function findAll($where = [])
    {
        $dataList = static::getDb()->select(static::tableName(), '*', $where);

        if (empty($dataList))
            return [];

        $models = [];

        foreach ($dataList as $data) {
            $models[] = (new static())->createModelsByDataBase($data);
        }

        return $models;
    }

    /**
     * insertOne
     * @param array $data
     * @return bool
     */
    public static function insertOne(array $data)
    {
        return static::getDb()->insert(static::tableName(), [$data])->rowCount();
    }

    /**
     * insertMulti
     * @param array $data
     * @return bool
     */
    public static function insertMulti(array $data)
    {
        return static::getDb()->insert(static::tableName(), $data)->rowCount();
    }

    /**
     * updateAll
     * @param array $data
     * @param array $where
     * @return bool
     */
    public static function updateAll(array $data, $where = [])
    {
        return static::getDb()->update(static::tableName(), $data, $where)->rowCount();
    }

    /**
     * deleteAll
     * @param null $where
     * @return bool
     */
    public static function deleteAll($where = null)
    {
        return static::getDb()->delete(static::tableName(), $where)->rowCount()?true:false;
    }

    /**
     * load
     * @param array $data
     */
    public function load(array $data)
    {
        foreach ($data as $key => $value) {

            if (!isset($this->{$key}))
                continue;

            $this->{$key} = $value;
        }
    }

}
