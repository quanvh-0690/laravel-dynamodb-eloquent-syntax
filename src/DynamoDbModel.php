<?php

namespace QuanKim\DynamoDbEloquentSyntax;

use Exception;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\Log;

/**
 * Class DynamoDbModel.
 */
abstract class DynamoDbModel extends Model
{
    /**
     * Always set this to false since DynamoDb does not support incremental Id.
     *
     * @var bool
     */
    public $incrementing = false;

    /**
     * @var \QuanKim\DynamoDbEloquentSyntax\DynamoDbClientInterface
     */
    protected static $dynamoDb;

    /**
     * @var \Aws\DynamoDb\DynamoDbClient
     */
    protected $client;

    /**
     * @var \Aws\DynamoDb\Marshaler
     */
    protected $marshaler;

    /**
     * @var \QuanKim\DynamoDbEloquentSyntax\EmptyAttributeFilter
     */
    protected $attributeFilter;

    /**
     * Indexes.
     * [
     *     'global_index_key' => 'global_index_name',
     *     'local_index_key' => 'local_index_name',
     * ].
     *
     * @var array
     */
    protected $dynamoDbIndexKeys = [];


    /**
     * Array of your composite key.
     * ['hash', 'range']
     *
     * @var array
     */
    protected $compositeKey = [];

    protected $softDelete = false;

    public function __construct(array $attributes = [], DynamoDbClientService $dynamoDb = null)
    {
        $this->bootIfNotBooted();

        $this->syncOriginal();

        $this->fill($attributes);

        if (is_null(static::$dynamoDb)) {
            static::$dynamoDb = $dynamoDb;
        }

        $this->setupDynamoDb();
    }

    protected function setupDynamoDb()
    {
        if (is_null(static::$dynamoDb)) {
            static::$dynamoDb = app(\QuanKim\DynamoDbEloquentSyntax\DynamoDbClientInterface::class);
        }

        $this->client = static::$dynamoDb->getClient();
        $this->marshaler = static::$dynamoDb->getMarshaler();
        $this->attributeFilter = static::$dynamoDb->getAttributeFilter();
    }

    public function save(array $options = [])
    {
        if (!$this->getKey()) {
            if ($this->fireModelEvent('creating') === false) {
                return false;
            }
        }
        
        if ($this->fireModelEvent('saving') === false) {
            return false;
        }

        $saved = $this->newQuery()->save($options);

        if ($saved) {
            $this->fireModelEvent('saved', false);
        }

        return $saved;
    }

    public static function insert(array $values, $withId = true, $withTimestamp = true)
    {
        $model = new static;

        return $model->newQuery()->insert($values, $withId, $withTimestamp);
    }

    public function update(array $attributes = [], array $options = [])
    {
        return $this->fill($attributes)->save();
    }

    public static function create(array $attributes = [])
    {
        $model = new static;

        $model->fill($attributes)->save();

        return $model;
    }

    public function delete()
    {
        if (is_null($this->getKeyName())) {
            throw new Exception('No primary key defined on model.');
        }

        if ($this->exists) {
            if ($this->fireModelEvent('deleting') === false) {
                return false;
            }

            $this->exists = false;

            if ($this->softDelete) {
                $this->deleted_at = time();
                $success = $this->save();
            } else {
                $success = $this->newQuery()->delete();
            }

            if ($success) {
                $this->fireModelEvent('deleted', false);
            }

            return $success;
        }
    }

    public function restore()
    {
        if (is_null($this->getKeyName())) {
            throw new Exception('No primary key defined on model.');
        }
        if ($this->softDelete) {
            if ($this->fireModelEvent('restoring') === false) {
                return false;
            }

            $this->deleted_at = null;
            $success = $this->save();

            if ($success) {
                $this->fireModelEvent('restored', false);
            }

            return $success;
        }

        return false;
    }

    public function forceDelete()
    {
        $success = false;
        if (is_null($this->getKeyName())) {
            throw new Exception('No primary key defined on model.');
        }

        if ($this->exists) {
            if ($this->fireModelEvent('deleting') === false) {
                return false;
            }

            $this->exists = false;
            if ($this->softDelete) {
                $success = $this->newQuery()->delete();
            }

            if ($success) {
                $this->fireModelEvent('deleted', false);
            }
        }

        return $success;
    }

    public static function all($columns = [])
    {
        $instance = new static;

        return $instance->newQuery()->get($columns);
    }

    /**
     * @return DynamoDbQueryBuilder
     */
    public function newQuery()
    {
        $builder = new DynamoDbQueryBuilder();

        $builder->setModel($this);

        $builder->setClient($this->client);

        if ($this->softDelete) {
            $builder->where('deleted_at', null);
        }

        return $builder;
    }

    public function setUnfillableAttributes($attributes)
    {
        $keysToFill = array_diff(array_keys($attributes), $this->fillable);

        foreach ($keysToFill as $key) {
            $this->setAttribute($key, $attributes[$key]);
        }
    }

    public function hasCompositeKey()
    {
        return !empty($this->compositeKey);
    }

    public function marshalItem($item)
    {
        return $this->marshaler->marshalItem($item);
    }

    public function unmarshalItem($item)
    {
        return $this->marshaler->unmarshalItem($item);
    }

    public function setId($id)
    {
        if (!is_array($id)) {
            $this->setAttribute($this->getKeyName(), $id);

            return $this;
        }

        foreach ($id as $keyName => $value) {
            $this->setAttribute($keyName, $value);
        }

        return $this;
    }

    /**
     * @return \Aws\DynamoDb\DynamoDbClient
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * @param \Aws\DynamoDb\DynamoDbClient $client
     */
    public function setClient($client)
    {
        $this->client = $client;
    }

    /**
     * @return array
     */
    public function getCompositeKey()
    {
        return $this->compositeKey;
    }

    /**
     * @param array $compositeKey
     */
    public function setCompositeKey($compositeKey)
    {
        $this->compositeKey = $compositeKey;
    }

    /**
     * @return array
     */
    public function getDynamoDbIndexKeys()
    {
        return $this->dynamoDbIndexKeys;
    }

    /**
     * @param array $dynamoDbIndexKeys
     */
    public function setDynamoDbIndexKeys($dynamoDbIndexKeys)
    {
        $this->dynamoDbIndexKeys = $dynamoDbIndexKeys;
    }
    
    public function getSoftDelete()
    {
        return $this->softDelete;
    }
}
