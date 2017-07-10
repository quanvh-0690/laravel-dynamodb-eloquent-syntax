<?php

namespace QuanKim\DynamoDbEloquentSyntax;

use Illuminate\Support\Arr;
use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;
use Exception;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Support\Facades\Log;

class DynamoDbQueryBuilder
{
    /**
     * @var array
     */
    protected $where = [];

    /**
     * @var DynamoDbModel
     */
    protected $model;

    /**
     * @var DynamoDbClient
     */
    protected $client;

    /**
     * When not using the iterator, you can store the lastEvaluatedKey to
     * paginate through the results. The getAll method will take this into account
     * when used with $useIterator = false.
     *
     * @var mixed
     */
    protected $lastEvaluatedKey;

    protected $orderBy;

    protected $indexName = null;

    public function where($column, $operator = null, $value = null, $boolean = 'AND')
    {
        $model = $this->getModel();

        // If the column is an array, we will assume it is an array of key-value pairs
        // and can add them each as a where clause. We will maintain the boolean we
        // received when the method was called and pass it into the nested where.
        if (is_array($column)) {
            foreach ($column as $key => $value) {
                return $this->where($key, '=', $value);
            }
        }

        // Here we will make some assumptions about the operator. If only 2 values are
        // passed to the method, we will assume that the operator is an equals sign
        // and keep going. Otherwise, we'll require the operator to be passed in.
        if (func_num_args() == 2) {
            list($value, $operator) = [$operator, '='];
        }

        // If the columns is actually a Closure instance, we will assume the developer
        // wants to begin a nested where statement which is wrapped in parenthesis.
        // We'll add that Closure to the query then return back out immediately.
        if ($column instanceof \Closure) {
            $this->where['conditional_operator_' . array_last(array_keys($this->where))] = $boolean;
            $query = $this->model->newQuery();
            $column($query);
            unset($query->where['deleted_at']);
            $this->where[] = $query->where;

            return $this;
        }

        // If the given operator is not found in the list of valid operators we will
        // assume that the developer is just short-cutting the '=' operators and
        // we will set the operators to '=' and set the values appropriately.
        if (!ComparisonOperator::isValidOperator($operator)) {
            list($value, $operator) = [$operator, '='];
        }

        // If the value is a Closure, it means the developer is performing an entire
        // sub-select within the query and we will need to compile the sub-select
        // within the where clause to get the appropriate query record results.
        if ($value instanceof \Closure) {
            throw new NotSupportedException('Closure in where clause is not supported');
        }

        if ((strtolower($operator) == 'in' || strtolower($operator) == 'not_in') && is_array($value)) {
            $valueList = [];
            foreach ($value as $item) {
                $attributeValueList = $model->marshalItem([
                    'AttributeValueList' => $item,
                ]);
                $valueList[] = $attributeValueList['AttributeValueList'];
            }
            if (!$valueList) {
                $valueList = [
                    ['S' => 'NULL_DEFINE_IS_NULL'],
                ];
            }
        } else {
            $attributeValueList = $model->marshalItem([
                'AttributeValueList' => $value,
            ]);

            $valueList = [$attributeValueList['AttributeValueList']];

            if (strtolower($operator) === 'between') {
                $valueList = head($valueList)['L'];
            }
        }

        $this->where[$column] = [
            'AttributeValueList' => $valueList,
            'ComparisonOperator' => ComparisonOperator::getDynamoDbOperator($operator),
            'ConditionalOperator' => $boolean,
        ];

        return $this;
    }

    public function orWhere($column, $operator = null, $value = null)
    {
        return $this->where($column, $operator, $value, 'OR');
    }

    /**
     * Implements the Query Chunk method
     *
     * @param int $chunkSize
     * @param callable $callback
     */
    public function chunk($chunkSize, callable $callback)
    {
        while (true) {
            $results = $this->getAll([], $chunkSize, false);

            call_user_func($callback, $results);

            if (empty($this->lastEvaluatedKey)) {
                break;
            }
        }
    }

    public function find($id, array $columns = [], $withTrashed = false)
    {
        $model = $this->model;

        $model->setId($id);
        $key = $this->getDynamoDbKey();

        $query = [
            'ConsistentRead' => true,
            'TableName' => $model->getTable(),
            'Key' => $key,
        ];

        if (!empty($columns)) {
            $query['AttributesToGet'] = $columns;
        }

        $item = $this->client->getItem($query);
        $item = array_get($item->toArray(), 'Item');

        if (empty($item)) {
            return;
        }

        $item = $model->unmarshalItem($item);

        if (!$withTrashed && isset($item['deleted_at']) && $item['deleted_at']) {
            return;
        }

        if (!empty($columns)) {
            $item = array_only($item, $columns);
        }

        $model->fill($item);

        $model->setUnfillableAttributes($item);

        $model->exists = true;

        return $model;
    }

    public function first($columns = [])
    {
        $item = $this->getAll($columns, 1);

        return $item->first();
    }

    public function get($columns = [])
    {
        return $this->getAll($columns);
    }

    public function delete()
    {
        $key = $this->getDynamoDbKey();

        $query = [
            'TableName' => $this->model->getTable(),
            'Key' => $key,
        ];

        $result = $this->client->deleteItem($query);
        $status = array_get($result->toArray(), '@metadata.statusCode');

        return $status == 200;
    }

    public function save(array $options = [])
    {
        try {
            $attributes = $this->model->getAttributes();

            $attributes = Arr::except($attributes, 'old_updated_at');

            if (Arr::get($options, 'ignore_update_at', false)) {
                if (isset($attributes['updated_at']) && isset($attributes['old_updated_at'])) {
                    $attributes['updated_at'] = $attributes['old_updated_at'];
                }
            }

            $this->client->putItem([
                'TableName' => $this->model->getTable(),
                'Item' => $this->model->marshalItem($attributes),
            ]);

            return true;
        } catch (Exception $e) {
            Log::info($e);

            return false;
        }
    }

    public function insert($values, $withId = true, $withTimestamp = true)
    {
        try {
            $itemPutRequest = [];

            $timestamp = $withTimestamp ? [
                'created_at' => [
                    'N' => (string)time(),
                ],
                'updated_at' => [
                    'N' => (string)time(),
                ],
            ] : [];
            foreach ($values as $val) {
                $this->model->fill($val);

                $id = $withId ? ['id' => ['S' => (string)uniqid()]] : [];

                $item = array_merge(
                    $id,
                    $this->model->marshalItem($this->model->getAttributes()),
                    $timestamp
                );

                $itemPutRequest[] = [
                    'PutRequest' => [
                        'Item' => $item,
                    ],
                ];
            }

            $insertedValues = [
                'RequestItems' => [
                    $this->model->getTable() => $itemPutRequest,
                ],
            ];

            $this->client->batchWriteItem($insertedValues);

            return true;
        } catch (Exception $e) {
            Log::info($e);

            return false;
        }
    }

    public function all($columns = [])
    {
        return $this->getAll($columns);
    }

    public function count()
    {
        return $this->getAll([$this->model->getKeyName()])->count();
    }

    public function orderBy($direction = 'DESC')
    {
        $this->orderBy = $direction;

        return $this;
    }

    public function useIndex($indexName)
    {
        $this->indexName = $indexName;

        return $this;
    }

    public function totalCount()
    {
        $query = [
            'TableName' => $this->model->getTable(),
            'Select' => 'COUNT',
            'ReturnConsumedCapacity' => 'TOTAL',
        ];

        $op = $this->addConditionInQuery($query);

        if ($op == 'Scan') {
            $res = $this->client->scan($query);
        } else {
            $res = $this->client->query($query);
        }

        if ($count = array_get($res, 'Count')) {
            return $count;
        }

        return 0;
    }

    protected function getAll($columns = [], $limit = -1, $useIterator = true, $lastEvaluatedKey = null)
    {
        $query = [
            'TableName' => $this->model->getTable(),
        ];

        if ($limit > -1) {
            $query['Limit'] = $limit;
        }

        if ($lastEvaluatedKey) {
            $this->lastEvaluatedKey = $lastEvaluatedKey;
        }

        if (!empty($columns)) {
            $query['ProjectionExpression'] = implode(', ', $columns);
        }

        if (!empty($this->lastEvaluatedKey)) {
            $query['ExclusiveStartKey'] = $this->lastEvaluatedKey;
        }

        $op = $this->addConditionInQuery($query);

        if (config('app.debug')) {
            Log::debug($query);
        }

        if ($useIterator) {
            $iterator = $this->client->getIterator($op, $query);
        } else {
            if ($op == 'Scan') {
                $res = $this->client->scan($query);
            } else {
                $res = $this->client->query($query);
            }
            $this->lastEvaluatedKey = array_get($res, 'LastEvaluatedKey');
            $iterator = $res['Items'];
        }

        $results = [];

        foreach ($iterator as $item) {
            $item = $this->model->unmarshalItem($item);
            $model = $this->model->newInstance($item, true);
            $model->setUnfillableAttributes($item);
            $results[] = $model;
        }

        return new Collection($results);
    }

    protected function addConditionInQuery(&$query)
    {
        $op = 'Scan';
        // If the $where is not empty, we run getIterator.
        if (!empty($this->where)) {
            // Index key condition exists, then use Query instead of Scan.
            // However, Query only supports a few conditions.
            $where = $this->where;
            if ($index = $this->conditionsContainIndexKey($this->indexName)) {
                $op = 'Query';
                $query['IndexName'] = $index['name'];
                $keysInfo = $index['keysInfo'];
                if ($this->checkValidQueryDynamoDbOperator($keysInfo, 'hash')) {
                    $hashKeyConditions = array_get($this->where, $keysInfo['hash']);
                    $hashKeyConditions = array_except($hashKeyConditions, 'ConditionalOperator');
                    $query['KeyConditions'][$keysInfo['hash']] = $hashKeyConditions;
                }
                
                if ($this->checkValidQueryDynamoDbOperator($keysInfo, 'range')) {
                    $rangeKeyConditions = array_get($this->where, $keysInfo['range']);
                    $rangeKeyConditions = array_except($rangeKeyConditions, 'ConditionalOperator');
                    $query['KeyConditions'][$keysInfo['range']] = $rangeKeyConditions;
                    
                    if ($this->orderBy) {
                        $query['ScanIndexForward'] = strtolower($this->orderBy) == 'desc' ? false : true;
                    }
                }
                
                $where = array_except($this->where, array_values($keysInfo));
            }

            $expressionAttributeValues = [];
            $expressionAttributeNames = [];
            $filterExpression = '';
            $this->writeQuery($filterExpression, $expressionAttributeValues, $expressionAttributeNames, $where);
            if ($filterExpression) {
                $query['FilterExpression'] = $filterExpression;
                $query['ExpressionAttributeNames'] = $expressionAttributeNames;
            }

            if ($expressionAttributeValues) {
                $query['ExpressionAttributeValues'] = $expressionAttributeValues;
            }
        }

        return $op;
    }

    protected function writeQuery(&$filterExpression, &$expressionAttributeValues, &$expressionAttributeNames, $where, $conditionalOperator = false)
    {
        $expressions = '';
        if ($conditionalOperator) {
            $filterExpression .= $conditionalOperator;
        }

        $lastColumn = null;
        foreach ($where as $key => $value) {
            if (is_string($key) && is_array($value)) {
                $lastColumn = $key;
                $expression = $this->getExpression($expressionAttributeNames, $expressionAttributeValues, $key, $value);
                if ($expressions) {
                    $expressions .= ' ' . $value['ConditionalOperator'] . ' ' . $expression;
                } else {
                    $expressions .= $expression;
                }
            } elseif (is_numeric($key)) {
                $conditionalOperator = isset($where['conditional_operator_' . $lastColumn]) ? ' ' .  $where['conditional_operator_' . $lastColumn] : ' AND';
                $this->writeQuery($expressions, $expressionAttributeValues, $expressionAttributeNames, $value, $conditionalOperator);
            }
        }
        $filterExpression = ($filterExpression) ? $filterExpression .= ' (' . $expressions . ')' : $expressions;
    }

    protected function getExpression(&$expressionAttributeNames, &$expressionAttributeValues, $key, $value)
    {
        $expression = '';
        switch (strtolower($value['ComparisonOperator'])) {
            case 'in':
                if (count($value['AttributeValueList']) > 100) {
                    $inExpressionsArray = [];
                    foreach (array_chunk($value['AttributeValueList'], 99) as $valueList) {
                        $expressionPart = '#' . $key . ' ' . $value['ComparisonOperator'] . ' (';
                        $inExpression = [];
                        foreach ($valueList as $attributeValue) {
                            $prefix = uniqid();
                            $inExpression[] = ':' . $key . '_' . $prefix;
                            $expressionAttributeValues[':' . $key . '_' . $prefix] = $attributeValue;
                        }
                        $expressionPart .= implode(',', $inExpression) . ')';
                        $inExpressionsArray[] = $expressionPart;
                    }
                    $expression .= '(' . implode(' OR ', $inExpressionsArray) . ')';
                } else {
                    $expression .= '#' . $key . ' ' . $value['ComparisonOperator'] . ' (';
                    $inExpression = [];
                    foreach ($value['AttributeValueList'] as $attributeValue) {
                        $prefix = uniqid();
                        $inExpression[] = ':' . $key . '_' . $prefix;
                        $expressionAttributeValues[':' . $key . '_' . $prefix] = $attributeValue;
                    }
                    $expression .= implode(',', $inExpression) . ')';
                }
                $expressionAttributeNames['#' . $key] = $key;
                break;
            case 'contains':
                $expression .= 'contains(#' . $key . ', :' . $key . ')';
                $expressionAttributeValues[':' . $key] = $value['AttributeValueList'][0];
                $expressionAttributeNames['#' . $key] = $key;
                break;
            case 'begins_with':
                $expression .= 'begins_with(#' . $key . ', :' . $key . ')';
                $expressionAttributeValues[':' . $key] = $value['AttributeValueList'][0];
                $expressionAttributeNames['#' . $key] = $key;
                break;
            case 'ne':
                $expression = '#' . $key . ' <> :' . $key;
                $expressionAttributeValues[':' . $key] = $value['AttributeValueList'][0];
                $expressionAttributeNames['#' . $key] = $key;
                break;
            case 'not_in':
                if (count($value['AttributeValueList']) > 100) {
                    $inExpressionsArray = [];
                    foreach (array_chunk($value['AttributeValueList'], 99) as $valueList) {
                        $expressionPart = 'NOT (#' . $key . ' IN (';
                        $inExpression = [];
                        foreach ($valueList as $attributeValue) {
                            $prefix = uniqid();
                            $inExpression[] = ':' . $key . '_' . $prefix;
                            $expressionAttributeValues[':' . $key . '_' . $prefix] = $attributeValue;
                        }
                        $expressionPart .= implode(',', $inExpression) . '))';
                        $inExpressionsArray[] = $expressionPart;
                    }
                    $expression .= '(' . implode(' AND ', $inExpressionsArray) . ')';
                } else {
                    $expression .= 'NOT (#' . $key . ' IN (';
                    $inExpression = [];
                    foreach ($value['AttributeValueList'] as $attributeValue) {
                        $prefix = uniqid();
                        $inExpression[] = ':' . $key . '_' . $prefix;
                        $expressionAttributeValues[':' . $key . '_' . $prefix] = $attributeValue;
                    }
                    $expression .= implode(',', $inExpression) . '))';
                }

                $expressionAttributeNames['#' . $key] = $key;
                break;
            case 'attribute_exists':
                $expression .= 'attribute_exists(#' . $key . ')';
                $expressionAttributeNames['#' . $key] = $key;
                break;
            case 'attribute_not_exists':
                $expression .= 'attribute_not_exists(#' . $key . ')';
                $expressionAttributeNames['#' . $key] = $key;
                break;
            default:
                $operator = array_flip(ComparisonOperator::getOperatorMapping());
                $operator = $operator[$value['ComparisonOperator']];
                $expression = '#' . $key . ' ' . $operator . ' :' . $key;
                $expressionAttributeValues[':' . $key] = $value['AttributeValueList'][0];
                $expressionAttributeNames['#' . $key] = $key;
        }

        return $expression;
    }

    protected function checkValidQueryDynamoDbOperator($keysInfo, $keyType = 'hash')
    {
        if ($keyType == 'hash') {
            $hashKeyOperator = array_get($this->where, $keysInfo['hash'] . '.ComparisonOperator');
            
            return ComparisonOperator::isValidQueryDynamoDbOperator($hashKeyOperator);
        }
        
        $isCompositeKey = isset($keysInfo['range']);
        if ($isCompositeKey) {
            $rangeKeyOperator = array_get($this->where, $keysInfo['range'] . '.ComparisonOperator');
            
            return ComparisonOperator::isValidQueryDynamoDbOperator($rangeKeyOperator, true);
        }

        return false;
    }

    /**
     * Check if conditions "where" contain primary key or composite key.
     * For composite key, it will return false if the conditions don't have all composite key.
     *
     * @return array|bool the condition value
     */
    protected function conditionsContainKey()
    {
        if (empty($this->where)) {
            return false;
        }

        $conditionKeys = array_keys($this->where);

        $model = $this->model;

        $keys = $model->hasCompositeKey() ? $model->getCompositeKey() : [$model->getKeyName()];

        $conditionsContainKey = count(array_intersect($conditionKeys, $keys)) === count($keys);

        if (!$conditionsContainKey) {
            return false;
        }

        $conditionValue = [];

        foreach ($keys as $key) {
            $condition = $this->where[$key];

            $value = $model->unmarshalItem(array_get($condition, 'AttributeValueList'))[0];

            $conditionValue[$key] = $value;
        }

        return $conditionValue;
    }

    /**
     * Check if conditions "where" contain index key
     * For composite index key, it will return false if the conditions don't have all composite key.
     *
     * @return array|bool false or array ['name' => 'index_name', 'keysInfo' => ['hash' => 'hash_key', 'range' => 'range_key']]
     */
    protected function conditionsContainIndexKey($indexName = null)
    {
        if (empty($this->where)) {
            return false;
        }

        $indexKeys = $this->model->getDynamoDbIndexKeys();
        if ($indexName && array_key_exists($indexName, $indexKeys)) {
            return [
                'name' => $indexName,
                'keysInfo' => $indexKeys[$indexName],
            ];
        }

        foreach ($indexKeys as $name => $keysInfo) {
            $conditionKeys = array_keys($this->where);
            $keys = array_values($keysInfo);
            if (count(array_intersect($conditionKeys, $keys)) === count($keys)) {
                return [
                    'name' => $name,
                    'keysInfo' => $keysInfo,
                ];
            }
        }

        return false;
    }

    protected function getDynamoDbKey()
    {
        if (!$this->model->hasCompositeKey()) {
            return $this->getDynamoDbPrimaryKey();
        }

        $keys = [];

        foreach ($this->model->getCompositeKey() as $key) {
            $keys = array_merge($keys, $this->getSpecificDynamoDbKey($key, $this->model->getAttribute($key)));
        }

        return $keys;
    }

    protected function getDynamoDbPrimaryKey()
    {
        return $this->getSpecificDynamoDbKey($this->model->getKeyName(), $this->model->getKey());
    }

    protected function getSpecificDynamoDbKey($keyName, $value)
    {
        $idKey = $this->model->marshalItem([
            $keyName => $value,
        ]);

        $key = [
            $keyName => $idKey[$keyName],
        ];

        return $key;
    }

    /**
     * @return DynamoDbModel
     */
    public function getModel()
    {
        return $this->model;
    }

    /**
     * @param DynamoDbModel $model
     */
    public function setModel($model)
    {
        $this->model = $model;
    }

    /**
     * @return DynamoDbClient
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * @param DynamoDbClient $client
     */
    public function setClient($client)
    {
        $this->client = $client;
    }

    public function paginate($columns = [], $limit = 1, $lastEvaluatedKey = null)
    {
        $totalCount = $this->totalCount();

        $limitProcess =  (int)$limit;

        if ($lastEvaluatedKey) {
            $lastEvaluatedKey = json_decode($lastEvaluatedKey, true);
        }

        $results = new Collection();

        while ($limitProcess > 0 && $totalCount > 0) {
            $limitProcess = ($totalCount < $limitProcess) ? -1 : $limitProcess;

            $items = $this->getAll($columns, $limitProcess, false, $lastEvaluatedKey);

            if ($count = $items->count()) {
                $results = $results->merge($items);
                $limitProcess = $limitProcess - $count;
            }

            $lastEvaluatedKey = $this->lastEvaluatedKey;
            if (empty($lastEvaluatedKey)) {
                break;
            }
        }

        $result = [
            'total' => $totalCount,
            'totalPage' => ceil($totalCount / $limit),
            'lastEvaluatedKey' => $lastEvaluatedKey ? json_encode($lastEvaluatedKey) : '',
            'items' => $results,
        ];

        return $result;
    }

    /**
     * Increment a column's value by a given amount.
     *
     * @param  string  $column
     * @param  int     $amount
     * @param  array   $extra
     * @return int
     */
    public function increment($column, $amount = 1, array $extra = [])
    {
        if (! is_numeric($amount)) {
            throw new InvalidArgumentException('Non-numeric value passed to increment method.');
        }


        $key = $this->getDynamoDbKey();

        $query = [
            'TableName' => $this->model->getTable(),
            'Key' => $key,
            'UpdateExpression' => 'ADD ' . $column . ' :num',
            'ExpressionAttributeValues' => [
                ':num' => ['N' => (string)$amount],
            ],
            'ReturnValues' => 'NONE',
        ];

        $result = $this->client->updateItem($query);
        $status = array_get($result->toArray(), '@metadata.statusCode');

        return $status == 200;
    }

    /**
     * Decrement a column's value by a given amount.
     *
     * @param  string  $column
     * @param  int     $amount
     * @param  array   $extra
     * @return int
     */
    public function decrement($column, $amount = 1, array $extra = [])
    {
        $key = $this->getDynamoDbKey();

        $query = [
            'TableName' => $this->model->getTable(),
            'Key' => $key,
            'UpdateExpression' => 'ADD ' . $column . ' :num',
            'ExpressionAttributeValues' => [
                ':num' => ['N' => (string)(-$amount)],
            ],
            'ReturnValues' => 'NONE',
        ];

        $result = $this->client->updateItem($query);
        $status = array_get($result->toArray(), '@metadata.statusCode');

        return $status == 200;
    }

    public function withTrashed()
    {
        unset($this->where['deleted_at']);

        return $this;
    }

    public function onlyTrashed()
    {
        $this->where('deleted_at', '>', 1);

        return $this;
    }

    public function deleteAll()
    {
        if ($this->model->getSoftDelete()) {
            $models = $this->get();
            foreach ($models as $model) {
                $model->delete();
            }
        } else {
            $models = $this->get();
            $keys = $models->pluck('id');
            if ($this->model->hasCompositeKey()) {
                $compositeKey = $this->model->getCompositeKey();
                $keys = $models->pluck($compositeKey[0], $compositeKey[1]);
            }

            if ($models->count()) {
                foreach ($keys->chunk(25) as $chunk) {
                    $deleteRequests = [];
                    foreach ($chunk as $rangeKey => $hashKey) {
                        $key = isset($compositeKey) ? [$compositeKey[1] => $rangeKey, $compositeKey[0] => $hashKey] : ['id' => $hashKey];
                        $deleteRequests[] = [
                            'DeleteRequest' => [
                                'Key' => $this->model->marshalItem($key),
                            ],
                        ];
                    }

                    try {
                        $this->client->batchWriteItem([
                            'RequestItems' => [
                                $this->model->getTable() => $deleteRequests,
                            ],
                        ]);
                    } catch (DynamoDbException $e) {
                        \Log::error($e);
                    }
                }

                return true;
            }
        }

        return false;
    }
}
