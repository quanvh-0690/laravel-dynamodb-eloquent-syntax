<?php

namespace QuanKim\DynamoDbEloquentSyntax;

use Illuminate\Support\Facades\App;

trait ModelTrait
{
    public static function boot()
    {
        parent::boot();

        $observer = static::getObserverClassName();

        static::observe(new $observer(
            App::make('QuanKim\DynamoDbEloquentSyntax\DynamoDbClientInterface')
        ));
    }

    public static function getObserverClassName()
    {
        return 'QuanKim\DynamoDbEloquentSyntax\ModelObserver';
    }

    public function getDynamoDbTableName()
    {
        return $this->getTableName();
    }
}
