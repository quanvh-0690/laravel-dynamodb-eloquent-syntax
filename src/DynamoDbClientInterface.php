<?php

namespace QuanKim\DynamoDbEloquentSyntax;

interface DynamoDbClientInterface
{
    public function getClient();

    public function getMarshaler();

    public function getAttributeFilter();
}
