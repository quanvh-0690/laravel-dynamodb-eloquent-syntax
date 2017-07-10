<?php

namespace QuanKim\DynamoDbEloquentSyntax;

use Aws\DynamoDb\Marshaler;
use Illuminate\Support\Facades\App;
use Illuminate\Support\ServiceProvider;

class DynamoDbServiceProvider extends ServiceProvider
{
    /**
     * Register the service provider.
     */
    public function register()
    {
        $marshalerOptions = [
            'nullify_invalid' => true,
        ];

        if ($this->app->environment() == 'testing' || config('aws.endpoint')) {
            return $this->bindForTesting($marshalerOptions);
        }

        $this->bindForApp($marshalerOptions);
    }

    protected function bindForApp($marshalerOptions = [])
    {
        $this->app->singleton('QuanKim\DynamoDbEloquentSyntax\DynamoDbClientInterface', function ($app) use ($marshalerOptions) {
            $config = [
                'credentials' => config('aws.credentials'),
                'region' => config('aws.region'),
                'version' => config('aws.version'),
                'http' => [
                    'verify' => config('aws.http.verify'),
                ],
            ];
            $client = new DynamoDbClientService($config, new Marshaler($marshalerOptions), new EmptyAttributeFilter());

            return $client;
        });
    }

    protected function bindForTesting($marshalerOptions = [])
    {
        $this->app->singleton('QuanKim\DynamoDbEloquentSyntax\DynamoDbClientInterface', function ($app) use ($marshalerOptions) {
            $config = [
                'credentials' => config('aws.credentials'),
                'region' => config('aws.region'),
                'version' => config('aws.version'),
                'endpoint' => config('aws.endpoint'),
            ];
            $client = new DynamoDbClientService($config, new Marshaler($marshalerOptions), new EmptyAttributeFilter());

            return $client;
        });
    }
}
