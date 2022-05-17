<?php

namespace PerfectDayLlc\Athena;

use Illuminate\Support\ServiceProvider;

class AthenaServiceProvider extends ServiceProvider
{
    public function register()
    {
        $this->app->resolving('db', function ($db) {
            $db->extend('athena', fn ($config) => new Connection($config));
        });
    }

    public function boot()
    {
        $this->publishes([
            __DIR__.'/../config/athena.php' => config_path('athena.php'),
        ], 'laravel-athena-config');

        Model::setConnectionResolver($this->app['db']);
        Model::setEventDispatcher($this->app['events']);
    }
}
