<?php

namespace PerfectDayLlc\Athena;

use Illuminate\Support\ServiceProvider;

class AthenaServiceProvider extends ServiceProvider
{
    public function register()
    {
        $this->mergeConfigFrom(__DIR__.'/../config/athena.php', 'perfectdayllc.athena');

        $this->app->resolving('db', function ($db) {
            $db->extend('athena', fn ($config) => new Connection);
        });
    }

    public function boot()
    {
        $this->publishes(
            [__DIR__.'/../config/athena.php' => config_path('perfectdayllc/athena.php')],
            'laravel-athena-config'
        );
    }
}
