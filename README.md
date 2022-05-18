# Laravel Athena
Laravel Athena database driver

## Compatibility
Laravel 5.8 + PHP 7.4

## Installation Steps
1. Add this repository to composer:
```javascript
"repositories": [
    {
        "type": "vcs",
        "url": "https://github.com/PerfectDayLLC/laravel-athena"
    },
]
```
2. `composer require perfectdayllc/laravel-athena`
3. `php artisan vendor:publish --tag=laravel-athena-config` to publish config file.
4. Open `config/database.php` and add new connection as specified below.
```php
'connections' => [
    // ...
    'athena' => [
        'driver' => 'athena',
    ],
],
```
