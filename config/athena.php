<?php

return [

    'credentials' => [
        'key' => env('AWS_ACCESS_KEY_ID', ''),
        'secret' => env('AWS_SECRET_ACCESS_KEY', ''),
    ],
    'region' => env('AWS_REGION', 'us-east-1'),
    'version' => 'latest',
    'database' => env('ATHENA_DB'),
    'prefix' => env('ATHENA_TABLE_PREFIX', ''),
    'bucket' => env('S3_BUCKET'),
    'output_folder' => env('ATHENA_OUTPUT_FOLDER'),
    's3output' => 's3://'.env('S3_BUCKET').'/'.env('ATHENA_OUTPUT_FOLDER'),

];
