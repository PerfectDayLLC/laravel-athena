<?php

return [

    'credentials' => [
        'key' => env('ATHENA_AWS_ACCESS_KEY_ID'),
        'secret' => env('ATHENA_AWS_SECRET_ACCESS_KEY'),
    ],
    'region' => env('ATHENA_AWS_REGION', 'us-east-1'),
    'version' => 'latest',
    'database' => env('ATHENA_DB'),
    'prefix' => env('ATHENA_TABLE_PREFIX', ''),
    'bucket' => env('ATHENA_S3_BUCKET'),
    'output_folder' => env('ATHENA_OUTPUT_FOLDER'),
    's3output' => 's3://'.env('ATHENA_S3_BUCKET').'/'.env('ATHENA_OUTPUT_FOLDER'),

];
