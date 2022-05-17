<?php

namespace PerfectDayLlc\Athena;

use Aws\Athena\AthenaClient;
use Aws\S3\S3Client;
use Exception;
use Goodby\CSV\Import\Standard\Interpreter;
use Goodby\CSV\Import\Standard\Lexer;
use Goodby\CSV\Import\Standard\LexerConfig;
use Illuminate\Database\PostgresConnection;
use Illuminate\Database\Schema\Builder as BaseBuilder;
use Illuminate\Database\Schema\Grammars\MySqlGrammar;
use Illuminate\Support\Arr;
use League\Flysystem\AwsS3v3\AwsS3Adapter;
use League\Flysystem\Filesystem;
use League\Flysystem\FilesystemInterface;
use PerfectDayLlc\Athena\Query\Grammar;

class Connection extends PostgresConnection
{
    protected ?AthenaClient $athenaClient = null;

    protected ?S3Client $s3Client = null;

    private ?string $localFilePath = null;

    public function __construct()
    {
        parent::__construct(
            null,
            $this->config['database'],
            $this->config['prefix'] ?? '',
            config('athena')
        );

        $this->prepareAthenaClient();
        $this->prepareS3Client();
    }

    private function prepareAthenaClient(): void
    {
        if ($this->athenaClient) {
            return;
        }

        $this->athenaClient = new AthenaClient([
            'version' => 'latest',
            'region' => $this->config['region'],
            'credentials' => $this->config['credentials']
        ]);
    }

    public function prepareS3client(): void
    {
        if ($this->s3Client) {
            return;
        }

        $this->s3Client = new S3Client([
            'credentials' => $this->config['credentials'],
            'region' => $this->config['region'],
            'version' => $this->config['version'],
        ]);
    }

    public function getSchemaBuilder()
    {
        if (is_null($this->schemaGrammar)) {
            $this->useDefaultSchemaGrammar();
        }

        return new BaseBuilder($this);
    }

    protected function getDefaultSchemaGrammar()
    {
        return new MySqlGrammar;
    }

    public function getDefaultQueryGrammar()
    {
        return $this->withTablePrefix(new Grammar);
    }

    /**
     * Return local file path downloaded from S3
     */
    public function getDownloadedFilePath(): ?string
    {
        return $this->localFilePath;
    }

    /**
     * @throws Exception
     */
    protected function executeQuery(string &$query, array $bindings): array
    {
        $query = $this->prepareQuery($query, $bindings);

        $param_Query = [
            'QueryString' => $query,
            'QueryExecutionContext' => ['Database' => $this->config['database']],
            'ResultConfiguration' => [
                'OutputLocation' => $this->config['s3output']
            ],
            'WorkGroup' => $this->config['work_group']
        ];

        $response = $this->athenaClient->startQueryExecution($param_Query);

        if (! $response) {
            throw new Exception('Got an error while running athena query');
        }

        $executionResponse = [];
        $queryStatus = 'None';
        while ($queryStatus === 'None' || $queryStatus === 'RUNNING' || $queryStatus === 'QUEUED') {
            $executionResponse = $this->athenaClient->getQueryExecution(
                ['QueryExecutionId' => $response['QueryExecutionId']]
            )
                ->toArray();

            $queryStatus = $executionResponse['QueryExecution']['Status']['State'];

            if ($queryStatus === 'FAILED' || $queryStatus === 'CANCELLED') {
                $stateChangeReason = @$executionResponse['QueryExecution']['Status']['StateChangeReason'];

                if (stripos($stateChangeReason, 'Partition already exists') === false) {
                    throw new Exception("Athena Query Error [$queryStatus]: $stateChangeReason");
                }
            } elseif ($queryStatus === 'RUNNING' || $queryStatus === 'QUEUED') {
                sleep(1);
            }
        }

        return $executionResponse;
    }

    /**
     * @throws Exception
     */
    protected function prepareQuery(string $query, array $binding): string
    {
        if (count($binding) > 0) {
            foreach ($binding as $oneBind) {
                $from = '/'.preg_quote('?', '/').'/';
                $to = is_numeric($oneBind) ? $oneBind : "'$oneBind'";

                $query = preg_replace($from, $to, $query, 1);
            }
        }

        // Modifying query & preparing it for LIMIT as per Athena
        if (stripos($query, 'BETWEENLIMIT') !== false) {
            // Checking if ROW_NUMBER() OVER() window function applied, then take it as LIMIT query
            if (stripos($query, 'ROW_NUMBER()') === false || stripos($query, ' rn ') === false) {
                throw new Exception('Error: Required `ROW_NUMBER() OVER(...) as rn` to implement LIMIT functionality');
            }

            $queryParts = preg_split('/BETWEENLIMIT/i', $query);

            preg_match_all('!\d+!', array_pop($queryParts), $matches);

            // Calculating offset and limit for Athena
            $perPage = $matches[0][0];

            // Only apply this limit if we have per page greater than 0.
            // This prevents BETWEEN as WHERE clause to be treated like LIMIT & OFFSET, which occurs if we have
            // both BETWEEN and ROW_NUMBER() in query but that no LIMIT
            if ($perPage > 0) {
                $page = ($matches[0][1] / $perPage) + 1;

                $from = ($perPage * ($page - 1)) + 1;
                $to = ($perPage * $page);

                $query = "SELECT * FROM (".Arr::first($queryParts).") WHERE rn BETWEEN $from AND $to";
            }
        }

        return str_replace('`', '', $query);
    }

    /**
     * @throws Exception
     */
    public function statement($query, $bindings = []): bool
    {
        if ($this->pretending()) {
            return true;
        }

        $start = microtime(true);

        $this->executeQuery($query, $bindings);

        $this->logQuery($query, [], $this->getElapsedTime($start));

        return true;
    }

    /**
     * @throws Exception
     */
    public function export(string $query, array $bindings = []): string
    {
        if ($this->pretending()) {
            return '';
        }

        $s3FilePath = '';

        $start = microtime(true);
        if ($executionResponse = $this->executeQuery($query, $bindings)) {
            $S3OutputLocation = $executionResponse['QueryExecution']['ResultConfiguration']['OutputLocation'];
            $s3FilePath = '/'.$this->config['output_folder'].'/'.basename($S3OutputLocation);
        }

        $this->logQuery($query, [], $this->getElapsedTime($start));

        return $s3FilePath;
    }

    /**
     * @throws Exception
     */
    public function select($query, $bindings = [], $useReadPdo = true): array
    {
        if ($this->pretending()) {
            return [];
        }

        $result = [];
        $start = microtime(true);
        if ($executionResponse = $this->executeQuery($query, $bindings)) {
            $S3OutputLocation = $executionResponse['QueryExecution']['ResultConfiguration']['OutputLocation'];
            $s3FilePath = '/'.$this->config['output_folder'].'/'.basename($S3OutputLocation);

            $localFilePath = storage_path(basename($s3FilePath));

            $this->downloadFileFromS3ToLocalServer($s3FilePath, $localFilePath);

            $result = $this->formatCSVFileQueryResults($this->localFilePath = $localFilePath);

            unlink($this->localFilePath);
        }

        $this->logQuery($query, [], $this->getElapsedTime($start));

        return $result;
    }

    /**
     * @param string $s3Path S3 path of the Athena query result
     * @param string $localPath Local path where to store the S3 file content
     *
     * @throws Exception
     */
    public function downloadFileFromS3ToLocalServer(string $s3Path, string $localPath): void
    {
        try {
            $file = fopen($localPath, 'w');

            fwrite($file, $this->getS3Filesystem($this->config['bucket'])->get($s3Path)->read());

            fclose($file);
        } catch (Exception $exception) {
            throw new Exception('Unable to download file from S3', 0, $exception);
        }
    }

    protected function getS3Filesystem(string $bucket = ''): FilesystemInterface
    {
        return new Filesystem(
            new AwsS3Adapter($this->s3Client, $bucket)
        );
    }

    public function formatCSVFileQueryResults(string $filePath): array
    {
        $interpreter = new Interpreter;

        $data = [];
        $interpreter->addObserver(
            function (array $row) use (&$data) {
                $data[] = $row;
            }
        );

        try {
            (new Lexer(new LexerConfig))->parse($filePath, $interpreter);
        } catch (Exception $exception) {
            // ...
        }

        $attributes = [];
        $items = [];
        foreach ($data as $index => $row) {
            if ($index === 0) {
                $attributes = $row;

                continue;
            }

            $currentRow = [];
            foreach ($attributes as $column => $attribute) {
                if (array_key_exists($column, $row)) {
                    $currentRow[$attribute] = $row[$column];
                }
            }

            $items[] = $currentRow;
        }

        return $items;
    }
}
