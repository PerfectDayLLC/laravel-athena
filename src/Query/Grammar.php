<?php

namespace PerfectDayLlc\Athena\Query;

use Illuminate\Database\Query\Grammars\MySqlGrammar;
use Illuminate\Database\Query\Builder;

class Grammar extends MySqlGrammar
{
    protected function compileLimit(Builder $query, $limit): string
    {
        // Only apply BETWEEN clause, if missing OFFSET, otherwise use presto way to LIMIT records
        if (is_int($query->offset)) {
            // using custom BETWEENLIMIT clause only to detect if it is limit to prevent conflict with BETWEEN.
            // Handling it in Connection.php
            return 'BETWEENLIMIT '.(int) $limit;
        }

        return parent::compileLimit($query, $limit);
    }

    protected function compileOffset(Builder $query, $offset): string
    {
        return 'AND '.(int) $offset;
    }
}
