<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    // Postgres marks the entire transaction as aborted after the first error,
    // which breaks subsequent Schema::hasTable/hasColumn checks. Run this
    // migration outside a transaction so our try/catch guards can continue.
    public $withinTransaction = false;

    public function up(): void
    {
        $driver = Schema::getConnection()->getDriverName();
        if ($driver !== 'pgsql') {
            return; // Only needed for PostgreSQL
        }

        // Helper to safely alter boolean -> smallint with USING clause
        $alter = function (string $table, string $column): void {
            try {
                DB::statement(
                    sprintf('ALTER TABLE %s ALTER COLUMN %s TYPE smallint USING (CASE WHEN %s THEN 1 ELSE 0 END)',
                        $table,
                        $column,
                        $column
                    )
                );
            } catch (\Throwable $e) {
                // ignore if already altered
            }
        };

        if (Schema::hasTable('currencies') && Schema::hasColumn('currencies', 'is_crypto')) {
            $alter('currencies', 'is_crypto');
        }

        if (Schema::hasTable('cross_reference_entries')) {
            if (Schema::hasColumn('cross_reference_entries', 'has_digital')) {
                $alter('cross_reference_entries', 'has_digital');
            }
            if (Schema::hasColumn('cross_reference_entries', 'has_physical')) {
                $alter('cross_reference_entries', 'has_physical');
            }
        }

        if (Schema::hasTable('users') && Schema::hasColumn('users', 'is_admin')) {
            $alter('users', 'is_admin');
        }

        if (Schema::hasTable('price_series_aggregates') && Schema::hasColumn('price_series_aggregates', 'tax_inclusive')) {
            $alter('price_series_aggregates', 'tax_inclusive');
        }

        if (Schema::hasTable('region_prices') && Schema::hasColumn('region_prices', 'tax_inclusive')) {
            $alter('region_prices', 'tax_inclusive');
        }
    }

    public function down(): void
    {
        $driver = Schema::getConnection()->getDriverName();
        if ($driver !== 'pgsql') {
            return;
        }

        // Revert smallint -> boolean
        $revert = function (string $table, string $column): void {
            try {
                DB::statement(
                    sprintf('ALTER TABLE %s ALTER COLUMN %s TYPE boolean USING (CASE WHEN %s = 1 THEN TRUE ELSE FALSE END)',
                        $table,
                        $column,
                        $column
                    )
                );
            } catch (\Throwable $e) {
                // ignore
            }
        };

        if (Schema::hasTable('currencies') && Schema::hasColumn('currencies', 'is_crypto')) {
            $revert('currencies', 'is_crypto');
        }
        if (Schema::hasTable('cross_reference_entries')) {
            if (Schema::hasColumn('cross_reference_entries', 'has_digital')) {
                $revert('cross_reference_entries', 'has_digital');
            }
            if (Schema::hasColumn('cross_reference_entries', 'has_physical')) {
                $revert('cross_reference_entries', 'has_physical');
            }
        }
        if (Schema::hasTable('users') && Schema::hasColumn('users', 'is_admin')) {
            $revert('users', 'is_admin');
        }
        if (Schema::hasTable('price_series_aggregates') && Schema::hasColumn('price_series_aggregates', 'tax_inclusive')) {
            $revert('price_series_aggregates', 'tax_inclusive');
        }
        if (Schema::hasTable('region_prices') && Schema::hasColumn('region_prices', 'tax_inclusive')) {
            $revert('region_prices', 'tax_inclusive');
        }
    }
};
