<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

/**
 * Idempotent enforcement of the desired game_providers constraint layout:
 *  - Global unique on provider_key must be removed.
 *  - Non-unique index on provider_key should exist for lookups.
 *  - Composite UNIQUE (providable_type, providable_id, provider_key).
 * Handles pgsql + mysql; sqlite falls back to schema operations.
 */
return new class extends Migration {
    public function up(): void
    {
        if (! Schema::hasTable('game_providers')) {
            return;
        }

        $driver = DB::getDriverName();

        // 1. Drop legacy global unique if present
        try {
            if ($driver === 'pgsql') {
                DB::statement('ALTER TABLE game_providers DROP CONSTRAINT IF EXISTS game_providers_provider_key_unique');
            } elseif ($driver === 'mysql') {
                DB::statement('ALTER TABLE game_providers DROP INDEX game_providers_provider_key_unique');
            } else {
                // sqlite or other – attempt via schema builder name
                Schema::table('game_providers', function ($table): void {
                    try { $table->dropUnique('game_providers_provider_key_unique'); } catch (\Throwable) {}
                });
            }
        } catch (\Throwable $e) {
            if (function_exists('logger')) logger()->warning('constraints.game_providers.drop_global_unique_failed', ['error' => $e->getMessage()]);
        }

        // 2. Ensure simple non-unique index exists on provider_key
        try {
            if ($driver === 'pgsql') {
                DB::statement('CREATE INDEX IF NOT EXISTS game_providers_provider_key_index ON game_providers (provider_key)');
            } elseif ($driver === 'mysql') {
                // MySQL lacks IF NOT EXISTS for indexes pre-8.0 – emulate
                $hasIndex = DB::select("SHOW INDEX FROM game_providers WHERE Key_name = 'game_providers_provider_key_index'");
                if (empty($hasIndex)) {
                    DB::statement('CREATE INDEX game_providers_provider_key_index ON game_providers (provider_key)');
                }
            } else {
                Schema::table('game_providers', function ($table): void {
                    try { $table->index('provider_key', 'game_providers_provider_key_index'); } catch (\Throwable) {}
                });
            }
        } catch (\Throwable $e) {
            if (function_exists('logger')) logger()->warning('constraints.game_providers.add_plain_index_failed', ['error' => $e->getMessage()]);
        }

        // 3. Deduplicate potential conflicts for composite unique (keep lowest id)
        try {
            DB::statement(<<<'SQL'
WITH dups AS (
    SELECT MIN(id) AS keep_id, providable_type, providable_id, provider_key
    FROM game_providers
    GROUP BY providable_type, providable_id, provider_key
    HAVING COUNT(*) > 1
), victims AS (
    SELECT gp.id
    FROM game_providers gp
    JOIN dups d ON gp.providable_type = d.providable_type
               AND gp.providable_id = d.providable_id
               AND gp.provider_key = d.provider_key
    WHERE gp.id <> d.keep_id
)
DELETE FROM game_providers WHERE id IN (SELECT id FROM victims);
SQL);
        } catch (\Throwable $e) {
            if (function_exists('logger')) logger()->warning('constraints.game_providers.dedupe_failed', ['error' => $e->getMessage()]);
        }

        // 4. Add composite unique (idempotent)
        try {
            if ($driver === 'pgsql') {
                // Check existing constraint
                $existing = DB::select("SELECT 1 FROM pg_constraint c JOIN pg_class t ON c.conrelid=t.oid WHERE t.relname='game_providers' AND c.conname='game_providers_providable_provider_key_unique'");
                if (empty($existing)) {
                    DB::statement('ALTER TABLE game_providers ADD CONSTRAINT game_providers_providable_provider_key_unique UNIQUE (providable_type, providable_id, provider_key)');
                }
            } elseif ($driver === 'mysql') {
                // MySQL: need to inspect information_schema
                $existing = DB::select("SELECT 1 FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name='game_providers' AND index_name='game_providers_providable_provider_key_unique'");
                if (empty($existing)) {
                    DB::statement('ALTER TABLE game_providers ADD UNIQUE INDEX game_providers_providable_provider_key_unique (providable_type, providable_id, provider_key)');
                }
            } else {
                Schema::table('game_providers', function ($table): void {
                    try { $table->unique(['providable_type','providable_id','provider_key'], 'game_providers_providable_provider_key_unique'); } catch (\Throwable) {}
                });
            }
        } catch (\Throwable $e) {
            if (function_exists('logger')) logger()->warning('constraints.game_providers.add_composite_unique_failed', ['error' => $e->getMessage()]);
        }
    }

    public function down(): void
    {
        if (! Schema::hasTable('game_providers')) {
            return;
        }
        $driver = DB::getDriverName();
        // Drop composite unique
        try {
            if ($driver === 'pgsql') {
                DB::statement('ALTER TABLE game_providers DROP CONSTRAINT IF EXISTS game_providers_providable_provider_key_unique');
            } elseif ($driver === 'mysql') {
                DB::statement('ALTER TABLE game_providers DROP INDEX game_providers_providable_provider_key_unique');
            } else {
                Schema::table('game_providers', function ($table): void {
                    try { $table->dropUnique('game_providers_providable_provider_key_unique'); } catch (\Throwable) {}
                });
            }
        } catch (\Throwable) {}

        // Optionally restore global unique (for reversibility only)
        try {
            if ($driver === 'pgsql') {
                DB::statement('ALTER TABLE game_providers ADD CONSTRAINT game_providers_provider_key_unique UNIQUE (provider_key)');
            } elseif ($driver === 'mysql') {
                DB::statement('ALTER TABLE game_providers ADD UNIQUE INDEX game_providers_provider_key_unique (provider_key)');
            } else {
                Schema::table('game_providers', function ($table): void {
                    try { $table->unique('provider_key', 'game_providers_provider_key_unique'); } catch (\Throwable) {}
                });
            }
        } catch (\Throwable) {}
    }
};
