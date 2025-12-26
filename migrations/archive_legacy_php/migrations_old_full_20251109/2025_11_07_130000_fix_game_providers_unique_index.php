<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{

    public function up(): void
    {
        if (! Schema::hasTable('game_providers')) {
            return;
        }

    $connection = config('database.default');
    $connections = config('database.connections', []);
    $driver = $connections[$connection]['driver'] ?? null;

        // 1. Drop global unique on provider_key (name fixed by Laravel when originally created)
        // We attempt idempotent raw SQL first for robustness, then Schema API.
        try {
            if ($driver === 'pgsql') {
                DB::statement('DROP INDEX IF EXISTS game_providers_provider_key_unique');
            } elseif ($driver === 'mysql') {
                DB::statement('ALTER TABLE game_providers DROP INDEX game_providers_provider_key_unique');
            } else {
                // Fallback to Schema API (e.g. sqlite)
                Schema::table('game_providers', function (Blueprint $table): void {
                    try { $table->dropUnique('game_providers_provider_key_unique'); } catch (\Throwable) {}
                });
            }
        } catch (\Throwable $e) {
            // Log but continue; existence check failures are non-fatal.
            if (function_exists('logger')) { logger()->warning('migration.fix_game_providers.drop_unique_failed', ['error' => $e->getMessage()]); }
        }

        // 2. Ensure simple non-unique index on provider_key exists for lookups (idempotent)
        try {
            Schema::table('game_providers', function (Blueprint $table): void {
                try { $table->index('provider_key', 'game_providers_provider_key_index'); } catch (\Throwable) {}
            });
        } catch (\Throwable $e) {
            if (function_exists('logger')) { logger()->warning('migration.fix_game_providers.add_index_failed', ['error' => $e->getMessage()]); }
        }

        // 3. De-duplicate rows that would violate the composite unique (keep lowest id per triple)
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
            if (function_exists('logger')) { logger()->warning('migration.fix_game_providers.dedupe_failed', ['error' => $e->getMessage()]); }
        }

        // 4. Add composite unique ensuring per-providable uniqueness only.
        try {
            Schema::table('game_providers', function (Blueprint $table): void {
                try { $table->unique(['providable_type', 'providable_id', 'provider_key'], 'game_providers_providable_provider_key_unique'); } catch (\Throwable) {}
            });
        } catch (\Throwable $e) {
            if (function_exists('logger')) { logger()->warning('migration.fix_game_providers.add_composite_unique_failed', ['error' => $e->getMessage()]); }
        }
    }

    public function down(): void
    {
        if (! Schema::hasTable('game_providers')) {
            return;
        }

    $connection = config('database.default');
    $connections = config('database.connections', []);
    $driver = $connections[$connection]['driver'] ?? null;

        // Drop composite unique
        try {
            Schema::table('game_providers', function (Blueprint $table): void {
                try { $table->dropUnique('game_providers_providable_provider_key_unique'); } catch (\Throwable) {}
            });
        } catch (\Throwable) {}

        // Drop non-unique index
        try {
            Schema::table('game_providers', function (Blueprint $table): void {
                try { $table->dropIndex('game_providers_provider_key_index'); } catch (\Throwable) {}
            });
        } catch (\Throwable) {}

        // Restore original global unique on provider_key
        try {
            if ($driver === 'pgsql') {
                // Recreate unique index manually
                DB::statement('CREATE UNIQUE INDEX game_providers_provider_key_unique ON game_providers (provider_key)');
            } elseif ($driver === 'mysql') {
                DB::statement('ALTER TABLE game_providers ADD UNIQUE INDEX game_providers_provider_key_unique (provider_key)');
            } else {
                Schema::table('game_providers', function (Blueprint $table): void {
                    try { $table->unique('provider_key', 'game_providers_provider_key_unique'); } catch (\Throwable) {}
                });
            }
        } catch (\Throwable $e) {
            if (function_exists('logger')) { logger()->warning('migration.fix_game_providers.restore_unique_failed', ['error' => $e->getMessage()]); }
        }
    }
};
