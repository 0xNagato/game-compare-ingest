<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (! Schema::hasTable('game_providers')) {
            return;
        }

        $driver = DB::getDriverName();

        if ($driver === 'pgsql') {
            // Drop the UNIQUE constraint on provider_key if it exists
            try { DB::statement('ALTER TABLE game_providers DROP CONSTRAINT IF EXISTS game_providers_provider_key_unique'); } catch (\Throwable) {}

            // Ensure a non-unique index on provider_key exists
            try { DB::statement('CREATE INDEX IF NOT EXISTS game_providers_provider_key_index ON game_providers (provider_key)'); } catch (\Throwable) {}

            // Deduplicate conflicting rows for the upcoming composite unique
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
            } catch (\Throwable) {}

            // Add composite unique index
            try { DB::statement('ALTER TABLE game_providers ADD CONSTRAINT game_providers_providable_provider_key_unique UNIQUE (providable_type, providable_id, provider_key)'); } catch (\Throwable) {}
        } elseif ($driver === 'mysql') {
            // MySQL variant for completeness
            try { DB::statement('ALTER TABLE game_providers DROP INDEX game_providers_provider_key_unique'); } catch (\Throwable) {}
            try { DB::statement('CREATE INDEX game_providers_provider_key_index ON game_providers (provider_key)'); } catch (\Throwable) {}
            try { DB::statement('ALTER TABLE game_providers ADD UNIQUE INDEX game_providers_providable_provider_key_unique (providable_type, providable_id, provider_key)'); } catch (\Throwable) {}
        }
    }

    public function down(): void
    {
        if (! Schema::hasTable('game_providers')) {
            return;
        }

        $driver = DB::getDriverName();

        if ($driver === 'pgsql') {
            // Drop composite unique
            try { DB::statement('ALTER TABLE game_providers DROP CONSTRAINT IF EXISTS game_providers_providable_provider_key_unique'); } catch (\Throwable) {}
            // Drop non-unique index
            try { DB::statement('DROP INDEX IF EXISTS game_providers_provider_key_index'); } catch (\Throwable) {}
            // Restore global unique (not recommended, but for reversibility)
            try { DB::statement('ALTER TABLE game_providers ADD CONSTRAINT game_providers_provider_key_unique UNIQUE (provider_key)'); } catch (\Throwable) {}
        } elseif ($driver === 'mysql') {
            try { DB::statement('ALTER TABLE game_providers DROP INDEX game_providers_providable_provider_key_unique'); } catch (\Throwable) {}
            try { DB::statement('DROP INDEX game_providers_provider_key_index ON game_providers'); } catch (\Throwable) {}
            try { DB::statement('ALTER TABLE game_providers ADD UNIQUE INDEX game_providers_provider_key_unique (provider_key)'); } catch (\Throwable) {}
        }
    }
};
