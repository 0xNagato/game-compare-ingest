<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        $driver = DB::getDriverName();

        if ($driver !== 'pgsql') {
            // Only relevant for PostgreSQL; MySQL keeps JSON as-is.
            return;
        }

        // Convert JSON columns to JSONB if present
        $columns = [
            'platforms',
            'aliases',
            'image',
            'images',
            'videos',
            'video_shows',
            'themes',
            'video_api_payloads',
            'original_game_rating',
            'raw_results',
            'metadata',
        ];

        foreach ($columns as $col) {
            if (Schema::hasColumn('giant_bomb_games', $col)) {
                DB::statement(sprintf(
                    'ALTER TABLE "giant_bomb_games" ALTER COLUMN "%s" TYPE JSONB USING "%s"::jsonb',
                    $col,
                    $col
                ));
            }
        }

        // Add GIN indexes for common containment queries
        // Note: Not using CONCURRENTLY to remain in migration transaction.
        if (Schema::hasColumn('giant_bomb_games', 'platforms')) {
            DB::statement('CREATE INDEX IF NOT EXISTS gbg_platforms_gin ON "giant_bomb_games" USING GIN ("platforms")');
        }

        if (Schema::hasColumn('giant_bomb_games', 'aliases')) {
            DB::statement('CREATE INDEX IF NOT EXISTS gbg_aliases_gin ON "giant_bomb_games" USING GIN ("aliases")');
        }

        if (Schema::hasColumn('giant_bomb_games', 'metadata')) {
            DB::statement('CREATE INDEX IF NOT EXISTS gbg_metadata_gin ON "giant_bomb_games" USING GIN ("metadata")');
        }
    }

    public function down(): void
    {
        $driver = DB::getDriverName();

        if ($driver !== 'pgsql') {
            return;
        }
        // Best-effort rollback: wrap each step so a single failure doesn't abort entire transaction.
        // 1. Drop indexes (ignore failures)
        foreach (['gbg_platforms_gin','gbg_aliases_gin','gbg_metadata_gin'] as $idx) {
            try { DB::statement('DROP INDEX IF EXISTS '.$idx); } catch (\Throwable $e) {
                if (function_exists('logger')) { logger()->warning('migration.gbg_jsonb_rollback.drop_index_failed', ['index'=>$idx,'error'=>$e->getMessage()]); }
            }
        }

        // 2. Attempt to convert JSONB columns back to JSON only if column is currently jsonb
        $columns = [
            'platforms', 'aliases', 'image', 'images', 'videos', 'video_shows', 'themes',
            'video_api_payloads', 'original_game_rating', 'raw_results', 'metadata',
        ];

        foreach ($columns as $col) {
            if (! Schema::hasColumn('giant_bomb_games', $col)) {
                continue;
            }

            // Detect type; skip if already json or unexpected
            try {
                $typeRow = DB::selectOne("SELECT data_type FROM information_schema.columns WHERE table_name = 'giant_bomb_games' AND column_name = ?", [$col]);
                $dataType = $typeRow->data_type ?? null;
            } catch (\Throwable $e) {
                $dataType = null;
            }

            if ($dataType !== 'jsonb') {
                continue; // Only try to revert jsonb → json
            }

            try {
                DB::statement(sprintf('ALTER TABLE "giant_bomb_games" ALTER COLUMN "%s" TYPE JSON USING "%s"::json', $col, $col));
            } catch (\Throwable $e) {
                if (function_exists('logger')) {
                    logger()->warning('migration.gbg_jsonb_rollback.column_failed', ['column'=>$col,'error'=>$e->getMessage()]);
                }
                // Continue with other columns
            }
        }
    }
};
