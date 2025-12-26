<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        // Only apply on Postgres
        if (DB::getDriverName() !== 'pgsql') {
            return;
        }

        // video_games json columns to convert & index
        $videoGameJsonCols = [
            'metadata',
            'external_ids',
            'external_links',
            'platform_codes',
            'region_codes',
            'title_keywords',
            'genres',
        ];

        foreach ($videoGameJsonCols as $col) {
            if (Schema::hasColumn('video_games', $col)) {
                // Alter to jsonb (Postgres specific)
                DB::statement("ALTER TABLE video_games ALTER COLUMN {$col} TYPE jsonb USING {$col}::jsonb");
            }
        }

        // Add targeted GIN indexes (avoid over-indexing). Use ops where valuable.
        // Existence checks to keep idempotent behavior.
        $indexes = [
            'video_games_metadata_gin' => 'CREATE INDEX video_games_metadata_gin ON video_games USING GIN (metadata)',
            'video_games_external_ids_gin' => 'CREATE INDEX video_games_external_ids_gin ON video_games USING GIN (external_ids)',
            'video_games_title_keywords_gin' => 'CREATE INDEX video_games_title_keywords_gin ON video_games USING GIN (title_keywords)',
        ];

        foreach ($indexes as $name => $sql) {
            $exists = DB::table('pg_indexes')
                ->where('schemaname', 'public')
                ->where('indexname', $name)
                ->exists();
            if (! $exists) {
                DB::statement($sql);
            }
        }

        // price_series_aggregates.metadata -> jsonb + GIN for metadata containment queries
        if (Schema::hasColumn('price_series_aggregates', 'metadata')) {
            DB::statement('ALTER TABLE price_series_aggregates ALTER COLUMN metadata TYPE jsonb USING metadata::jsonb');
            $exists = DB::table('pg_indexes')
                ->where('schemaname', 'public')
                ->where('indexname', 'price_series_metadata_gin')
                ->exists();
            if (! $exists) {
                DB::statement('CREATE INDEX price_series_metadata_gin ON price_series_aggregates USING GIN (metadata)');
            }
        }

        // games_provider table metadata/provider_payload if present
        if (Schema::hasTable('games_providers')) {
            foreach (['metadata', 'provider_payload'] as $col) {
                if (Schema::hasColumn('games_providers', $col)) {
                    DB::statement("ALTER TABLE games_providers ALTER COLUMN {$col} TYPE jsonb USING {$col}::jsonb");
                }
            }
        }
    }

    public function down(): void
    {
        if (DB::getDriverName() !== 'pgsql') {
            return;
        }

        // Rollback: convert jsonb back to json (lossless) and drop indexes
        $dropIndexes = [
            'video_games_metadata_gin',
            'video_games_external_ids_gin',
            'video_games_title_keywords_gin',
            'price_series_metadata_gin',
        ];
        foreach ($dropIndexes as $idx) {
            try {
                DB::statement("DROP INDEX IF EXISTS {$idx}");
            } catch (Throwable $e) {
                // ignore
            }
        }

        $videoGameJsonCols = [
            'metadata', 'external_ids', 'external_links', 'platform_codes', 'region_codes', 'title_keywords', 'genres',
        ];
        foreach ($videoGameJsonCols as $col) {
            if (Schema::hasColumn('video_games', $col)) {
                DB::statement("ALTER TABLE video_games ALTER COLUMN {$col} TYPE json USING {$col}::json");
            }
        }

        if (Schema::hasColumn('price_series_aggregates', 'metadata')) {
            DB::statement('ALTER TABLE price_series_aggregates ALTER COLUMN metadata TYPE json USING metadata::json');
        }
        if (Schema::hasTable('games_providers')) {
            foreach (['metadata', 'provider_payload'] as $col) {
                if (Schema::hasColumn('games_providers', $col)) {
                    DB::statement("ALTER TABLE games_providers ALTER COLUMN {$col} TYPE json USING {$col}::json");
                }
            }
        }
    }
};
