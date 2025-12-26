<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;

return new class extends Migration
{
    public function up(): void
    {
        if (DB::getDriverName() !== 'pgsql') {
            return; // expression jsonb indexing only for Postgres
        }

        // Add targeted expression indexes for provider lookups inside metadata->'sources'
        // giantbomb id, guid; nexarda id. Avoid broad indexing; keep scoped.
        $indexes = [
            'video_games_sources_giantbomb_id_idx' => "CREATE INDEX video_games_sources_giantbomb_id_idx ON video_games ((metadata->'sources'->'giantbomb'->>'id')) WHERE metadata->'sources' ? 'giantbomb'",
            'video_games_sources_giantbomb_guid_idx' => "CREATE INDEX video_games_sources_giantbomb_guid_idx ON video_games ((metadata->'sources'->'giantbomb'->>'guid')) WHERE metadata->'sources' ? 'giantbomb'",
            'video_games_sources_nexarda_id_idx' => "CREATE INDEX video_games_sources_nexarda_id_idx ON video_games ((metadata->'sources'->'nexarda'->>'id')) WHERE metadata->'sources' ? 'nexarda'",
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
    }

    public function down(): void
    {
        if (DB::getDriverName() !== 'pgsql') {
            return;
        }
        foreach ([
            'video_games_sources_giantbomb_id_idx',
            'video_games_sources_giantbomb_guid_idx',
            'video_games_sources_nexarda_id_idx',
        ] as $idx) {
            try {
                DB::statement("DROP INDEX IF EXISTS {$idx}");
            } catch (Throwable $e) {
                // ignore
            }
        }
    }
};
