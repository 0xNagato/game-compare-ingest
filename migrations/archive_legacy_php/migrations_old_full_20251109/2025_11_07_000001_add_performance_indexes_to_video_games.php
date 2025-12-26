<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run this migration outside a transaction to avoid aborting the whole
     * migration on Postgres when a statement (e.g., CREATE EXTENSION) fails.
     */
    public $withinTransaction = false;

    public function up(): void
    {
        if (! Schema::hasTable('video_games')) {
            return; // nothing to index yet
        }

        if (! Schema::hasColumn('video_games', 'title')) {
            // Safety: don't proceed if schema is unexpected
            return;
        }

        $driver = config('database.connections.'.config('database.default').'.driver');

        if ($driver === 'pgsql') {
            // Enable pg_trgm for trigram index if available and permitted
            try {
                $hasTrgm = (bool) DB::table('pg_extension')
                    ->where('extname', 'pg_trgm')
                    ->exists();
                if (! $hasTrgm) {
                    // Attempt creation, but swallow permission errors
                    try { DB::statement('CREATE EXTENSION IF NOT EXISTS pg_trgm;'); } catch (\Throwable) {}
                }
            } catch (\Throwable) {
                // ignore if cannot query catalog or create extension
            }

            // Title trigram GIN for fast ILIKE searches
            try { DB::statement('CREATE INDEX IF NOT EXISTS video_games_title_trgm_gin ON video_games USING gin (title gin_trgm_ops);'); } catch (\Throwable) {}

            // Normalized title btree (already populated in model saving hook)
            if (Schema::hasColumn('video_games', 'normalized_title')) {
                try { DB::statement('CREATE INDEX IF NOT EXISTS video_games_normalized_title_idx ON video_games (normalized_title);'); } catch (\Throwable) {}
            }

            // Slug btree
            try { DB::statement('CREATE INDEX IF NOT EXISTS video_games_slug_idx ON video_games (slug);'); } catch (\Throwable) {}

            // JSONB / array GIN indexes
            if (Schema::hasColumn('video_games', 'external_ids')) {
                try { DB::statement('CREATE INDEX IF NOT EXISTS video_games_external_ids_gin ON video_games USING gin (external_ids);'); } catch (\Throwable) {}
            }
            if (Schema::hasColumn('video_games', 'metadata')) {
                try { DB::statement('CREATE INDEX IF NOT EXISTS video_games_metadata_gin ON video_games USING gin (metadata);'); } catch (\Throwable) {}
            }
            if (Schema::hasColumn('video_games', 'platform_codes')) {
                try { DB::statement('CREATE INDEX IF NOT EXISTS video_games_platform_codes_gin ON video_games USING gin (platform_codes);'); } catch (\Throwable) {}
            }
        } else {
            // Fallback generic indexes for MySQL/SQLite
            if (! $this->indexExists('video_games', 'video_games_title_idx')) {
                Schema::table('video_games', function ($table) {
                    /** @var \Illuminate\Database\Schema\Blueprint $table */
                    $table->index('title', 'video_games_title_idx');
                });
            }
            if (! $this->indexExists('video_games', 'video_games_slug_idx')) {
                Schema::table('video_games', function ($table) {
                    $table->index('slug', 'video_games_slug_idx');
                });
            }
        }
    }

    public function down(): void
    {
        if (! Schema::hasTable('video_games')) {
            return;
        }

        $driver = config('database.connections.'.config('database.default').'.driver');

        if ($driver === 'pgsql') {
            foreach ([
                'video_games_title_trgm_gin',
                'video_games_normalized_title_idx',
                'video_games_slug_idx',
                'video_games_external_ids_gin',
                'video_games_metadata_gin',
                'video_games_platform_codes_gin',
            ] as $idx) {
                try { DB::statement("DROP INDEX IF EXISTS {$idx};"); } catch (\Throwable) {}
            }
        } else {
            // Drop generic indexes if present
            if ($this->indexExists('video_games', 'video_games_title_idx')) {
                Schema::table('video_games', function ($table) {
                    /** @var \Illuminate\Database\Schema\Blueprint $table */
                    $table->dropIndex('video_games_title_idx');
                });
            }
            if ($this->indexExists('video_games', 'video_games_slug_idx')) {
                Schema::table('video_games', function ($table) {
                    $table->dropIndex('video_games_slug_idx');
                });
            }
        }
    }

    private function indexExists(string $table, string $index): bool
    {
        try {
            $connection = DB::connection();
            $schemaManager = $connection->getDoctrineSchemaManager();
            $doctrineTable = $schemaManager->listTableDetails($table);
            return $doctrineTable->hasIndex($index);
        } catch (\Throwable) {
            return false;
        }
    }
};
