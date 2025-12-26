<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        if (config('database.default') !== 'pgsql') {
            return; // Only applies to Postgres
        }

        if (! Schema::hasTable('media')) {
            return;
        }

        // Ensure custom_properties is jsonb (Laravel maps json to jsonb on PG, but be explicit if needed)
        try {
            DB::statement("ALTER TABLE media ALTER COLUMN custom_properties TYPE jsonb USING custom_properties::jsonb");
        } catch (\Throwable) {
            // ignore if already jsonb
        }

        // GIN index on full JSONB for generic path queries
        try {
            DB::statement("CREATE INDEX IF NOT EXISTS media_custom_props_gin ON media USING GIN (custom_properties)");
        } catch (\Throwable) {}

        // Functional btree indexes for common lookups
        try {
            DB::statement("CREATE INDEX IF NOT EXISTS media_provider_idx ON media ((custom_properties->>'provider'))");
        } catch (\Throwable) {}

        try {
            DB::statement("CREATE INDEX IF NOT EXISTS media_kind_idx ON media ((custom_properties->>'kind'))");
        } catch (\Throwable) {}

        try {
            DB::statement("CREATE INDEX IF NOT EXISTS media_youtube_id_idx ON media ((custom_properties->>'youtube_id'))");
        } catch (\Throwable) {}
    }

    public function down(): void
    {
        if (config('database.default') !== 'pgsql') {
            return;
        }

        try { DB::statement("DROP INDEX IF EXISTS media_youtube_id_idx"); } catch (\Throwable) {}
        try { DB::statement("DROP INDEX IF EXISTS media_kind_idx"); } catch (\Throwable) {}
        try { DB::statement("DROP INDEX IF EXISTS media_provider_idx"); } catch (\Throwable) {}
        try { DB::statement("DROP INDEX IF EXISTS media_custom_props_gin"); } catch (\Throwable) {}
    }
};
