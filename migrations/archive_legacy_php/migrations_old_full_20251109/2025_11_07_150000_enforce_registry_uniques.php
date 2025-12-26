<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        // Ensure unique on (video_game_source_id, provider_item_id) for titles
        if (Schema::hasTable('video_game_titles') &&
            Schema::hasColumn('video_game_titles', 'video_game_source_id') &&
            Schema::hasColumn('video_game_titles', 'provider_item_id')) {
            Schema::table('video_game_titles', function (Blueprint $table): void {
                try {
                    $table->unique(['video_game_source_id', 'provider_item_id'], 'vgt_source_item_unique');
                } catch (\Throwable $e) {
                    // Ignore if already exists
                }
                try {
                    $table->index('provider_item_id', 'vgt_provider_item_idx');
                } catch (\Throwable $e) {
                }
            });
        }

        // Ensure provider_key unique exists on video_game_sources (some envs may have missed prior migration)
        if (Schema::hasTable('video_game_sources') && Schema::hasColumn('video_game_sources', 'provider_key')) {
            $driver = Schema::getConnection()->getDriverName();
            if ($driver === 'pgsql') {
                // Skip if constraint already exists
                \Illuminate\Support\Facades\DB::statement(<<<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname = 'video_game_sources'
          AND c.conname = 'video_game_sources_provider_key_unique'
    ) THEN
        ALTER TABLE video_game_sources
            ADD CONSTRAINT video_game_sources_provider_key_unique UNIQUE (provider_key);
    END IF;
END$$;
SQL);
            } else {
                Schema::table('video_game_sources', function (Blueprint $table): void {
                    try { $table->unique('provider_key', 'video_game_sources_provider_key_unique'); } catch (\Throwable $e) {}
                });
            }
        }
    }

    public function down(): void
    {
        if (Schema::hasTable('video_game_titles')) {
            Schema::table('video_game_titles', function (Blueprint $table): void {
                try { $table->dropUnique('vgt_source_item_unique'); } catch (\Throwable $e) {}
                try { $table->dropIndex('vgt_provider_item_idx'); } catch (\Throwable $e) {}
            });
        }

        if (Schema::hasTable('video_game_sources')) {
            Schema::table('video_game_sources', function (Blueprint $table): void {
                try { $table->dropUnique('video_game_sources_provider_key_unique'); } catch (\Throwable $e) {}
            });
        }
    }
};
