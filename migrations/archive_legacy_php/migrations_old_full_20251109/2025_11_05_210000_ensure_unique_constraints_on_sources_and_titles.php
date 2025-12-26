<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        // Ensure unique provider_key on video_game_sources (only if that column exists in this schema)
        if (Schema::hasTable('video_game_sources') && Schema::hasColumn('video_game_sources', 'provider_key')) {
            if (! $this->indexExists('video_game_sources', 'video_game_sources_provider_key_unique')) {
                Schema::table('video_game_sources', function (Blueprint $table): void {
                    // Named index to allow clean down() handling
                    $table->unique('provider_key', 'video_game_sources_provider_key_unique');
                });
            }
        }

        // Ensure unique (video_game_source_id, provider_item_id) on video_game_titles
        if (Schema::hasTable('video_game_titles')) {
            if (! $this->indexExists('video_game_titles', 'video_game_titles_source_item_unique')) {
                Schema::table('video_game_titles', function (Blueprint $table): void {
                    $table->unique(['video_game_source_id', 'provider_item_id'], 'video_game_titles_source_item_unique');
                });
            }
        }
    }

    public function down(): void
    {
        if (Schema::hasTable('video_game_sources')) {
            Schema::table('video_game_sources', function (Blueprint $table): void {
                // Drop if present
                if ($this->indexExists('video_game_sources', 'video_game_sources_provider_key_unique')) {
                    $table->dropUnique('video_game_sources_provider_key_unique');
                }
            });
        }

        if (Schema::hasTable('video_game_titles')) {
            Schema::table('video_game_titles', function (Blueprint $table): void {
                if ($this->indexExists('video_game_titles', 'video_game_titles_source_item_unique')) {
                    $table->dropUnique('video_game_titles_source_item_unique');
                }
            });
        }
    }

    private function indexExists(string $table, string $indexName): bool
    {
        $driver = DB::getDriverName();

        if ($driver === 'sqlite') {
            $indexes = DB::select('PRAGMA index_list("'.$table.'")');
            foreach ($indexes as $index) {
                // sqlite returns objects with name property
                if (isset($index->name) && $index->name === $indexName) {
                    return true;
                }
            }
            return false;
        }

        if ($driver === 'mysql') {
            $rows = DB::select('SHOW INDEX FROM `'.$table.'` WHERE Key_name = ?', [$indexName]);
            return count($rows) > 0;
        }

        if ($driver === 'pgsql') {
            $rows = DB::select('SELECT to_regclass(?) as name', [$indexName]);
            return !empty($rows) && isset($rows[0]->name) && $rows[0]->name !== null;
        }

        // Fallback: attempt to create/drop and ignore errors (assume not exists)
        return false;
    }
};
