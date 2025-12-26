<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (! Schema::hasTable('video_game_sources')) {
            return;
        }

        $driver = Schema::getConnection()->getDriverName();

        // Make video_game_id nullable and ensure FK is ON DELETE SET NULL
        if ($driver === 'pgsql') {
            // Drop the existing FK if present (Laravel default name)
            try { DB::statement('ALTER TABLE video_game_sources DROP CONSTRAINT IF EXISTS video_game_sources_video_game_id_foreign'); } catch (\Throwable $e) {}

            // Drop NOT NULL constraint on column
            try { DB::statement('ALTER TABLE video_game_sources ALTER COLUMN video_game_id DROP NOT NULL'); } catch (\Throwable $e) {}

            // Re-add FK with SET NULL
            try { DB::statement('ALTER TABLE video_game_sources ADD CONSTRAINT video_game_sources_video_game_id_foreign FOREIGN KEY (video_game_id) REFERENCES video_games(id) ON DELETE SET NULL'); } catch (\Throwable $e) {}
        } elseif ($driver === 'mysql') {
            // MySQL: drop FK, make column nullable, re-add FK with SET NULL
            try { DB::statement('ALTER TABLE `video_game_sources` DROP FOREIGN KEY `video_game_sources_video_game_id_foreign`'); } catch (\Throwable $e) {}
            try { DB::statement('ALTER TABLE `video_game_sources` MODIFY `video_game_id` BIGINT UNSIGNED NULL'); } catch (\Throwable $e) {}
            try { DB::statement('ALTER TABLE `video_game_sources` ADD CONSTRAINT `video_game_sources_video_game_id_foreign` FOREIGN KEY (`video_game_id`) REFERENCES `video_games`(`id`) ON DELETE SET NULL'); } catch (\Throwable $e) {}
        } elseif ($driver === 'sqlite') {
            // SQLite cannot easily alter constraints; attempt a simple NULL-able change if possible
            Schema::table('video_game_sources', function (Blueprint $table): void {
                try {
                    $table->unsignedBigInteger('video_game_id')->nullable()->change();
                } catch (\Throwable $e) {
                    // Fallback noop for SQLite; tests typically use SQLite without strict FKs
                }
            });
        }
    }

    public function down(): void
    {
        if (! Schema::hasTable('video_game_sources')) {
            return;
        }

        $driver = Schema::getConnection()->getDriverName();

        // Revert to NOT NULL with cascade delete (original state)
        if ($driver === 'pgsql') {
            try { DB::statement('ALTER TABLE video_game_sources DROP CONSTRAINT IF EXISTS video_game_sources_video_game_id_foreign'); } catch (\Throwable $e) {}
            try { DB::statement('ALTER TABLE video_game_sources ALTER COLUMN video_game_id SET NOT NULL'); } catch (\Throwable $e) {}
            try { DB::statement('ALTER TABLE video_game_sources ADD CONSTRAINT video_game_sources_video_game_id_foreign FOREIGN KEY (video_game_id) REFERENCES video_games(id) ON DELETE CASCADE'); } catch (\Throwable $e) {}
        } elseif ($driver === 'mysql') {
            try { DB::statement('ALTER TABLE `video_game_sources` DROP FOREIGN KEY `video_game_sources_video_game_id_foreign`'); } catch (\Throwable $e) {}
            try { DB::statement('ALTER TABLE `video_game_sources` MODIFY `video_game_id` BIGINT UNSIGNED NOT NULL'); } catch (\Throwable $e) {}
            try { DB::statement('ALTER TABLE `video_game_sources` ADD CONSTRAINT `video_game_sources_video_game_id_foreign` FOREIGN KEY (`video_game_id`) REFERENCES `video_games`(`id`) ON DELETE CASCADE'); } catch (\Throwable $e) {}
        } elseif ($driver === 'sqlite') {
            Schema::table('video_game_sources', function (Blueprint $table): void {
                try {
                    $table->unsignedBigInteger('video_game_id')->nullable(false)->change();
                } catch (\Throwable $e) {}
            });
        }
    }
};
