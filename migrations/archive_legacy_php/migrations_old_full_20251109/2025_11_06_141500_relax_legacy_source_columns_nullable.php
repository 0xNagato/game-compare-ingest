<?php

use Illuminate\Database\Migrations\Migration;
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

        if ($driver === 'pgsql') {
            foreach (['provider','provider_game_id','provider_slug','provider_hash'] as $col) {
                try { DB::statement("ALTER TABLE video_game_sources ALTER COLUMN {$col} DROP NOT NULL"); } catch (\Throwable $e) {}
            }
        } elseif ($driver === 'mysql') {
            // Modify column definitions to be nullable retaining sizes
            $sqls = [
                'ALTER TABLE `video_game_sources` MODIFY `provider` VARCHAR(64) NULL',
                'ALTER TABLE `video_game_sources` MODIFY `provider_game_id` VARCHAR(128) NULL',
                'ALTER TABLE `video_game_sources` MODIFY `provider_slug` VARCHAR(128) NULL',
                'ALTER TABLE `video_game_sources` MODIFY `provider_hash` VARCHAR(64) NULL',
            ];
            foreach ($sqls as $sql) {
                try { DB::statement($sql); } catch (\Throwable $e) {}
            }
        } elseif ($driver === 'sqlite') {
            // Best-effort: SQLite migrations for altering nullability are limited; skipping
        }
    }

    public function down(): void
    {
        if (! Schema::hasTable('video_game_sources')) {
            return;
        }

        $driver = Schema::getConnection()->getDriverName();

        if ($driver === 'pgsql') {
            foreach (['provider','provider_game_id'] as $col) {
                try { DB::statement("ALTER TABLE video_game_sources ALTER COLUMN {$col} SET NOT NULL"); } catch (\Throwable $e) {}
            }
        } elseif ($driver === 'mysql') {
            $sqls = [
                'ALTER TABLE `video_game_sources` MODIFY `provider` VARCHAR(64) NOT NULL',
                'ALTER TABLE `video_game_sources` MODIFY `provider_game_id` VARCHAR(128) NOT NULL',
            ];
            foreach ($sqls as $sql) {
                try { DB::statement($sql); } catch (\Throwable $e) {}
            }
        } elseif ($driver === 'sqlite') {
            // No-op
        }
    }
};
