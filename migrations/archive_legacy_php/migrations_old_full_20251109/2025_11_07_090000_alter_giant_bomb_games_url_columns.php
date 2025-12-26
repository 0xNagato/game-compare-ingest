<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        // Promote selected URL columns to TEXT to avoid truncation and future import failures.
        $columns = [
            'site_detail_url',
            'primary_image_url',
            'image_super_url',
            'image_small_url',
            'image_original_url',
            'primary_video_high_url',
            'primary_video_hd_url',
        ];

        $connection = DB::connection();
        $driver = $connection->getDriverName();

        foreach ($columns as $col) {
            if (! Schema::hasColumn('giant_bomb_games', $col)) {
                continue;
            }

            if ($driver === 'pgsql') {
                // Postgres: ALTER TYPE to TEXT; keep NULLability
                DB::statement(sprintf('ALTER TABLE "%s" ALTER COLUMN "%s" TYPE TEXT', 'giant_bomb_games', $col));
            } elseif ($driver === 'mysql') {
                // MySQL: MODIFY COLUMN to TEXT NULL
                DB::statement(sprintf('ALTER TABLE `%s` MODIFY `%s` TEXT NULL', 'giant_bomb_games', $col));
            } else {
                // Fallback to schema change() for drivers that support it
                Schema::table('giant_bomb_games', function (Blueprint $table) use ($col): void {
                    $table->text($col)->nullable()->change();
                });
            }
        }
    }

    public function down(): void
    {
        // Revert back to string(255) where safe.
        $columns = [
            'site_detail_url',
            'primary_image_url',
            'image_super_url',
            'image_small_url',
            'image_original_url',
            'primary_video_high_url',
            'primary_video_hd_url',
        ];

        $connection = DB::connection();
        $driver = $connection->getDriverName();

        foreach ($columns as $col) {
            if (! Schema::hasColumn('giant_bomb_games', $col)) {
                continue;
            }

            if ($driver === 'pgsql') {
                DB::statement(sprintf('ALTER TABLE "%s" ALTER COLUMN "%s" TYPE VARCHAR(255)', 'giant_bomb_games', $col));
            } elseif ($driver === 'mysql') {
                DB::statement(sprintf('ALTER TABLE `%s` MODIFY `%s` VARCHAR(255) NULL', 'giant_bomb_games', $col));
            } else {
                Schema::table('giant_bomb_games', function (Blueprint $table) use ($col): void {
                    $table->string($col)->nullable()->change();
                });
            }
        }
    }
};
