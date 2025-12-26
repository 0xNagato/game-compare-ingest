<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (! Schema::hasTable('price_series_aggregates')) {
            return;
        }

        Schema::table('price_series_aggregates', function (Blueprint $table) {
            if (! Schema::hasColumn('price_series_aggregates', 'video_game_id')) {
                $table->foreignId('video_game_id')->nullable()->after('id')->constrained('video_games')->cascadeOnDelete();
            }
        });

        // Backfill video_game_id by joining via products -> video_games on product_id
        // Works on MySQL and SQLite; adjust syntax for common engines
        $driver = DB::getDriverName();
        if ($driver === 'mysql') {
            DB::statement(
                'UPDATE price_series_aggregates psa
                 JOIN video_games vg ON vg.product_id = psa.product_id
                 SET psa.video_game_id = vg.id
                 WHERE psa.video_game_id IS NULL'
            );
        } else {
            // SQLite & others: use correlated subquery
            DB::statement(
                'UPDATE price_series_aggregates
                 SET video_game_id = (
                    SELECT vg.id FROM video_games vg WHERE vg.product_id = price_series_aggregates.product_id
                 )
                 WHERE video_game_id IS NULL'
            );
        }

        // Ensure all rows are populated
        // If any remain null, leave as-is to avoid migration failure; application can backfill when needed.

        Schema::table('price_series_aggregates', function (Blueprint $table) {
            // Drop old unique/indexes if they exist
            try {
                $table->dropUnique('price_series_unique');
            } catch (\Throwable $e) {
                // ignore if not present
            }
            try {
                $table->dropIndex(['product_id', 'bucket', 'window_start']);
            } catch (\Throwable $e) {
                // ignore if not present
            }
        });

        // Make new column required once backfilled
        Schema::table('price_series_aggregates', function (Blueprint $table) {
            $table->foreignId('video_game_id')->nullable(false)->change();
        });

        Schema::table('price_series_aggregates', function (Blueprint $table) {
            // Recreate unique/indexes with video_game_id
            $table->unique(['video_game_id', 'region_code', 'bucket', 'window_start', 'tax_inclusive'], 'price_series_unique');
            $table->index(['video_game_id', 'bucket', 'window_start']);
        });

        // Finally, drop product_id column
        Schema::table('price_series_aggregates', function (Blueprint $table) {
            if (Schema::hasColumn('price_series_aggregates', 'product_id')) {
                $table->dropConstrainedForeignId('product_id');
            }
        });
    }

    public function down(): void
    {
        // Best effort rollback: reintroduce product_id and try to backfill from video_game_id
        if (! Schema::hasTable('price_series_aggregates')) {
            return;
        }

        Schema::table('price_series_aggregates', function (Blueprint $table) {
            if (! Schema::hasColumn('price_series_aggregates', 'product_id')) {
                $table->foreignId('product_id')->nullable()->constrained()->cascadeOnDelete();
            }
        });

        $driver = DB::getDriverName();
        if ($driver === 'mysql') {
            DB::statement(
                'UPDATE price_series_aggregates psa
                 JOIN video_games vg ON vg.id = psa.video_game_id
                 SET psa.product_id = vg.product_id
                 WHERE psa.product_id IS NULL'
            );
        } else {
            DB::statement(
                'UPDATE price_series_aggregates
                 SET product_id = (
                    SELECT vg.product_id FROM video_games vg WHERE vg.id = price_series_aggregates.video_game_id
                 )
                 WHERE product_id IS NULL'
            );
        }

        Schema::table('price_series_aggregates', function (Blueprint $table) {
            // Restore original indexes
            try {
                $table->dropUnique('price_series_unique');
            } catch (\Throwable $e) {}

            try {
                $table->dropIndex(['video_game_id', 'bucket', 'window_start']);
            } catch (\Throwable $e) {}

            $table->unique(['product_id', 'region_code', 'bucket', 'window_start', 'tax_inclusive'], 'price_series_unique');
            $table->index(['product_id', 'bucket', 'window_start']);
        });

        Schema::table('price_series_aggregates', function (Blueprint $table) {
            if (Schema::hasColumn('price_series_aggregates', 'video_game_id')) {
                $table->dropConstrainedForeignId('video_game_id');
            }
        });
    }
};
