<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (! Schema::hasTable('region_prices')) {
            return;
        }

        // 1) Add nullable column and FK
        Schema::table('region_prices', function (Blueprint $table) {
            if (! Schema::hasColumn('region_prices', 'video_game_id')) {
                $table->foreignId('video_game_id')->nullable()->after('sku_region_id')
                    ->constrained('video_games')
                    ->cascadeOnDelete();
            }
        });

        // 2) Backfill video_game_id by joining through sku_regions -> products -> video_games
        // Prefer the first matching video_game for a product (if multiple)
        $driver = DB::getDriverName();
        if ($driver === 'mysql') {
            DB::statement(
                'UPDATE region_prices rp
                 JOIN sku_regions sr ON sr.id = rp.sku_region_id
                 JOIN video_games vg ON vg.product_id = sr.product_id
                 SET rp.video_game_id = vg.id
                 WHERE rp.video_game_id IS NULL'
            );
        } elseif ($driver === 'pgsql') {
            // Postgres: perform batched updates to avoid long-running single statement issues
            $batchSize = 1000;
            $total = DB::table('region_prices')->whereNull('video_game_id')->count();
            for ($offset = 0; $offset < $total; $offset += $batchSize) {
                $ids = DB::table('region_prices')
                    ->whereNull('video_game_id')
                    ->orderBy('id')
                    ->limit($batchSize)
                    ->pluck('id');
                if ($ids->isEmpty()) {
                    break;
                }
                // Use a WITH clause to map region_price id -> video_game_id
                $idList = $ids->implode(',');
                DB::statement(
                    "WITH mapping AS (
                        SELECT rp.id AS rp_id, vg.id AS vg_id
                        FROM region_prices rp
                        JOIN sku_regions sr ON sr.id = rp.sku_region_id
                        JOIN video_games vg ON vg.product_id = sr.product_id
                        WHERE rp.id IN ($idList)
                    )
                    UPDATE region_prices SET video_game_id = mapping.vg_id
                    FROM mapping WHERE region_prices.id = mapping.rp_id AND region_prices.video_game_id IS NULL"
                );
            }
        } else {
            // SQLite & others: fallback to correlated subquery
            DB::statement(
                'UPDATE region_prices
                 SET video_game_id = (
                    SELECT vg.id
                    FROM sku_regions sr
                    JOIN video_games vg ON vg.product_id = sr.product_id
                    WHERE sr.id = region_prices.sku_region_id
                    LIMIT 1
                 )
                 WHERE video_game_id IS NULL'
            );
        }

        // 3) Make column not null and add indexes
        Schema::table('region_prices', function (Blueprint $table) {
            $table->foreignId('video_game_id')->nullable(false)->change();
            $table->index(['video_game_id', 'recorded_at']);
        });
    }

    public function down(): void
    {
        if (! Schema::hasTable('region_prices')) {
            return;
        }

        Schema::table('region_prices', function (Blueprint $table) {
            // Drop index if exists (wrapped in try/catch by schema builder in most DBs)
            try {
                $table->dropIndex(['video_game_id', 'recorded_at']);
            } catch (\Throwable $e) {
                // ignore
            }

            if (Schema::hasColumn('region_prices', 'video_game_id')) {
                $table->dropConstrainedForeignId('video_game_id');
            }
        });
    }
};
