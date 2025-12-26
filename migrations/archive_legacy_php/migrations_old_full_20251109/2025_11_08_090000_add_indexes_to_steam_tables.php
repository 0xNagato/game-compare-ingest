<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Facades\DB;

return new class extends Migration {
    public function up(): void
    {
        // Extra safety: ensure steam_apps exists before altering
        if (Schema::hasTable('steam_apps')) {
            // Use raw SQL with IF NOT EXISTS for Postgres safety
            if (Schema::hasColumn('steam_apps', 'released_at')) {
                DB::statement('CREATE INDEX IF NOT EXISTS steam_apps_released_at_idx ON steam_apps (released_at)');
            }
            if (Schema::hasColumn('steam_apps', 'released_at') && Schema::hasColumn('steam_apps', 'name')) {
                DB::statement('CREATE INDEX IF NOT EXISTS steam_apps_release_name_idx ON steam_apps (released_at, name)');
            }
        }

        if (Schema::hasTable('steam_app_prices')) {
            // Composite covering index for price lookups (appid + cc + last_synced_at)
            DB::statement('CREATE INDEX IF NOT EXISTS steam_app_prices_appid_cc_synced_idx ON steam_app_prices (appid, cc, last_synced_at)');

            // Add bigint surrogate for potential FK to steam_apps if not present (future-proof)
            if (! Schema::hasColumn('steam_app_prices', 'steam_app_id') && Schema::hasColumn('steam_app_prices','appid')) {
                Schema::table('steam_app_prices', function (Blueprint $table): void {
                    $table->unsignedBigInteger('steam_app_id')->nullable()->after('appid');
                });
                DB::statement('CREATE INDEX IF NOT EXISTS steam_app_prices_steam_app_id_idx ON steam_app_prices (steam_app_id)');
            }
        }
    }

    public function down(): void
    {
        if (Schema::hasTable('steam_apps')) {
            try { DB::statement('DROP INDEX IF EXISTS steam_apps_released_at_idx'); } catch (\Throwable $e) {}
            try { DB::statement('DROP INDEX IF EXISTS steam_apps_release_name_idx'); } catch (\Throwable $e) {}
        }
        if (Schema::hasTable('steam_app_prices')) {
            try { DB::statement('DROP INDEX IF EXISTS steam_app_prices_appid_cc_synced_idx'); } catch (\Throwable $e) {}
            try { DB::statement('DROP INDEX IF EXISTS steam_app_prices_steam_app_id_idx'); } catch (\Throwable $e) {}
            if (Schema::hasColumn('steam_app_prices','steam_app_id')) {
                Schema::table('steam_app_prices', function (Blueprint $table): void {
                    $table->dropColumn('steam_app_id');
                });
            }
        }
    }
};
