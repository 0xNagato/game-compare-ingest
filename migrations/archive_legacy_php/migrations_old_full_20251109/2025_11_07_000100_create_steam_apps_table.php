<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        // This migration now serves as a schema harmonizer for steam_apps.
        // If the table exists, we add any missing columns used by the app.
        // We deliberately DO NOT attempt to create the table here to avoid
        // collisions with older create migrations.
        if (! Schema::hasTable('steam_apps')) {
            // Table will be created by the earlier 2025_11_06_230100 migration.
            return;
        }

        Schema::table('steam_apps', function (Blueprint $table): void {
            // Align with command expectations (used in upserts)
            if (! Schema::hasColumn('steam_apps', 'short_description')) {
                $table->text('short_description')->nullable()->after('header_image');
            }
            if (! Schema::hasColumn('steam_apps', 'genres')) {
                $table->json('genres')->nullable()->after('short_description');
            }
            if (! Schema::hasColumn('steam_apps', 'developers')) {
                $table->json('developers')->nullable()->after('genres');
            }
            if (! Schema::hasColumn('steam_apps', 'publishers')) {
                $table->json('publishers')->nullable()->after('developers');
            }
            if (! Schema::hasColumn('steam_apps', 'price_overview')) {
                $table->json('price_overview')->nullable()->after('publishers');
            }
            // Human-friendly formatted price string (e.g., "$9.99")
            if (! Schema::hasColumn('steam_apps', 'price_text')) {
                $table->string('price_text', 64)->nullable()->after('price_overview');
            }
            if (! Schema::hasColumn('steam_apps', 'screenshots')) {
                $table->json('screenshots')->nullable()->after('price_text');
            }
            if (! Schema::hasColumn('steam_apps', 'movies')) {
                $table->json('movies')->nullable()->after('screenshots');
            }
            if (! Schema::hasColumn('steam_apps', 'raw')) {
                $table->json('raw')->nullable()->after('movies');
            }
            if (! Schema::hasColumn('steam_apps', 'last_synced_at')) {
                $table->timestamp('last_synced_at')->nullable()->after('raw');
            }
        });
    }

    public function down(): void
    {
        if (! Schema::hasTable('steam_apps')) {
            return;
        }

        Schema::table('steam_apps', function (Blueprint $table): void {
            // Best-effort rollback of columns added by this migration
            $drops = [
                'short_description',
                'genres',
                'developers',
                'publishers',
                'price_overview',
                'price_text',
                'screenshots',
                'movies',
                'raw',
                'last_synced_at',
            ];

            foreach ($drops as $col) {
                if (Schema::hasColumn('steam_apps', $col)) {
                    $table->dropColumn($col);
                }
            }
        });
    }
};
