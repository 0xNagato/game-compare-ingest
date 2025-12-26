<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        if (! Schema::hasTable('video_game_sources')) {
            return;
        }

        Schema::table('video_game_sources', function (Blueprint $table): void {
            // New canonical columns expected by the registry layer
            if (! Schema::hasColumn('video_game_sources', 'provider_key')) {
                $table->string('provider_key', 64)->nullable()->after('id');
            }
            if (! Schema::hasColumn('video_game_sources', 'display_name')) {
                $table->string('display_name')->nullable()->after('provider_key');
            }
            if (! Schema::hasColumn('video_game_sources', 'category')) {
                $table->string('category', 64)->nullable()->after('display_name');
            }
            if (! Schema::hasColumn('video_game_sources', 'slug')) {
                $table->string('slug')->nullable()->after('category');
            }
            if (! Schema::hasColumn('video_game_sources', 'metadata')) {
                $table->json('metadata')->nullable()->after('slug');
            }
        });

        // Add indexes after columns exist
        Schema::table('video_game_sources', function (Blueprint $table): void {
            try {
                if (Schema::hasColumn('video_game_sources', 'provider_key')) {
                    $table->unique('provider_key', 'video_game_sources_provider_key_unique');
                }
            } catch (\Throwable $e) {
                // Ignore if unique already exists
            }

            try {
                if (Schema::hasColumn('video_game_sources', 'slug')) {
                    $table->index('slug', 'video_game_sources_slug_index');
                }
            } catch (\Throwable $e) {
                // Ignore if index already exists
            }
        });
    }

    public function down(): void
    {
        if (! Schema::hasTable('video_game_sources')) {
            return;
        }

        Schema::table('video_game_sources', function (Blueprint $table): void {
            // Drop indexes first (if present)
            try { $table->dropUnique('video_game_sources_provider_key_unique'); } catch (\Throwable $e) {}
            try { $table->dropIndex('video_game_sources_slug_index'); } catch (\Throwable $e) {}

            // Then drop columns we added
            foreach (['metadata', 'slug', 'category', 'display_name', 'provider_key'] as $col) {
                if (Schema::hasColumn('video_game_sources', $col)) {
                    $table->dropColumn($col);
                }
            }
        });
    }
};
