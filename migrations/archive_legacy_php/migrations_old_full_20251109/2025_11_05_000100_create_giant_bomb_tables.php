<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        // The GB tables already exist in our prebuilt SQLite schema used by tests.
        // This migration only adds canonical registry linkage columns and indexes safely.

        if (Schema::hasTable('giant_bomb_games')) {
            Schema::table('giant_bomb_games', function (Blueprint $table) {
                if (! Schema::hasColumn('giant_bomb_games', 'video_game_source_id')) {
                    $table->foreignId('video_game_source_id')->nullable()->after('video_game_id')
                        ->constrained('video_game_sources')->nullOnDelete();
                }
                if (! Schema::hasColumn('giant_bomb_games', 'video_game_title_id')) {
                    $table->foreignId('video_game_title_id')->nullable()->after('video_game_source_id')
                        ->constrained('video_game_titles')->nullOnDelete();
                }
                if (! Schema::hasColumn('giant_bomb_games', 'provider_item_id')) {
                    $table->string('provider_item_id')->nullable()->after('video_game_title_id');
                    $table->index('provider_item_id');
                }
            });

            // Unique pair (video_game_source_id, provider_item_id) if not already present
            // Some drivers (like SQLite) don't expose index existence the same way; try/catch is fine here.
            try {
                Schema::table('giant_bomb_games', function (Blueprint $table) {
                    $table->unique(['video_game_source_id', 'provider_item_id'], 'gb_games_source_item_unique');
                });
            } catch (\Throwable $e) {
                // ignore if it already exists
            }
        }

        if (Schema::hasTable('giant_bomb_game_images')) {
            Schema::table('giant_bomb_game_images', function (Blueprint $table) {
                if (! Schema::hasColumn('giant_bomb_game_images', 'video_game_source_id')) {
                    $table->foreignId('video_game_source_id')->nullable()->after('giant_bomb_game_id')
                        ->constrained('video_game_sources')->nullOnDelete();
                }
                if (! Schema::hasColumn('giant_bomb_game_images', 'video_game_title_id')) {
                    $table->foreignId('video_game_title_id')->nullable()->after('video_game_source_id')
                        ->constrained('video_game_titles')->nullOnDelete();
                }
                if (! Schema::hasColumn('giant_bomb_game_images', 'provider_item_id')) {
                    $table->string('provider_item_id')->nullable()->after('video_game_title_id');
                    $table->index('provider_item_id');
                }
            });
        }

        if (Schema::hasTable('giant_bomb_game_videos')) {
            Schema::table('giant_bomb_game_videos', function (Blueprint $table) {
                if (! Schema::hasColumn('giant_bomb_game_videos', 'video_game_source_id')) {
                    $table->foreignId('video_game_source_id')->nullable()->after('giant_bomb_game_id')
                        ->constrained('video_game_sources')->nullOnDelete();
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'video_game_title_id')) {
                    $table->foreignId('video_game_title_id')->nullable()->after('video_game_source_id')
                        ->constrained('video_game_titles')->nullOnDelete();
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'provider_item_id')) {
                    $table->string('provider_item_id')->nullable()->after('video_game_title_id');
                    $table->index('provider_item_id');
                }
            });
        }
    }

    public function down(): void
    {
        if (Schema::hasTable('giant_bomb_games')) {
            // drop unique first (if present)
            try {
                Schema::table('giant_bomb_games', function (Blueprint $table) {
                    $table->dropUnique('gb_games_source_item_unique');
                });
            } catch (\Throwable $e) {
                // ignore if it doesn't exist
            }

            Schema::table('giant_bomb_games', function (Blueprint $table) {
                if (Schema::hasColumn('giant_bomb_games', 'provider_item_id')) {
                    $table->dropColumn('provider_item_id');
                }
                if (Schema::hasColumn('giant_bomb_games', 'video_game_title_id')) {
                    $table->dropConstrainedForeignId('video_game_title_id');
                }
                if (Schema::hasColumn('giant_bomb_games', 'video_game_source_id')) {
                    $table->dropConstrainedForeignId('video_game_source_id');
                }
            });
        }

        if (Schema::hasTable('giant_bomb_game_images')) {
            Schema::table('giant_bomb_game_images', function (Blueprint $table) {
                if (Schema::hasColumn('giant_bomb_game_images', 'provider_item_id')) {
                    $table->dropColumn('provider_item_id');
                }
                if (Schema::hasColumn('giant_bomb_game_images', 'video_game_title_id')) {
                    $table->dropConstrainedForeignId('video_game_title_id');
                }
                if (Schema::hasColumn('giant_bomb_game_images', 'video_game_source_id')) {
                    $table->dropConstrainedForeignId('video_game_source_id');
                }
            });
        }

        if (Schema::hasTable('giant_bomb_game_videos')) {
            Schema::table('giant_bomb_game_videos', function (Blueprint $table) {
                if (Schema::hasColumn('giant_bomb_game_videos', 'provider_item_id')) {
                    $table->dropColumn('provider_item_id');
                }
                if (Schema::hasColumn('giant_bomb_game_videos', 'video_game_title_id')) {
                    $table->dropConstrainedForeignId('video_game_title_id');
                }
                if (Schema::hasColumn('giant_bomb_game_videos', 'video_game_source_id')) {
                    $table->dropConstrainedForeignId('video_game_source_id');
                }
            });
        }
    }
};
