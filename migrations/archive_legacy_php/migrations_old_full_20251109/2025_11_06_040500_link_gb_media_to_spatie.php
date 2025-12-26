<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        // Link GB images directly to video_games and optionally to spatie media
        if (Schema::hasTable('giant_bomb_game_images')) {
            Schema::table('giant_bomb_game_images', function (Blueprint $table): void {
                if (! Schema::hasColumn('giant_bomb_game_images', 'video_game_id')) {
                    $table->unsignedBigInteger('video_game_id')->nullable()->after('giant_bomb_game_id')->index();
                }
                if (! Schema::hasColumn('giant_bomb_game_images', 'media_id')) {
                    $table->unsignedBigInteger('media_id')->nullable()->after('video_game_id')->index();
                }

                // Soft FKs to avoid engine diffs between envs
                if (Schema::hasColumn('giant_bomb_game_images', 'video_game_id')) {
                    try {
                        $table->foreign('video_game_id')->references('id')->on('video_games')->onDelete('set null');
                    } catch (\Throwable) {
                        // ignore if already exists / unsupported
                    }
                }
                if (Schema::hasColumn('giant_bomb_game_images', 'media_id')) {
                    try {
                        $table->foreign('media_id')->references('id')->on('media')->onDelete('set null');
                    } catch (\Throwable) {
                        // ignore if already exists / unsupported
                    }
                }
            });
        }

        // Link GB videos directly to video_games and optionally to spatie media
        if (Schema::hasTable('giant_bomb_game_videos')) {
            Schema::table('giant_bomb_game_videos', function (Blueprint $table): void {
                if (! Schema::hasColumn('giant_bomb_game_videos', 'video_game_id')) {
                    $table->unsignedBigInteger('video_game_id')->nullable()->after('giant_bomb_game_id')->index();
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'media_id')) {
                    $table->unsignedBigInteger('media_id')->nullable()->after('video_game_id')->index();
                }

                if (Schema::hasColumn('giant_bomb_game_videos', 'video_game_id')) {
                    try {
                        $table->foreign('video_game_id')->references('id')->on('video_games')->onDelete('set null');
                    } catch (\Throwable) {
                        // ignore if already exists / unsupported
                    }
                }
                if (Schema::hasColumn('giant_bomb_game_videos', 'media_id')) {
                    try {
                        $table->foreign('media_id')->references('id')->on('media')->onDelete('set null');
                    } catch (\Throwable) {
                        // ignore if already exists / unsupported
                    }
                }
            });
        }

        // Opportunistically backfill video_game_id from parent giant_bomb_games
        if (Schema::hasTable('giant_bomb_games') && Schema::hasTable('giant_bomb_game_images')) {
            // We intentionally avoid raw SQL here; backfill handled by sync command
        }
    }

    public function down(): void
    {
        if (Schema::hasTable('giant_bomb_game_images')) {
            Schema::table('giant_bomb_game_images', function (Blueprint $table): void {
                if (Schema::hasColumn('giant_bomb_game_images', 'media_id')) {
                    try { $table->dropConstrainedForeignId('media_id'); } catch (\Throwable) {}
                    $table->dropColumn('media_id');
                }
                if (Schema::hasColumn('giant_bomb_game_images', 'video_game_id')) {
                    try { $table->dropConstrainedForeignId('video_game_id'); } catch (\Throwable) {}
                    $table->dropColumn('video_game_id');
                }
            });
        }

        if (Schema::hasTable('giant_bomb_game_videos')) {
            Schema::table('giant_bomb_game_videos', function (Blueprint $table): void {
                if (Schema::hasColumn('giant_bomb_game_videos', 'media_id')) {
                    try { $table->dropConstrainedForeignId('media_id'); } catch (\Throwable) {}
                    $table->dropColumn('media_id');
                }
                if (Schema::hasColumn('giant_bomb_game_videos', 'video_game_id')) {
                    try { $table->dropConstrainedForeignId('video_game_id'); } catch (\Throwable) {}
                    $table->dropColumn('video_game_id');
                }
            });
        }
    }
};
