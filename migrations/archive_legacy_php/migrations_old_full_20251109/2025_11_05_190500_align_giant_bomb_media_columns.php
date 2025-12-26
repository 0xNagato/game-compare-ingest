<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        // Align giant_bomb_game_images with model/command expectations
        if (Schema::hasTable('giant_bomb_game_images')) {
            Schema::table('giant_bomb_game_images', function (Blueprint $table): void {
                if (! Schema::hasColumn('giant_bomb_game_images', 'ordinal')) {
                    $table->unsignedInteger('ordinal')->default(0)->after('source');
                }
                if (! Schema::hasColumn('giant_bomb_game_images', 'name')) {
                    $table->string('name')->nullable()->after('ordinal');
                }
                if (! Schema::hasColumn('giant_bomb_game_images', 'tag')) {
                    $table->string('tag')->nullable()->after('name');
                }
                if (! Schema::hasColumn('giant_bomb_game_images', 'original_url')) {
                    $table->string('original_url')->nullable()->after('tag');
                }
                if (! Schema::hasColumn('giant_bomb_game_images', 'super_url')) {
                    $table->string('super_url')->nullable()->after('original_url');
                }
                if (! Schema::hasColumn('giant_bomb_game_images', 'screen_url')) {
                    $table->string('screen_url')->nullable()->after('super_url');
                }
                if (! Schema::hasColumn('giant_bomb_game_images', 'medium_url')) {
                    $table->string('medium_url')->nullable()->after('screen_url');
                }
                if (! Schema::hasColumn('giant_bomb_game_images', 'small_url')) {
                    $table->string('small_url')->nullable()->after('medium_url');
                }
                if (! Schema::hasColumn('giant_bomb_game_images', 'thumb_url')) {
                    $table->string('thumb_url')->nullable()->after('small_url');
                }
                if (! Schema::hasColumn('giant_bomb_game_images', 'tiny_url')) {
                    $table->string('tiny_url')->nullable()->after('thumb_url');
                }
                if (! Schema::hasColumn('giant_bomb_game_images', 'icon_url')) {
                    $table->string('icon_url')->nullable()->after('tiny_url');
                }
                if (! Schema::hasColumn('giant_bomb_game_images', 'variants')) {
                    $table->json('variants')->nullable()->after('icon_url');
                }
                if (! Schema::hasColumn('giant_bomb_game_images', 'metadata')) {
                    $table->json('metadata')->nullable()->after('variants');
                }

                // Registry linkage columns if missing
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

            // Unique key on (giant_bomb_game_id, source, ordinal)
            try {
                Schema::table('giant_bomb_game_images', function (Blueprint $table): void {
                    $table->unique(['giant_bomb_game_id', 'source', 'ordinal'], 'gb_images_game_source_ordinal_unique');
                });
            } catch (\Throwable $e) {
                // ignore if already exists
            }

            // Helpful index for tag queries
            try {
                Schema::table('giant_bomb_game_images', function (Blueprint $table): void {
                    $table->index(['source', 'tag'], 'gb_images_source_tag_index');
                });
            } catch (\Throwable $e) {
                // ignore if already exists
            }
        }

        // Align giant_bomb_game_videos with model/command expectations
        if (Schema::hasTable('giant_bomb_game_videos')) {
            Schema::table('giant_bomb_game_videos', function (Blueprint $table): void {
                if (! Schema::hasColumn('giant_bomb_game_videos', 'ordinal')) {
                    $table->unsignedInteger('ordinal')->default(0)->after('source');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'video_id')) {
                    $table->string('video_id')->nullable()->after('ordinal');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'guid')) {
                    $table->string('guid')->nullable()->after('video_id');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'name')) {
                    $table->string('name')->nullable()->after('guid');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'deck')) {
                    $table->text('deck')->nullable()->after('name');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'site_detail_url')) {
                    $table->string('site_detail_url')->nullable()->after('deck');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'api_detail_url')) {
                    $table->string('api_detail_url')->nullable()->after('site_detail_url');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'url')) {
                    $table->string('url')->nullable()->after('api_detail_url');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'hd_url')) {
                    $table->string('hd_url')->nullable()->after('url');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'high_url')) {
                    $table->string('high_url')->nullable()->after('hd_url');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'low_url')) {
                    $table->string('low_url')->nullable()->after('high_url');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'embed_player')) {
                    $table->string('embed_player')->nullable()->after('low_url');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'playable_url')) {
                    $table->string('playable_url')->nullable()->after('embed_player');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'poster_url')) {
                    $table->string('poster_url')->nullable()->after('playable_url');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'length_seconds')) {
                    $table->unsignedInteger('length_seconds')->nullable()->after('poster_url');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'publish_date')) {
                    $table->dateTime('publish_date')->nullable()->after('length_seconds');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'video_type')) {
                    $table->string('video_type')->nullable()->after('publish_date');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'video_show')) {
                    $table->string('video_show')->nullable()->after('video_type');
                }
                if (! Schema::hasColumn('giant_bomb_game_videos', 'metadata')) {
                    $table->json('metadata')->nullable()->after('video_show');
                }

                // Registry linkage columns if missing
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

            // Unique key on (giant_bomb_game_id, source, ordinal)
            try {
                Schema::table('giant_bomb_game_videos', function (Blueprint $table): void {
                    $table->unique(['giant_bomb_game_id', 'source', 'ordinal'], 'gb_videos_game_source_ordinal_unique');
                });
            } catch (\Throwable $e) {
                // ignore if already exists
            }

            try {
                Schema::table('giant_bomb_game_videos', function (Blueprint $table): void {
                    $table->index('video_id', 'gb_videos_video_id_index');
                });
            } catch (\Throwable $e) {
                // ignore if already exists
            }

            try {
                Schema::table('giant_bomb_game_videos', function (Blueprint $table): void {
                    $table->index('guid', 'gb_videos_guid_index');
                });
            } catch (\Throwable $e) {
                // ignore if already exists
            }
        }
    }

    public function down(): void
    {
        // Non-destructive down: we won't drop columns to avoid data loss.
        // If needed, a future migration can explicitly remove columns.
    }
};
