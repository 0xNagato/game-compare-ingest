<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        // game_images: add provider_item_id, video_game_source_id, provider_payload
        if (Schema::hasTable('game_images')) {
            Schema::table('game_images', function (Blueprint $table) {
                if (! Schema::hasColumn('game_images', 'provider_item_id')) {
                    $table->string('provider_item_id')->nullable()->after('game_provider_id');
                }
                if (! Schema::hasColumn('game_images', 'video_game_source_id')) {
                    $table->unsignedBigInteger('video_game_source_id')->nullable()->after('media_id');
                }
                if (! Schema::hasColumn('game_images', 'provider_payload')) {
                    $table->json('provider_payload')->nullable()->after('metadata');
                }

                // Indexes to support upserts and lookups
                if (! $this->hasIndex('game_images', 'game_images_provider_unique')) {
                    $table->unique(['game_provider_id', 'provider_item_id'], 'game_images_provider_unique');
                }

                // Add FK to video_game_sources if table exists
                if (Schema::hasTable('video_game_sources')) {
                    $table->foreign('video_game_source_id')
                        ->references('id')
                        ->on('video_game_sources')
                        ->onDelete('set null');
                }
            });
        }

        // game_videos: add provider_item_id, video_game_source_id, provider_payload
        if (Schema::hasTable('game_videos')) {
            Schema::table('game_videos', function (Blueprint $table) {
                if (! Schema::hasColumn('game_videos', 'provider_item_id')) {
                    $table->string('provider_item_id')->nullable()->after('game_provider_id');
                }
                if (! Schema::hasColumn('game_videos', 'video_game_source_id')) {
                    $table->unsignedBigInteger('video_game_source_id')->nullable()->after('media_id');
                }
                if (! Schema::hasColumn('game_videos', 'provider_payload')) {
                    $table->json('provider_payload')->nullable()->after('metadata');
                }

                if (! $this->hasIndex('game_videos', 'game_videos_provider_unique')) {
                    $table->unique(['game_provider_id', 'provider_item_id'], 'game_videos_provider_unique');
                }

                if (Schema::hasTable('video_game_sources')) {
                    $table->foreign('video_game_source_id')
                        ->references('id')
                        ->on('video_game_sources')
                        ->onDelete('set null');
                }
            });
        }
    }

    public function down(): void
    {
        if (Schema::hasTable('game_images')) {
            Schema::table('game_images', function (Blueprint $table) {
                if (Schema::hasColumn('game_images', 'video_game_source_id')) {
                    $table->dropForeign(['video_game_source_id']);
                }
                if (Schema::hasColumn('game_images', 'provider_payload')) {
                    $table->dropColumn('provider_payload');
                }
                if (Schema::hasColumn('game_images', 'provider_item_id')) {
                    if ($this->hasIndex('game_images', 'game_images_provider_unique')) {
                        $table->dropUnique('game_images_provider_unique');
                    }
                    $table->dropColumn('provider_item_id');
                }
                if (Schema::hasColumn('game_images', 'video_game_source_id')) {
                    $table->dropColumn('video_game_source_id');
                }
            });
        }

        if (Schema::hasTable('game_videos')) {
            Schema::table('game_videos', function (Blueprint $table) {
                if (Schema::hasColumn('game_videos', 'video_game_source_id')) {
                    $table->dropForeign(['video_game_source_id']);
                }
                if (Schema::hasColumn('game_videos', 'provider_payload')) {
                    $table->dropColumn('provider_payload');
                }
                if (Schema::hasColumn('game_videos', 'provider_item_id')) {
                    if ($this->hasIndex('game_videos', 'game_videos_provider_unique')) {
                        $table->dropUnique('game_videos_provider_unique');
                    }
                    $table->dropColumn('provider_item_id');
                }
                if (Schema::hasColumn('game_videos', 'video_game_source_id')) {
                    $table->dropColumn('video_game_source_id');
                }
            });
        }
    }

    private function hasIndex(string $table, string $indexName): bool
    {
        try {
            // SQLite pragma to check indexes; for other drivers, Schema doesn't expose a direct API
            $connection = Schema::getConnection();
            $driver = $connection->getDriverName();
            if ($driver === 'sqlite') {
                $indexes = $connection->select("PRAGMA index_list('{$table}')");
                foreach ($indexes as $idx) {
                    if ((string) ($idx->name ?? '') === $indexName) {
                        return true;
                    }
                }
                return false;
            }
            // Fallback: attempt to create and catch
            return false;
        } catch (\Throwable) {
            return false;
        }
    }
};
