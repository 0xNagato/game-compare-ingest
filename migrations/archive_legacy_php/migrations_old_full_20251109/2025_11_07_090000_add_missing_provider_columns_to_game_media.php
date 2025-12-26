<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        // Ensure game_images has provider columns even if earlier migration order skipped them
        if (Schema::hasTable('game_images')) {
            Schema::table('game_images', function (Blueprint $table): void {
                if (! Schema::hasColumn('game_images', 'provider_item_id')) {
                    $table->string('provider_item_id')->nullable()->after('game_provider_id');
                }
                if (! Schema::hasColumn('game_images', 'video_game_source_id')) {
                    $table->unsignedBigInteger('video_game_source_id')->nullable()->after('media_id');
                }
                if (! Schema::hasColumn('game_images', 'provider_payload')) {
                    $table->json('provider_payload')->nullable()->after('metadata');
                }

                // Unique constraint to prevent duplicate provider items per provider
                if (! $this->hasIndex('game_images', 'game_images_provider_unique')) {
                    // Only create if both columns exist
                    if (Schema::hasColumn('game_images', 'game_provider_id') && Schema::hasColumn('game_images', 'provider_item_id')) {
                        $table->unique(['game_provider_id', 'provider_item_id'], 'game_images_provider_unique');
                    }
                }

                // Add FK to video_game_sources if table exists
                if (Schema::hasTable('video_game_sources') && ! $this->hasForeign('game_images', 'game_images_video_game_source_id_foreign')) {
                    $table->foreign('video_game_source_id')
                        ->references('id')
                        ->on('video_game_sources')
                        ->onDelete('set null');
                }
            });
        }

        // Ensure game_videos has provider columns even if earlier migration order skipped them
        if (Schema::hasTable('game_videos')) {
            Schema::table('game_videos', function (Blueprint $table): void {
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
                    if (Schema::hasColumn('game_videos', 'game_provider_id') && Schema::hasColumn('game_videos', 'provider_item_id')) {
                        $table->unique(['game_provider_id', 'provider_item_id'], 'game_videos_provider_unique');
                    }
                }

                if (Schema::hasTable('video_game_sources') && ! $this->hasForeign('game_videos', 'game_videos_video_game_source_id_foreign')) {
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
            Schema::table('game_images', function (Blueprint $table): void {
                // Drop unique before columns
                if ($this->hasIndex('game_images', 'game_images_provider_unique')) {
                    $table->dropUnique('game_images_provider_unique');
                }
                if ($this->hasForeign('game_images', 'game_images_video_game_source_id_foreign')) {
                    $table->dropForeign('game_images_video_game_source_id_foreign');
                }
                if (Schema::hasColumn('game_images', 'provider_payload')) {
                    $table->dropColumn('provider_payload');
                }
                if (Schema::hasColumn('game_images', 'provider_item_id')) {
                    $table->dropColumn('provider_item_id');
                }
                if (Schema::hasColumn('game_images', 'video_game_source_id')) {
                    $table->dropColumn('video_game_source_id');
                }
            });
        }

        if (Schema::hasTable('game_videos')) {
            Schema::table('game_videos', function (Blueprint $table): void {
                if ($this->hasIndex('game_videos', 'game_videos_provider_unique')) {
                    $table->dropUnique('game_videos_provider_unique');
                }
                if ($this->hasForeign('game_videos', 'game_videos_video_game_source_id_foreign')) {
                    $table->dropForeign('game_videos_video_game_source_id_foreign');
                }
                if (Schema::hasColumn('game_videos', 'provider_payload')) {
                    $table->dropColumn('provider_payload');
                }
                if (Schema::hasColumn('game_videos', 'provider_item_id')) {
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

            // For MySQL/Postgres, a best-effort check via information_schema
            if (in_array($driver, ['mysql', 'pgsql'], true)) {
                $schema = $driver === 'mysql' ? $connection->getDatabaseName() : 'public';
                $sql = $driver === 'mysql'
                    ? "SELECT INDEX_NAME as name FROM information_schema.statistics WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ?"
                    : "SELECT indexname as name FROM pg_indexes WHERE schemaname = ? AND tablename = ? AND indexname = ?";
                $res = $connection->select($sql, [$schema, $table, $indexName]);
                return ! empty($res);
            }
        } catch (\Throwable) {
            // ignore
        }

        return false;
    }

    private function hasForeign(string $table, string $foreignName): bool
    {
        try {
            $connection = Schema::getConnection();
            $driver = $connection->getDriverName();
            if ($driver === 'sqlite') {
                // SQLite doesn't name FKs the same way; skip
                return false;
            }
            if ($driver === 'mysql') {
                $sql = 'SELECT CONSTRAINT_NAME as name FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = ?';
                $res = $connection->select($sql, [$connection->getDatabaseName(), $table, $foreignName]);
                return ! empty($res);
            }
            if ($driver === 'pgsql') {
                $sql = 'SELECT constraint_name as name FROM information_schema.table_constraints WHERE table_schema = ? AND table_name = ? AND constraint_name = ?';
                $res = $connection->select($sql, ['public', $table, $foreignName]);
                return ! empty($res);
            }
        } catch (\Throwable) {
            // ignore
        }

        return false;
    }
};
