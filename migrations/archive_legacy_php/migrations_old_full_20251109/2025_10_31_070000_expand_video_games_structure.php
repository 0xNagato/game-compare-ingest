<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('video_games', function (Blueprint $table): void {
            if (! Schema::hasColumn('video_games', 'slug')) {
                $table->string('slug')->nullable()->after('title');
            }

            if (! Schema::hasColumn('video_games', 'normalized_title')) {
                $table->string('normalized_title')->nullable()->after('slug');
            }

            if (! Schema::hasColumn('video_games', 'external_ids')) {
                $table->json('external_ids')->nullable()->after('metadata');
            }

            if (! Schema::hasColumn('video_games', 'external_links')) {
                $table->json('external_links')->nullable()->after('external_ids');
            }

            if (! Schema::hasColumn('video_games', 'platform_codes')) {
                $table->json('platform_codes')->nullable()->after('external_links');
            }

            if (! Schema::hasColumn('video_games', 'region_codes')) {
                $table->json('region_codes')->nullable()->after('platform_codes');
            }

            if (! Schema::hasColumn('video_games', 'title_keywords')) {
                $table->json('title_keywords')->nullable()->after('region_codes');
            }

            if (! Schema::hasColumn('video_games', 'payload_hash')) {
                $table->string('payload_hash', 64)->nullable()->after('title_keywords');
            }

            if (! Schema::hasColumn('video_games', 'last_synced_at')) {
                $table->timestamp('last_synced_at')->nullable()->after('payload_hash');
            }
        });

        Schema::table('video_games', function (Blueprint $table): void {
            if (! $this->indexExists('video_games', 'video_games_slug_unique') && Schema::hasColumn('video_games', 'slug')) {
                $table->unique('slug');
            }

            if (! $this->indexExists('video_games', 'video_games_normalized_title_index') && Schema::hasColumn('video_games', 'normalized_title')) {
                $table->index('normalized_title');
            }

            if (! $this->indexExists('video_games', 'video_games_last_synced_at_index') && Schema::hasColumn('video_games', 'last_synced_at')) {
                $table->index('last_synced_at');
            }
        });

        if (! Schema::hasTable('video_game_sources')) {
            Schema::create('video_game_sources', function (Blueprint $table): void {
                $table->id();
                $table->foreignId('video_game_id')->constrained()->cascadeOnDelete();
                $table->string('provider', 64);
                $table->string('provider_game_id', 128);
                $table->string('provider_slug', 128)->nullable();
                $table->string('provider_hash', 64)->nullable();
                $table->json('payload')->nullable();
                $table->json('links')->nullable();
                $table->json('media')->nullable();
                $table->timestamp('synced_at')->nullable();
                $table->timestamps();

                $table->unique(['provider', 'provider_game_id']);
                $table->index(['video_game_id', 'provider']);
            });
        }
    }

    public function down(): void
    {
        if (Schema::hasTable('video_game_sources')) {
            Schema::drop('video_game_sources');
        }

        Schema::table('video_games', function (Blueprint $table): void {
            if (Schema::hasColumn('video_games', 'last_synced_at')) {
                if ($this->indexExists('video_games', 'video_games_last_synced_at_index')) {
                    $table->dropIndex('video_games_last_synced_at_index');
                }
                $table->dropColumn('last_synced_at');
            }

            if (Schema::hasColumn('video_games', 'payload_hash')) {
                $table->dropColumn('payload_hash');
            }

            if (Schema::hasColumn('video_games', 'title_keywords')) {
                $table->dropColumn('title_keywords');
            }

            if (Schema::hasColumn('video_games', 'region_codes')) {
                $table->dropColumn('region_codes');
            }

            if (Schema::hasColumn('video_games', 'platform_codes')) {
                $table->dropColumn('platform_codes');
            }

            if (Schema::hasColumn('video_games', 'external_links')) {
                $table->dropColumn('external_links');
            }

            if (Schema::hasColumn('video_games', 'external_ids')) {
                $table->dropColumn('external_ids');
            }

            if (Schema::hasColumn('video_games', 'normalized_title')) {
                if ($this->indexExists('video_games', 'video_games_normalized_title_index')) {
                    $table->dropIndex('video_games_normalized_title_index');
                }
                $table->dropColumn('normalized_title');
            }

            if (Schema::hasColumn('video_games', 'slug')) {
                if ($this->indexExists('video_games', 'video_games_slug_unique')) {
                    $table->dropUnique('video_games_slug_unique');
                }
                $table->dropColumn('slug');
            }
        });
    }

    private function indexExists(string $table, string $index): bool
    {
        $connection = Schema::getConnection();
        $tableName = $connection->getTablePrefix().$table;

        if ($connection->getDriverName() === 'sqlite') {
            $indexes = $connection->select("PRAGMA index_list('".$tableName."')");
            foreach ($indexes as $item) {
                $name = is_array($item) ? ($item['name'] ?? null) : ($item->name ?? null);
                if ($name === $index) {
                    return true;
                }
            }

            return false;
        }

        try {
            $schemaManager = $connection->getDoctrineSchemaManager();
            $indexes = $schemaManager->listTableIndexes($tableName);

            return array_key_exists($index, $indexes);
        } catch (\Throwable) {
            // fall back checks below
        }

        if ($connection->getDriverName() === 'mysql') {
            $result = $connection->select(
                'SELECT 1 FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? AND index_name = ? LIMIT 1',
                [$connection->getDatabaseName(), $tableName, $index]
            );

            return $result !== [];
        }

        if ($connection->getDriverName() === 'pgsql') {
            $result = $connection->select(
                'SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND tablename = ? AND indexname = ? LIMIT 1',
                [$tableName, $index]
            );

            return $result !== [];
        }

        return false;
    }
};
