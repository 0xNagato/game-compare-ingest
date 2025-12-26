<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (! Schema::hasTable('game_media')) {
            return;
        }

        Schema::table('game_media', function (Blueprint $table): void {
            if (! Schema::hasColumn('game_media', 'media_id')) {
                // Add a plain column only; avoid FK to keep cross-driver/order safe
                $table->unsignedBigInteger('media_id')->nullable()->after('id');
            }
        });

        Schema::table('product_media', function (Blueprint $table): void {
            if (! $this->indexExists('product_media', 'product_media_media_id_index')) {
                $table->index('media_id');
            }
        });
    }

    public function down(): void
    {
        if (! Schema::hasTable('product_media')) {
            return;
        }

        Schema::table('game_media', function (Blueprint $table): void {
            if (Schema::hasColumn('game_media', 'media_id')) {
                try { if ($this->indexExists('game_media', 'game_media_media_id_index')) { $table->dropIndex('game_media_id_index'); } } catch (\Throwable) {}
                try { $table->dropConstrainedForeignId('media_id'); } catch (\Throwable) {}
                try { $table->dropColumn('media_id'); } catch (\Throwable) {}
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
            // fallback below
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
