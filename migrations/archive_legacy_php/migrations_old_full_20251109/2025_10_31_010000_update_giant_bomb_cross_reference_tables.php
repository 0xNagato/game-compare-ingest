<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (! $this->indexExists('giant_bomb_games', 'giant_bomb_games_normalized_name_index')) {
            Schema::table('giant_bomb_games', function (Blueprint $table): void {
                $table->index('normalized_name', 'giant_bomb_games_normalized_name_index');
            });
        }
    }

    public function down(): void
    {
        if ($this->indexExists('giant_bomb_games', 'giant_bomb_games_normalized_name_index')) {
            Schema::table('giant_bomb_games', function (Blueprint $table): void {
                $table->dropIndex('giant_bomb_games_normalized_name_index');
            });
        }
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
            // Fallback for drivers without Doctrine support.
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