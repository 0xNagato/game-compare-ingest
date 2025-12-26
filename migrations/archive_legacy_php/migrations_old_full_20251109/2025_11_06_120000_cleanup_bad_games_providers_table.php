<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        // Drop accidentally created legacy table if it exists
        if (Schema::hasTable('games_providers')) {
            try {
                Schema::drop('games_providers');
            } catch (\Throwable $e) {
                // ignore if cannot drop on this driver; not critical
            }
        }
    }

    public function down(): void
    {
        // no-op: we do not recreate the legacy table
    }
};
