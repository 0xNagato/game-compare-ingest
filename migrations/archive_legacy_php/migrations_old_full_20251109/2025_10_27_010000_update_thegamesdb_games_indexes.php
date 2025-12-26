<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('thegamesdb_games', function (Blueprint $table): void {
            $table->dropUnique('thegamesdb_games_slug_platform_unique');
            $table->index(['slug', 'platform']);
        });
    }

    public function down(): void
    {
        Schema::table('thegamesdb_games', function (Blueprint $table): void {
            $table->dropIndex(['slug', 'platform']);
            $table->unique(['slug', 'platform']);
        });
    }
};
