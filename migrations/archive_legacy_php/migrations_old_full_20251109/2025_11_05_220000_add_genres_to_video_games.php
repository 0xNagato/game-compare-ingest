<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (Schema::hasTable('video_games') && ! Schema::hasColumn('video_games', 'genres')) {
            Schema::table('video_games', function (Blueprint $table): void {
                $table->json('genres')->nullable()->after('genre');
            });
        }
    }

    public function down(): void
    {
        if (Schema::hasTable('video_games') && Schema::hasColumn('video_games', 'genres')) {
            Schema::table('video_games', function (Blueprint $table): void {
                $table->dropColumn('genres');
            });
        }
    }
};
