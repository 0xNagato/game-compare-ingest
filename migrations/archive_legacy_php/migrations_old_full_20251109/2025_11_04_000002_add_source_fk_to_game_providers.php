<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('game_providers', function (Blueprint $table): void {
            if (! Schema::hasColumn('game_providers', 'video_game_source_id')) {
                $table->foreignId('video_game_source_id')->nullable()->after('providable_id')->constrained('video_game_sources')->nullOnDelete();
                $table->index('video_game_source_id');
            }
        });
    }

    public function down(): void
    {
        Schema::table('game_providers', function (Blueprint $table): void {
            if (Schema::hasColumn('game_providers', 'video_game_source_id')) {
                $table->dropConstrainedForeignId('video_game_source_id');
            }
        });
    }
};
