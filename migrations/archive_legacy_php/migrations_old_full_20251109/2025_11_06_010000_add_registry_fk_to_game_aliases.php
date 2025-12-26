<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        if (Schema::hasTable('game_aliases')) {
            Schema::table('game_aliases', function (Blueprint $table): void {
                if (! Schema::hasColumn('game_aliases', 'video_game_source_id')) {
                    $table->foreignId('video_game_source_id')->nullable()->after('video_game_id')
                        ->constrained('video_game_sources')->nullOnDelete();
                }
                if (! Schema::hasColumn('game_aliases', 'video_game_title_id')) {
                    $table->foreignId('video_game_title_id')->nullable()->after('video_game_source_id')
                        ->constrained('video_game_titles')->nullOnDelete();
                }
            });
        }
    }

    public function down(): void
    {
        if (Schema::hasTable('game_aliases')) {
            Schema::table('game_aliases', function (Blueprint $table): void {
                try {
                    $table->dropConstrainedForeignId('video_game_title_id');
                } catch (\Throwable $e) {
                    // ignore if missing
                }
                try {
                    $table->dropConstrainedForeignId('video_game_source_id');
                } catch (\Throwable $e) {
                    // ignore if missing
                }
            });
        }
    }
};
