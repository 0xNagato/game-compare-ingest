<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (Schema::hasTable('game_media')) {
            return;
        }

        Schema::create('game_media', function (Blueprint $table): void {
            $table->id();
            // legacy pivot to VideoGame
            if (Schema::hasTable('video_games')) {
                $table->foreignId('video_game_id')->constrained('video_games')->cascadeOnDelete();
            } else {
                $table->unsignedBigInteger('video_game_id');
                $table->index('video_game_id');
            }

            $table->string('kind')->default('default');
            $table->string('slug')->nullable();
            $table->string('title')->nullable();
            $table->text('description')->nullable();
            $table->timestamp('fetched_at')->nullable();
            $table->text('metadata')->nullable();
            $table->text('stats')->nullable();
            $table->timestamps();

            $table->unique(['video_game_id', 'kind', 'slug'], 'game_media_video_game_id_kind_slug_unique');
            $table->index(['video_game_id', 'kind'], 'game_media_video_game_id_kind_index');
        });
    }

    public function down(): void
    {
        if (! Schema::hasTable('game_media')) {
            return;
        }

        Schema::dropIfExists('game_media');
    }
};
