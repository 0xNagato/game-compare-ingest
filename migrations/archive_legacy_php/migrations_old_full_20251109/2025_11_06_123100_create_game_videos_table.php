<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (Schema::hasTable('game_videos')) {
            return;
        }

        Schema::create('game_videos', function (Blueprint $table): void {
            $table->id();
            if (Schema::hasTable('game_providers')) {
                $table->foreignId('game_provider_id')->constrained('game_providers')->cascadeOnDelete();
            } else {
                $table->unsignedBigInteger('game_provider_id');
                $table->index('game_provider_id');
            }
            $table->string('video_key')->nullable();
            $table->string('name')->nullable();
            $table->text('description')->nullable();
            $table->string('site_detail_url')->nullable();
            $table->string('embed_url')->nullable();
            $table->string('stream_url')->nullable();
            $table->integer('duration_seconds')->nullable();
            $table->timestamp('published_at')->nullable();
            $table->json('thumbnails')->nullable();
            $table->json('metadata')->nullable();
            $table->timestamps();

            // Optional storage linkage to spatie media
            $table->unsignedBigInteger('media_id')->nullable();
            $table->string('storage_disk')->nullable();
            $table->string('storage_path')->nullable();

            $table->index(['game_provider_id', 'published_at'], 'game_videos_game_provider_id_published_at_index');
            $table->unique(['game_provider_id', 'video_key'], 'game_videos_game_provider_id_video_key_unique');
            $table->index('media_id');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('game_videos');
    }
};
