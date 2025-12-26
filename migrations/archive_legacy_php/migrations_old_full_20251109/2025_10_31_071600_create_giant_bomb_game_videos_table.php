<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (Schema::hasTable('giant_bomb_game_videos')) {
            return;
        }

        Schema::create('giant_bomb_game_videos', function (Blueprint $table): void {
            $table->id();
            $table->foreignId('giant_bomb_game_id')->constrained()->cascadeOnDelete();
            $table->string('source', 64);
            $table->unsignedSmallInteger('ordinal')->default(0);
            $table->string('video_id', 128)->nullable();
            $table->string('guid', 128)->nullable();
            $table->string('name')->nullable();
            $table->text('deck')->nullable();
            $table->string('site_detail_url')->nullable();
            $table->string('api_detail_url')->nullable();
            $table->string('url')->nullable();
            $table->string('hd_url')->nullable();
            $table->string('high_url')->nullable();
            $table->string('low_url')->nullable();
            $table->string('embed_player')->nullable();
            $table->string('playable_url')->nullable();
            $table->string('poster_url')->nullable();
            $table->integer('length_seconds')->nullable();
            $table->timestamp('publish_date')->nullable();
            $table->string('video_type')->nullable();
            $table->string('video_show')->nullable();
            $table->json('metadata')->nullable();
            $table->timestamps();

            $table->unique(['giant_bomb_game_id', 'source', 'ordinal']);
            $table->index(['video_id']);
            $table->index(['guid']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('giant_bomb_game_videos');
    }
};
