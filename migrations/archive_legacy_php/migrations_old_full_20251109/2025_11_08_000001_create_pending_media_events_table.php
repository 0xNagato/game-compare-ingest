<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('pending_media_events', function (Blueprint $table): void {
            $table->bigIncrements('id');
            $table->unsignedBigInteger('video_game_id')->index();
            $table->string('provider_key', 64)->index();
            $table->json('context')->nullable();
            $table->timestamp('processed_at')->nullable()->index();
            $table->timestamps();

            // Optional FK if video_games table exists in all envs
            if (Schema::hasTable('video_games')) {
                $table->foreign('video_game_id')->references('id')->on('video_games')->onDelete('cascade');
            }

            $table->index(['video_game_id', 'provider_key', 'processed_at'], 'pending_media_events_lookup');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('pending_media_events');
    }
};
