<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('giant_bomb_games', function (Blueprint $table): void {
            $table->id();
            $table->foreignId('video_game_id')->nullable()->constrained()->cascadeOnDelete();
            $table->string('guid')->unique();
            $table->unsignedBigInteger('giantbomb_id')->nullable()->index();
            $table->string('name');
            $table->string('slug')->nullable()->index();
            $table->string('site_detail_url')->nullable();
            $table->string('deck', 512)->nullable();
            $table->longText('description')->nullable();
            $table->json('platforms')->nullable();
            $table->json('aliases')->nullable();
            $table->string('primary_image_url')->nullable();
            $table->string('image_super_url')->nullable();
            $table->string('image_small_url')->nullable();
            $table->string('image_original_url')->nullable();
            $table->string('normalized_name')->nullable()->index();
            $table->string('payload_hash', 64)->nullable();
            $table->timestamp('last_synced_at')->nullable()->index();
            $table->timestamps();

            $table->unique('video_game_id');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('giant_bomb_games');
    }
};
