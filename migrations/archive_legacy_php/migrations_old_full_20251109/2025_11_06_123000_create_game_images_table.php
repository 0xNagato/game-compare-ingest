<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (Schema::hasTable('game_images')) {
            return;
        }

        Schema::create('game_images', function (Blueprint $table): void {
            $table->id();
            if (Schema::hasTable('game_providers')) {
                $table->foreignId('game_provider_id')->constrained('game_providers')->cascadeOnDelete();
            } else {
                $table->unsignedBigInteger('game_provider_id');
                $table->index('game_provider_id');
            }
            $table->string('image_key')->nullable();
            $table->string('url');
            $table->string('mime_type')->nullable();
            $table->integer('width')->nullable();
            $table->integer('height')->nullable();
            $table->integer('rank')->default(0);
            $table->string('caption')->nullable();
            $table->json('variants')->nullable();
            $table->json('metadata')->nullable();
            $table->timestamps();

            // Optional storage linkage to spatie media
            $table->unsignedBigInteger('media_id')->nullable();
            $table->string('storage_disk')->nullable();
            $table->string('storage_path')->nullable();

            $table->unique(['game_provider_id', 'image_key'], 'game_images_game_provider_id_image_key_unique');
            $table->index(['game_provider_id', 'rank'], 'game_images_game_provider_id_rank_index');
            $table->index('media_id');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('game_images');
    }
};
