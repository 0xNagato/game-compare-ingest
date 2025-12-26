<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (Schema::hasTable('giant_bomb_game_images')) {
            return;
        }

        Schema::create('giant_bomb_game_images', function (Blueprint $table): void {
            $table->id();
            $table->foreignId('giant_bomb_game_id')->constrained()->cascadeOnDelete();
            $table->string('source', 64);
            $table->unsignedSmallInteger('ordinal')->default(0);
            $table->string('name')->nullable();
            $table->string('tag')->nullable();
            $table->string('original_url')->nullable();
            $table->string('super_url')->nullable();
            $table->string('screen_url')->nullable();
            $table->string('medium_url')->nullable();
            $table->string('small_url')->nullable();
            $table->string('thumb_url')->nullable();
            $table->string('tiny_url')->nullable();
            $table->string('icon_url')->nullable();
            $table->json('variants')->nullable();
            $table->json('metadata')->nullable();
            $table->timestamps();

            $table->unique(['giant_bomb_game_id', 'source', 'ordinal']);
            $table->index(['source', 'tag']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('giant_bomb_game_images');
    }
};
