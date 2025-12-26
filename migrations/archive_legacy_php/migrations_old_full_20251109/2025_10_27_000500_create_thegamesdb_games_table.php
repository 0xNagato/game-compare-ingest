<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('thegamesdb_games', function (Blueprint $table): void {
            $table->id();
            $table->unsignedBigInteger('external_id')->unique();
            $table->string('title');
            $table->string('slug');
            $table->string('platform')->nullable();
            $table->string('category')->nullable();
            $table->string('players')->nullable();
            $table->json('genres')->nullable();
            $table->string('developer')->nullable();
            $table->string('publisher')->nullable();
            $table->date('release_date')->nullable();
            $table->string('image_url')->nullable();
            $table->string('thumb_url')->nullable();
            $table->json('metadata')->nullable();
            $table->timestamp('last_synced_at')->nullable();
            $table->timestamps();

            $table->index('slug');
            $table->index('platform');
            $table->unique(['slug', 'platform']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('thegamesdb_games');
    }
};
