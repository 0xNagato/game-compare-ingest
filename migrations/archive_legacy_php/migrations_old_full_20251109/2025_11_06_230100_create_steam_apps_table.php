<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        Schema::create('steam_apps', function (Blueprint $table): void {
            $table->bigIncrements('id');
            $table->unsignedBigInteger('appid')->unique();
            $table->string('name')->index();
            $table->string('type')->nullable()->index();
            $table->boolean('is_free')->default(false);

            // Common store fields (nullable, many kept as JSON to reduce churn)
            $table->string('header_image')->nullable();
            $table->json('price_overview')->nullable();
            $table->json('release_date')->nullable();
            $table->json('platforms')->nullable();
            $table->json('genres')->nullable();
            $table->json('developers')->nullable();
            $table->json('publishers')->nullable();
            $table->json('screenshots')->nullable();
            $table->json('movies')->nullable();
            $table->json('raw')->nullable(); // raw appdetails payload snapshot

            // Registry linkage
            $table->unsignedBigInteger('video_game_id')->nullable()->index();
            $table->unsignedBigInteger('video_game_source_id')->nullable()->index();
            $table->unsignedBigInteger('video_game_title_id')->nullable()->index();

            $table->timestamp('last_synced_at')->nullable()->index();
            $table->timestamps();

            $table->index(['video_game_id', 'appid']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('steam_apps');
    }
};
