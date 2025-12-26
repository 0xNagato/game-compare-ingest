<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        // Drop children first to avoid FK issues
        Schema::dropIfExists('giant_bomb_game_product_media');
        Schema::dropIfExists('giant_bomb_game_images');
        Schema::dropIfExists('giant_bomb_game_videos');
        Schema::dropIfExists('giant_bomb_games');

        Schema::create('giant_bomb_games', function (Blueprint $table): void {
            $table->bigIncrements('id');

            // Canonical linkages (nullable; populated by backfill)
            $table->unsignedBigInteger('video_game_id')->nullable()->index();
            $table->unsignedBigInteger('video_game_source_id')->nullable()->index();
            $table->unsignedBigInteger('video_game_title_id')->nullable()->index();
            $table->string('provider_item_id')->nullable()->index(); // e.g. 3030-23054 or giantbomb:23054

            // Provider identifiers + basic fields
            $table->unsignedBigInteger('giantbomb_id')->nullable()->index();
            $table->string('guid', 64)->nullable()->unique();
            $table->string('slug')->nullable()->index();
            $table->string('name')->nullable();
            $table->string('normalized_name')->nullable()->index();
            $table->string('site_detail_url')->nullable();
            $table->text('deck')->nullable();
            $table->text('description')->nullable();
            $table->json('platforms')->nullable();
            $table->json('aliases')->nullable();

            // Image fields from provider payload
            $table->json('image')->nullable();
            $table->json('images')->nullable();
            $table->string('primary_image_url')->nullable();
            $table->string('image_super_url')->nullable();
            $table->string('image_small_url')->nullable();
            $table->string('image_original_url')->nullable();

            // Primary video metadata
            $table->string('primary_video_name')->nullable();
            $table->string('primary_video_high_url')->nullable();
            $table->string('primary_video_hd_url')->nullable();
            $table->unsignedInteger('video_count')->default(0);
            $table->json('videos')->nullable();
            $table->json('video_shows')->nullable();
            $table->json('themes')->nullable();
            $table->json('video_api_payloads')->nullable();
            $table->json('original_game_rating')->nullable();
            $table->json('raw_results')->nullable();

            // Import bookkeeping
            $table->string('payload_hash')->nullable();
            $table->timestamp('last_synced_at')->nullable();
            $table->string('primary_platform')->nullable();

            $table->json('metadata')->nullable();

            $table->timestamps();

            // Uniques to match registry invariants
            $table->unique(['video_game_source_id', 'provider_item_id'], 'gbg_unique_source_item');
            $table->unique('video_game_id', 'gbg_unique_video_game');

            // FKs (soft; ignore engine differences)
            $table->foreign('video_game_id')->references('id')->on('video_games')->onDelete('set null');
            $table->foreign('video_game_source_id')->references('id')->on('video_game_sources')->onDelete('set null');
            $table->foreign('video_game_title_id')->references('id')->on('video_game_titles')->onDelete('set null');
        });

        Schema::create('giant_bomb_game_images', function (Blueprint $table): void {
            $table->bigIncrements('id');
            $table->unsignedBigInteger('giant_bomb_game_id')->index();

            // Registry linkage on children too for convenience
            $table->unsignedBigInteger('video_game_source_id')->nullable()->index();
            $table->unsignedBigInteger('video_game_title_id')->nullable()->index();
            $table->string('provider_item_id')->nullable()->index();

            // Common fields expected by tests/importers
            $table->string('source')->nullable();
            $table->string('name')->nullable();
            $table->string('original_url')->nullable();
            $table->string('super_url')->nullable();
            $table->string('small_url')->nullable();
            $table->json('metadata')->nullable();
            $table->timestamps();

            $table->foreign('giant_bomb_game_id')->references('id')->on('giant_bomb_games')->onDelete('cascade');
        });

        Schema::create('giant_bomb_game_videos', function (Blueprint $table): void {
            $table->bigIncrements('id');
            $table->unsignedBigInteger('giant_bomb_game_id')->index();

            // Registry linkage on children too for convenience
            $table->unsignedBigInteger('video_game_source_id')->nullable()->index();
            $table->unsignedBigInteger('video_game_title_id')->nullable()->index();
            $table->string('provider_item_id')->nullable()->index();

            $table->string('source')->nullable();
            $table->string('guid')->nullable()->index();
            $table->string('name')->nullable();
            // Common URL fields used by tests/importers
            $table->string('high_url')->nullable();
            $table->string('hd_url')->nullable();
            // Keep generic slots too for other providers
            $table->string('stream_url')->nullable();
            $table->string('preview_url')->nullable();
            $table->json('metadata')->nullable();
            $table->timestamps();

            $table->foreign('giant_bomb_game_id')->references('id')->on('giant_bomb_games')->onDelete('cascade');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('giant_bomb_game_images');
        Schema::dropIfExists('giant_bomb_game_videos');
        Schema::dropIfExists('giant_bomb_games');
    }
};
