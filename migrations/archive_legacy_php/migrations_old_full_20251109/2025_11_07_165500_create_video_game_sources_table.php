<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        if (Schema::hasTable('video_game_sources')) {
            return; // already created somewhere else
        }

        Schema::create('video_game_sources', function (Blueprint $table): void {
            $table->id();
            // Optional link to a specific video game (mirrors may be global; keep nullable)
            $table->foreignId('video_game_id')->nullable()->constrained('video_games')->nullOnDelete();
            // Canonical provider namespace (e.g., giantbomb, igdb, rawg, playstation_store)
            $table->string('provider_key', 64)->unique();
            $table->string('display_name')->nullable();
            $table->string('category', 64)->nullable(); // store|metadata|media|aggregator|fx|market
            $table->string('slug')->nullable();
            $table->json('metadata')->nullable();
            $table->timestamps();

            $table->index('slug', 'video_game_sources_slug_index');
            $table->index('category');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('video_game_sources');
    }
};
