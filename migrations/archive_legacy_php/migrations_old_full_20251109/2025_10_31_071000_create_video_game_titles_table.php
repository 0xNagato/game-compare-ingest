<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (Schema::hasTable('video_game_titles')) {
            return;
        }

        Schema::create('video_game_titles', function (Blueprint $table): void {
            $table->id();
            // Optional external unique identifier (not a primary key)
            $table->string('uid', 64)->nullable()->unique();
            // Relations
            if (Schema::hasTable('video_games')) {
                $table->foreignId('video_game_id')->nullable()->constrained('video_games')->nullOnDelete();
            } else {
                $table->unsignedBigInteger('video_game_id')->nullable()->index();
            }

            if (Schema::hasTable('video_game_sources')) {
                $table->foreignId('video_game_source_id')->nullable()->constrained('video_game_sources')->nullOnDelete();
            } else {
                $table->unsignedBigInteger('video_game_source_id')->nullable()->index();
            }

            // Provider item identifier is an opaque external ID (string) — do not FK to a non-existent provider_items table
            $table->string('provider_item_id', 191)->nullable();
            // Canonical display title (optional). Some providers only supply raw_title
            $table->string('title')->nullable();
            $table->string('normalized_title')->nullable();
            $table->string('locale', 41)->nullable();
            $table->json('payload')->nullable();
            $table->json('links')->nullable();
            $table->json('media')->nullable();
            $table->timestamp('synced_at')->nullable();
            $table->timestamps();

            $table->unique(['video_game_source_id', 'provider_item_id'], 'video_game_titles_source_item_unique');
            $table->index(['video_game_id']);
            $table->index(['normalized_title']);
            $table->index(['provider_item_id']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('video_game_titles');
    }
};
