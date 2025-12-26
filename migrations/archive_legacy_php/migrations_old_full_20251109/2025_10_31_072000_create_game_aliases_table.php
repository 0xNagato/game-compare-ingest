<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (! Schema::hasTable('game_aliases')) {
            Schema::create('game_aliases', function (Blueprint $table): void {
                $table->id();
                $table->foreignId('product_id')->constrained()->cascadeOnDelete();
                $table->foreignId('video_game_id')->nullable()->constrained('video_games')->nullOnDelete();
                $table->string('provider');
                $table->string('provider_game_id');
                $table->string('alias_title');
                $table->string('alias_slug')->nullable()->index();
                $table->foreignId('video_game_source_id')->nullable()->constrained('video_game_sources')->nullOnDelete();
                $table->foreignId('video_game_title_id')->nullable()->constrained('video_game_titles')->nullOnDelete();
                $table->json('metadata')->nullable();
                $table->timestamps();

                $table->unique(['provider', 'provider_game_id'], 'game_aliases_provider_provider_game_id_unique');
                $table->index(['product_id', 'provider'], 'game_aliases_product_id_provider_index');
            });
        }
    }

    public function down(): void
    {
        Schema::dropIfExists('game_aliases');

    }
};
