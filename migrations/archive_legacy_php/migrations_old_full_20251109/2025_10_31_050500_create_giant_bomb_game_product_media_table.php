<?php

use App\Models\GiantBombGame;
use App\Models\ProductMedia;
use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (Schema::hasTable('giant_bomb_game_product_media')) {
            return;
        }

        Schema::create('giant_bomb_game_product_media', function (Blueprint $table): void {
            $table->id();
            $table->foreignIdFor(GiantBombGame::class)->constrained()->cascadeOnDelete();
            $table->foreignIdFor(ProductMedia::class)->constrained()->cascadeOnDelete();
            $table->timestamps();
            $table->unique(['giant_bomb_game_id', 'product_media_id'], 'gb_game_product_media_unique');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('giant_bomb_game_product_media');
    }
};