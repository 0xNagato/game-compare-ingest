<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (Schema::hasTable('game_retailers')) {
            return;
        }

        Schema::create('game_retailers', function (Blueprint $table): void {
            $table->id();
            $table->unsignedBigInteger('game_provider_id')->index();
            $table->unsignedBigInteger('game_price_id')->nullable()->index();
            $table->unsignedBigInteger('video_game_source_id')->nullable()->index();
            $table->string('provider_item_id')->nullable();
            $table->string('retailer_key');
            $table->string('name')->nullable();
            $table->string('slug')->nullable();
            $table->string('country_code', 2)->nullable();
            $table->string('region_code', 5)->nullable();
            $table->string('currency_code', 3)->nullable();
            $table->json('metadata')->nullable();
            $table->json('provider_payload')->nullable();
            $table->timestamps();

            $table->unique(['game_provider_id', 'retailer_key']);
            $table->index(['region_code']);
            $table->index(['currency_code']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('game_retailers');
    }
};
