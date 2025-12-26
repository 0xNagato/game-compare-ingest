<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('game_prices', function (Blueprint $table): void {
            $table->id();
            $table->foreignId('video_game_id')->constrained()->cascadeOnDelete();
            $table->string('slug');
            $table->string('base_currency', 3)->default('USD');
            $table->boolean('tax_inclusive')->default(true);
            $table->timestamp('fetched_at')->nullable();
            $table->json('metadata')->nullable();
            $table->json('stats')->nullable();
            $table->timestamps();

            $table->unique(['video_game_id', 'slug']);
            $table->index(['slug']);
        });

        if (! Schema::hasTable('game_retailers')) {
            Schema::create('game_retailers', function (Blueprint $table): void {
                $table->id();
                if (Schema::hasTable('game_providers')) {
                    $table->foreignId('game_provider_id')->constrained('game_providers')->cascadeOnDelete();
                } else {
                    $table->unsignedBigInteger('game_provider_id')->index();
                }
                $table->foreignId('game_price_id')->nullable()->constrained('game_prices')->cascadeOnDelete();
                $table->foreignId('video_game_source_id')->nullable()->constrained()->nullOnDelete();
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
                $table->index(['game_price_id']);
                $table->index(['region_code']);
                $table->index(['currency_code']);
            });
        }

        Schema::create('game_price_points', function (Blueprint $table): void {
            $table->id();
            $table->foreignId('game_retailer_id')->constrained('game_retailers')->cascadeOnDelete();
            $table->timestamp('collected_at');
            $table->timestamp('effective_at')->nullable();
            $table->string('currency_code', 3);
            $table->unsignedBigInteger('amount_minor')->nullable();
            $table->unsignedBigInteger('btc_value_sats')->nullable();
            $table->boolean('is_sale')->default(false);
            $table->json('metadata')->nullable();
            $table->timestamps();

            $table->unique(['game_retailer_id', 'collected_at', 'currency_code']);
            $table->index(['collected_at']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('game_price_points');
        Schema::dropIfExists('game_retailers');
        Schema::dropIfExists('game_prices');
    }
};
