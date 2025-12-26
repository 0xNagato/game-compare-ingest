<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        if (! Schema::hasTable('products')) {
            Schema::create('products', function (Blueprint $table) {
                $table->id();
                $table->string('name');
                $table->string('platform');
                $table->string('slug')->unique();
                $table->string('category')->nullable();
                $table->date('release_date')->nullable();
                $table->json('metadata')->nullable();
                $table->timestamps();

                $table->index(['platform', 'category']);
            });
        }

        if (! Schema::hasTable('sku_regions')) {
            Schema::create('sku_regions', function (Blueprint $table) {
                $table->id();
                $table->foreignId('product_id')->constrained()->cascadeOnDelete();
                $table->string('region_code', 2);
                $table->string('retailer');
                $table->string('currency', 3);
                $table->string('sku')->nullable();
                $table->boolean('is_active')->default(true);
                $table->json('metadata')->nullable();
                $table->timestamps();

                $table->unique(['product_id', 'region_code', 'retailer']);
                $table->index(['region_code', 'retailer']);
                $table->index('is_active');
            });
        }

        if (! Schema::hasTable('region_prices')) {
            Schema::create('region_prices', function (Blueprint $table) {
                $table->id();
                $table->foreignId('sku_region_id')->constrained()->cascadeOnDelete();
                $table->timestamp('recorded_at');
                $table->decimal('fiat_amount', 12, 2);
                $table->decimal('btc_value', 18, 8);
                $table->boolean('tax_inclusive');
                $table->decimal('fx_rate_snapshot', 14, 6);
                $table->decimal('btc_rate_snapshot', 18, 8);
                $table->json('raw_payload')->nullable();
                $table->timestamps();

                $table->index(['sku_region_id', 'recorded_at']);
                $table->index(['recorded_at', 'btc_value']);
                $table->index('tax_inclusive');
            });
        }

        if (! Schema::hasTable('exchange_rates')) {
            Schema::create('exchange_rates', function (Blueprint $table) {
                $table->id();
                $table->string('base_currency', 3);
                $table->string('quote_currency', 6);
                $table->decimal('rate', 18, 8);
                $table->timestamp('fetched_at');
                $table->string('provider');
                $table->json('metadata')->nullable();
                $table->timestamps();

                $table->index(['base_currency', 'quote_currency', 'fetched_at']);
                $table->index(['provider', 'fetched_at']);
            });
        }

        if (! Schema::hasTable('tax_profiles')) {
            Schema::create('tax_profiles', function (Blueprint $table) {
                $table->id();
                $table->string('region_code', 2);
                $table->decimal('vat_rate', 5, 2);
                $table->date('effective_from');
                $table->text('notes')->nullable();
                $table->timestamps();

                $table->index(['region_code', 'effective_from']);
            });
        }

        if (! Schema::hasTable('alerts')) {
            Schema::create('alerts', function (Blueprint $table) {
                $table->id();
                $table->foreignId('user_id')->constrained()->cascadeOnDelete();
                $table->foreignId('product_id')->constrained()->cascadeOnDelete();
                $table->string('region_code', 2);
                $table->decimal('threshold_btc', 18, 8);
                $table->enum('comparison_operator', ['below', 'above'])->default('below');
                $table->enum('channel', ['email', 'discord']);
                $table->boolean('is_active')->default(true);
                $table->timestamp('last_triggered_at')->nullable();
                $table->json('settings')->nullable();
                $table->timestamps();

                $table->index(['product_id', 'region_code', 'is_active']);
                $table->index(['user_id', 'is_active']);
            });
        }

    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('alerts');
        Schema::dropIfExists('tax_profiles');
        Schema::dropIfExists('exchange_rates');
        Schema::dropIfExists('region_prices');
        Schema::dropIfExists('sku_regions');
        Schema::dropIfExists('products');
    }
};
