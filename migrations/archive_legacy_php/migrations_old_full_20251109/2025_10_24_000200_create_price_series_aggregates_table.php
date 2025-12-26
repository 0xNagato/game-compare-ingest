<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (Schema::hasTable('price_series_aggregates')) {
            return; // idempotent: table already exists
        }

        Schema::create('price_series_aggregates', function (Blueprint $table) {
            $table->id();
            $table->foreignId('product_id')->constrained()->cascadeOnDelete();
            $table->string('region_code', 2);
            $table->string('bucket', 10);
            $table->timestamp('window_start');
            $table->timestamp('window_end');
            $table->boolean('tax_inclusive');
            $table->decimal('min_btc', 18, 8);
            $table->decimal('max_btc', 18, 8);
            $table->decimal('avg_btc', 18, 8);
            $table->decimal('min_fiat', 12, 2);
            $table->decimal('max_fiat', 12, 2);
            $table->decimal('avg_fiat', 12, 2);
            $table->unsignedInteger('sample_count');
            $table->json('metadata')->nullable();
            $table->timestamps();

            $table->unique(['product_id', 'region_code', 'bucket', 'window_start', 'tax_inclusive'], 'price_series_unique');
            $table->index(['product_id', 'bucket', 'window_start']);
            $table->index(['region_code', 'bucket']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('price_series_aggregates');
    }
};
