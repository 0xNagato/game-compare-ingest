<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        Schema::create('cross_reference_entries', function (Blueprint $table): void {
            $table->id();
            $table->string('normalized_key')->unique();
            $table->string('name');
            $table->string('image_url')->nullable();
            $table->boolean('has_digital')->default(false);
            $table->boolean('has_physical')->default(false);
            $table->json('platforms')->nullable();
            $table->json('currencies')->nullable();
            $table->json('digital_payload')->nullable();
            $table->json('physical_payload')->nullable();
            $table->json('best_digital')->nullable();
            $table->json('best_physical')->nullable();
            $table->timestamps();

            $table->index('has_digital');
            $table->index('has_physical');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('cross_reference_entries');
    }
};
