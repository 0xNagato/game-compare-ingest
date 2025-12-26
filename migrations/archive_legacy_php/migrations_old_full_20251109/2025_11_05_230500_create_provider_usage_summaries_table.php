<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        Schema::create('provider_usage_summaries', function (Blueprint $table) {
            $table->id();
            $table->string('provider')->unique();
            $table->unsignedBigInteger('total_calls')->default(0);
            $table->unsignedInteger('daily_calls')->default(0);
            $table->date('daily_window')->nullable();
            $table->timestamp('last_called_at')->nullable();
            $table->timestamps();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('provider_usage_summaries');
    }
};
