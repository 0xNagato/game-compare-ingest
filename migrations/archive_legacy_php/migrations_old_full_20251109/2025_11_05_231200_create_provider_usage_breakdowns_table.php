<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        Schema::create('provider_usage_breakdowns', function (Blueprint $table) {
            $table->id();
            $table->string('provider');
            $table->string('target_table');
            $table->unsignedBigInteger('total_rows')->default(0);
            $table->timestamp('last_event_at')->nullable();
            $table->timestamps();

            $table->unique(['provider', 'target_table']);
            $table->index(['target_table']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('provider_usage_breakdowns');
    }
};
