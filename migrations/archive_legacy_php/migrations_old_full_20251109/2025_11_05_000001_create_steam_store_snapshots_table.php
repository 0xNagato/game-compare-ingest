<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        Schema::create('steam_store_snapshots', function (Blueprint $table) {
            $table->id();
            $table->unsignedBigInteger('app_id');
            $table->string('country', 2);
            $table->string('currency', 3);
            $table->string('language', 32)->nullable();
            $table->json('payload');
            $table->string('payload_hash', 64);
            $table->timestamp('fetched_at')->useCurrent();
            $table->timestamps();

            $table->index(['app_id']);
            $table->index(['country']);
            $table->index(['currency']);
            $table->index(['language']);
            $table->index(['fetched_at']);
            $table->unique(['app_id', 'country', 'currency', 'payload_hash'], 'steam_snapshot_app_market_hash_unique');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('steam_store_snapshots');
    }
};
