<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (! Schema::hasTable('steam_app_prices')) {
            Schema::create('steam_app_prices', function (Blueprint $table): void {
                $table->bigIncrements('id');
                $table->unsignedBigInteger('appid');
                $table->string('cc', 2);
                $table->string('currency', 8)->nullable();
                $table->integer('initial')->nullable();
                $table->integer('final')->nullable();
                $table->integer('discount_percent')->nullable();
                $table->string('initial_formatted', 32)->nullable();
                $table->string('final_formatted', 32)->nullable();
                $table->timestamp('last_synced_at')->nullable();
                $table->timestamps();

                $table->unique(['appid', 'cc']);
                $table->index('appid');
                $table->index('cc');
                $table->index('last_synced_at');
            });
        }
    }

    public function down(): void
    {
        Schema::dropIfExists('steam_app_prices');
    }
};
