<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (Schema::hasTable('game_providers')) {
            return;
        }

        Schema::create('game_providers', function (Blueprint $table): void {
            $table->id();
            // Stable identifier used across the codebase (matches model's provider_key)
            $table->string('provider_key', 64)->unique();
            $table->string('name')->nullable();
            $table->string('slug')->nullable()->index();
            $table->string('website_url')->nullable();

            // Polymorphic link to the underlying entity this provider entry is bound to
            $table->nullableMorphs('providable'); // provides providable_type + providable_id

            // Opaque credentials/config and metadata from the provider
            $table->json('credentials')->nullable();
            $table->json('metadata')->nullable();

            // Sync bookkeeping
            $table->timestamp('last_synced_at')->nullable();
            $table->timestamp('refreshed_at')->nullable();

            $table->timestamps();
        });


    }

    public function down(): void
    {
        Schema::dropIfExists('game_providers');
    }
};
