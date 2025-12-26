<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (! Schema::hasTable('providers')) {
            Schema::create('providers', function (Blueprint $table) {
                $table->id();
                $table->string('code')->unique();
                $table->string('name');
                $table->enum('kind', ['retailer_api', 'catalog', 'media', 'pricing_api']);
                $table->json('metadata')->nullable();
                $table->timestampsTz();
            });
        }

        if (! Schema::hasTable('provider_items')) {
            Schema::create('provider_items', function (Blueprint $table) {
                $table->id();
                $table->foreignId('provider_id')->constrained()->cascadeOnDelete();
                $table->string('external_id');
                $table->foreignId('product_version_id')->nullable()->constrained()->nullOnDelete();
                $table->foreignId('offer_id')->nullable()->constrained()->nullOnDelete();
                $table->timestampTz('last_synced_at')->nullable();
                $table->string('payload_hash')->nullable();
                $table->json('metadata')->nullable();
                $table->timestampsTz();
                $table->unique(['provider_id', 'external_id']);
            });
        }

        if (! Schema::hasTable('provider_runs')) {
            Schema::create('provider_runs', function (Blueprint $table) {
                $table->id();
                $table->foreignId('provider_id')->constrained()->cascadeOnDelete();
                $table->timestampTz('started_at')->useCurrent();
                $table->timestampTz('finished_at')->nullable();
                $table->enum('status', ['queued', 'running', 'succeeded', 'failed', 'partial']);
                $table->integer('item_count')->nullable();
                $table->json('error_summary')->nullable();
                $table->timestampsTz();
            });
        }

        DB::statement('CREATE INDEX IF NOT EXISTS provider_items_provider_id_idx ON provider_items (provider_id)');
        DB::statement('CREATE INDEX IF NOT EXISTS provider_items_offer_id_idx ON provider_items (offer_id)');
        DB::statement('CREATE INDEX IF NOT EXISTS provider_runs_provider_started_idx ON provider_runs (provider_id, started_at DESC)');
    }

    public function down(): void
    {
        DB::statement('DROP INDEX IF EXISTS provider_runs_provider_started_idx');
        DB::statement('DROP INDEX IF EXISTS provider_items_offer_id_idx');
        DB::statement('DROP INDEX IF EXISTS provider_items_provider_id_idx');

        Schema::dropIfExists('provider_runs');
        Schema::dropIfExists('provider_items');
        Schema::dropIfExists('providers');
    }
};
