<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (! Schema::hasTable('offer_regions')) {
            Schema::create('offer_regions', function (Blueprint $table) {
                $table->id();
                $table->foreignId('offer_id')->constrained()->cascadeOnDelete();
                $table->foreignId('jurisdiction_id')->constrained()->cascadeOnDelete();
                $table->foreignId('currency_id')->constrained('currencies');
                $table->foreignId('tax_rule_id')->nullable()->constrained('tax_rules')->nullOnDelete();
                $table->json('metadata')->nullable();
                $table->unique(['offer_id', 'jurisdiction_id']);
            });
        }

        if (! Schema::hasTable('current_prices')) {
            Schema::create('current_prices', function (Blueprint $table) {
                $table->foreignId('offer_region_id')->primary()->constrained('offer_regions')->cascadeOnDelete();
                $table->bigInteger('amount_minor');
                $table->timestampTz('recorded_at');
            });
        }

        Schema::table('offers', function (Blueprint $table) {
            if (! Schema::hasColumn('offers', 'product_version_id')) {
                $table->foreignId('product_version_id')->nullable()->constrained()->cascadeOnDelete()->after('sellable_id');
            }

            if (! Schema::hasColumn('offers', 'metadata')) {
                $table->json('metadata')->nullable()->after('is_active');
            }
        });

        DB::statement(<<<'SQL'
            CREATE UNIQUE INDEX IF NOT EXISTS offers_version_retailer_unique_idx
            ON offers (product_version_id, retailer_id, COALESCE(sku, ''))
        SQL);

        Schema::table('prices', function (Blueprint $table) {
            if (! Schema::hasColumn('prices', 'offer_region_id')) {
                $table->foreignId('offer_region_id')->nullable()->constrained('offer_regions')->cascadeOnDelete()->after('offer_jurisdiction_id');
            }
        });
    }

    public function down(): void
    {
        if (Schema::hasColumn('prices', 'offer_region_id')) {
            Schema::table('prices', function (Blueprint $table) {
                $table->dropConstrainedForeignId('offer_region_id');
            });
        }

        DB::statement('DROP INDEX IF EXISTS offers_version_retailer_unique_idx');

        Schema::table('offers', function (Blueprint $table) {
            if (Schema::hasColumn('offers', 'metadata')) {
                $table->dropColumn('metadata');
            }

            if (Schema::hasColumn('offers', 'product_version_id')) {
                $table->dropConstrainedForeignId('product_version_id');
            }
        });

        Schema::dropIfExists('current_prices');
        Schema::dropIfExists('offer_regions');
    }
};
