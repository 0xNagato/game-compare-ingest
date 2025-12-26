<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('product_versions', function (Blueprint $table) {
            $table->id();
            $table->foreignId('product_id')->constrained()->cascadeOnDelete();
            $table->foreignId('platform_id')->nullable()->constrained()->nullOnDelete();
            $table->string('edition')->nullable();
            $table->string('form_factor')->nullable();
            $table->date('release_date')->nullable();
            $table->json('metadata')->nullable();
            $table->timestampsTz();
        });

        Schema::table('products', function (Blueprint $table) {
            if (! Schema::hasColumn('products', 'kind')) {
                $table->enum('kind', ['software', 'hardware'])->nullable()->after('id');
            }
        });

        DB::statement(<<<'SQL'
            CREATE UNIQUE INDEX IF NOT EXISTS product_versions_unique_idx
            ON product_versions (product_id, COALESCE(platform_id, 0), COALESCE(edition, ''))
        SQL);
    }

    public function down(): void
    {
        DB::statement('DROP INDEX IF EXISTS product_versions_unique_idx');

        Schema::dropIfExists('product_versions');

        Schema::table('products', function (Blueprint $table) {
            if (Schema::hasColumn('products', 'kind')) {
                $table->dropColumn('kind');
            }
        });
    }
};