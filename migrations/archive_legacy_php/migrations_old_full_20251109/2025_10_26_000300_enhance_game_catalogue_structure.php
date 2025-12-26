<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Facades\DB;

return new class extends Migration
{
    public function up(): void
    {
        // Legacy safeguard: this migration targeted an older schema (VideoGameTitles/products).
        // If those legacy tables are not present, skip the entire migration to remain compatible with the new structure.
        if (! Schema::hasTable('VideoGameTitles')) {
            return;
        }

        // Add/alter products columns idempotently (safe for re-runs)
        if (Schema::hasTable('VideoGameTitles') && ! Schema::hasColumn('VideoGameTitles', 'uid')) {
            Schema::table('VideoGameTitles', function (Blueprint $table) {
                $table->string('uid', 64)->nullable()->after('id');
            });
        }

        if (Schema::hasTable('VideoGameTitles') && ! Schema::hasColumn('VideoGameTitles', 'synopsis')) {
            Schema::table('VideoGameTitles', function (Blueprint $table) {
                $table->text('synopsis')->nullable()->after('category');
            });
        }

        if (Schema::hasTable('VideoGameTitles') && ! Schema::hasColumn('VideoGameTitles', 'primary_platform_family')) {
            Schema::table('VideoGameTitles', function (Blueprint $table) {
                $table->string('primary_platform_family', 32)->nullable()->after('platform');
            });
        }

        if (Schema::hasTable('VideoGameTitles') && ! Schema::hasColumn('VideoGameTitles', 'popularity_score')) {
            Schema::table('VideoGameTitles', function (Blueprint $table) {
                $table->decimal('popularity_score', 8, 3)->default(0)->after('metadata');
            });
        }

        if (Schema::hasTable('VideoGameTitles') && ! Schema::hasColumn('VideoGameTitles', 'rating')) {
            Schema::table('VideoGameTitles', function (Blueprint $table) {
                $table->decimal('rating', 5, 2)->default(0)->after('popularity_score');
            });
        }

        if (Schema::hasTable('VideoGameTitles') && ! Schema::hasColumn('VideoGameTitles', 'freshness_score')) {
            Schema::table('VideoGameTitles', function (Blueprint $table) {
                $table->decimal('freshness_score', 6, 3)->default(0)->after('rating');
            });
        }

        if (Schema::hasTable('VideoGameTitles') && ! Schema::hasColumn('VideoGameTitles', 'external_ids')) {
            Schema::table('VideoGameTitles', function (Blueprint $table) {
                $table->json('external_ids')->nullable()->after('metadata');
            });
        }

        // Create indexes idempotently (Postgres-safe). Other drivers fall back to Schema methods
        $driver = Schema::getConnection()->getDriverName();

        if ($driver === 'pgsql') {
            // Skip creating indexes on legacy 'products' table in new structure
        } else {
            // Skip for non-pgsql as well under new structure
        }

        if (! Schema::hasTable('platforms')) {
            Schema::create('platforms', function (Blueprint $table) {
                $table->id();
                $table->string('code', 32)->unique();
                $table->string('name');
                $table->string('family', 32);
                $table->json('metadata')->nullable();
                $table->timestamps();

                $table->index('family');
            });
        }

        if (! Schema::hasTable('game_platform')) {
            Schema::create('game_platform', function (Blueprint $table) {
                $table->foreignId('product_id')->constrained()->cascadeOnDelete();
                $table->foreignId('platform_id')->constrained()->cascadeOnDelete();
                $table->timestamps();

                $table->primary(['product_id', 'platform_id']);
            });
        }

        if (! Schema::hasTable('genres')) {
            Schema::create('genres', function (Blueprint $table) {
                $table->id();
                $table->string('slug')->unique();
                $table->string('name');
                $table->foreignId('parent_id')->nullable()->constrained('genres')->nullOnDelete();
                $table->timestamps();
            });
        }

        if (! Schema::hasTable('game_genre')) {
            Schema::create('game_genre', function (Blueprint $table) {
                $table->foreignId('product_id')->constrained()->cascadeOnDelete();
                $table->foreignId('genre_id')->constrained()->cascadeOnDelete();
                $table->timestamps();

                $table->primary(['product_id', 'genre_id']);
            });
        }
    }

    public function down(): void
    {
        Schema::dropIfExists('game_genre');
        Schema::dropIfExists('genres');
        Schema::dropIfExists('game_platform');
        Schema::dropIfExists('platforms');

        // Drop product indexes/columns defensively
        $driver = Schema::getConnection()->getDriverName();
        if ($driver === 'pgsql') {
            // Dropping columns will also drop dependent indexes, but remove explicitly if present
            DB::statement('DROP INDEX IF EXISTS products_uid_unique');
            DB::statement('DROP INDEX IF EXISTS products_primary_platform_family_index');
            DB::statement('DROP INDEX IF EXISTS products_popularity_score_rating_index');
        }

        // Drop columns only if they exist
        foreach ([
            'external_ids',
            'freshness_score',
            'rating',
            'popularity_score',
            'primary_platform_family',
            'synopsis',
            'uid',
        ] as $column) {
            if (Schema::hasColumn('products', $column)) {
                Schema::table('products', function (Blueprint $table) use ($column) {
                    $table->dropColumn([$column]);
                });
            }
        }
    }
};
