<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        // Ensure products has expected columns used by seeders
        if (Schema::hasTable('products')) {
            Schema::table('products', function (Blueprint $table): void {
                if (! Schema::hasColumn('products', 'uid')) {
                    $table->string('uid', 64)->nullable()->after('id')->index();
                }
                if (! Schema::hasColumn('products', 'primary_platform_family')) {
                    $table->string('primary_platform_family', 64)->nullable()->after('platform')->index();
                }
                if (! Schema::hasColumn('products', 'popularity_score')) {
                    $table->decimal('popularity_score', 6, 3)->nullable()->after('metadata');
                }
                if (! Schema::hasColumn('products', 'rating')) {
                    $table->decimal('rating', 6, 3)->nullable()->after('popularity_score');
                }
                if (! Schema::hasColumn('products', 'freshness_score')) {
                    $table->decimal('freshness_score', 6, 3)->nullable()->after('rating');
                }
                if (! Schema::hasColumn('products', 'external_ids')) {
                    $table->json('external_ids')->nullable()->after('metadata');
                }
                if (! Schema::hasColumn('products', 'synopsis')) {
                    $table->text('synopsis')->nullable()->after('external_ids');
                }
            });
        }

        // Ensure platforms table exists
        if (! Schema::hasTable('platforms')) {
            Schema::create('platforms', function (Blueprint $table): void {
                $table->id();
                $table->string('code')->unique();
                $table->string('name');
                $table->string('family')->nullable()->index();
                $table->json('metadata')->nullable();
                $table->timestamps();
            });
        }

        // Ensure genres table exists
        if (! Schema::hasTable('genres')) {
            Schema::create('genres', function (Blueprint $table): void {
                $table->id();
                $table->string('slug')->unique();
                $table->string('name');
                $table->foreignId('parent_id')->nullable()->constrained('genres')->nullOnDelete();
                $table->timestamps();
            });
        }

        // Ensure consoles table exists
        if (! Schema::hasTable('consoles')) {
            Schema::create('consoles', function (Blueprint $table): void {
                $table->id();
                $table->foreignId('product_id')->constrained()->cascadeOnDelete();
                $table->string('name');
                $table->string('manufacturer')->nullable();
                $table->date('release_date')->nullable();
                $table->json('metadata')->nullable();
                $table->timestamps();
                $table->index(['manufacturer', 'release_date']);
            });
        }

        // Ensure video_games table exists
        if (! Schema::hasTable('video_games')) {
            Schema::create('video_games', function (Blueprint $table): void {
                $table->id();
                $table->foreignId('product_id')->constrained()->cascadeOnDelete();
                $table->string('title');
                $table->string('genre')->nullable();
                $table->date('release_date')->nullable();
                $table->string('developer')->nullable();
                $table->json('metadata')->nullable();
                $table->json('external_ids')->nullable();
                $table->json('external_links')->nullable();
                $table->json('platform_codes')->nullable();
                $table->json('region_codes')->nullable();
                $table->json('title_keywords')->nullable();
                $table->timestamps();
                $table->index(['genre', 'release_date']);
            });
        }

        // Ensure game_platform pivot exists (Product <-> Platform)
        if (! Schema::hasTable('game_platform')) {
            Schema::create('game_platform', function (Blueprint $table): void {
                $table->id();
                $table->foreignId('product_id')->constrained()->cascadeOnDelete();
                $table->foreignId('platform_id')->constrained()->cascadeOnDelete();
                $table->timestamps();
                $table->unique(['product_id', 'platform_id']);
            });
        }

        // Ensure game_genre pivot exists (Product <-> Genre)
        if (! Schema::hasTable('game_genre')) {
            Schema::create('game_genre', function (Blueprint $table): void {
                $table->id();
                $table->foreignId('product_id')->constrained()->cascadeOnDelete();
                $table->foreignId('genre_id')->constrained()->cascadeOnDelete();
                $table->timestamps();
                $table->unique(['product_id', 'genre_id']);
            });
        }
    }

    public function down(): void
    {
        // Non-destructive: drop only tables we created if they exist
        foreach (['game_genre', 'game_platform', 'video_games', 'consoles', 'genres', 'platforms'] as $table) {
            if (Schema::hasTable($table)) {
                Schema::dropIfExists($table);
            }
        }

        if (Schema::hasTable('products')) {
            Schema::table('products', function (Blueprint $table): void {
                foreach (['synopsis', 'external_ids', 'freshness_score', 'rating', 'popularity_score', 'primary_platform_family', 'uid'] as $col) {
                    if (Schema::hasColumn('products', $col)) {
                        $table->dropColumn($col);
                    }
                }
            });
        }
    }
};
