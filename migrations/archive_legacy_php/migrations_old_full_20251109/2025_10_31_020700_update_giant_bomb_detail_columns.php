<?php

use App\Models\GiantBombGame;
use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('giant_bomb_games', function (Blueprint $table): void {
            $table->string('primary_platform')->nullable()->after('site_detail_url');
            $table->json('image')->nullable()->after('image_original_url');
            $table->json('images')->nullable()->after('image');
            $table->json('video_shows')->nullable()->after('videos');
            $table->json('themes')->nullable()->after('video_shows');
            $table->json('video_api_payloads')->nullable()->after('themes');
            $table->json('original_game_rating')->nullable()->after('video_api_payloads');
            $table->json('raw_results')->nullable()->after('original_game_rating');
        });

        if (Schema::hasTable('giant_bomb_game_media')) {
            Schema::drop('giant_bomb_game_media');
        }
    }

    public function down(): void
    {
        Schema::table('giant_bomb_games', function (Blueprint $table): void {
            $table->dropColumn([
                'primary_platform',
                'image',
                'images',
                'video_shows',
                'themes',
                'video_api_payloads',
                'original_game_rating',
                'raw_results',
            ]);
        });
        Schema::create('giant_bomb_game_media', function (Blueprint $table): void {
            $table->id();
            $table->foreignIdFor(GiantBombGame::class)->constrained()->cascadeOnDelete();
            if (Schema::hasTable('media')) {
                $table->foreignId('media_id')->nullable()->constrained('media')->nullOnDelete();
            } else {
                $table->unsignedBigInteger('media_id')->nullable()->index();
            }
            $table->timestamps();
            $table->unique(['giant_bomb_game_id', 'media_id'], 'gb_game_media_unique');
        });
    }
};