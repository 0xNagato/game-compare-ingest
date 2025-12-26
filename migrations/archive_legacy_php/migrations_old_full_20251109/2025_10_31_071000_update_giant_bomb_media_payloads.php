<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('giant_bomb_games', function (Blueprint $table): void {
            if (! Schema::hasColumn('giant_bomb_games', 'media_payload_hash')) {
                $table->string('media_payload_hash', 64)->nullable()->after('payload_hash');
            }

            if (! Schema::hasColumn('giant_bomb_games', 'video_assets')) {
                $table->json('video_assets')->nullable()->after('video_api_payloads');
            }

            if (! Schema::hasColumn('giant_bomb_games', 'image_assets')) {
                $table->json('image_assets')->nullable()->after('video_assets');
            }

            if (! Schema::hasColumn('giant_bomb_games', 'streaming_assets')) {
                $table->json('streaming_assets')->nullable()->after('image_assets');
            }

            if (! Schema::hasColumn('giant_bomb_games', 'external_links')) {
                $table->json('external_links')->nullable()->after('streaming_assets');
            }
        });
    }

    public function down(): void
    {
        Schema::table('giant_bomb_games', function (Blueprint $table): void {
            if (Schema::hasColumn('giant_bomb_games', 'external_links')) {
                $table->dropColumn('external_links');
            }

            if (Schema::hasColumn('giant_bomb_games', 'streaming_assets')) {
                $table->dropColumn('streaming_assets');
            }

            if (Schema::hasColumn('giant_bomb_games', 'image_assets')) {
                $table->dropColumn('image_assets');
            }

            if (Schema::hasColumn('giant_bomb_games', 'video_assets')) {
                $table->dropColumn('video_assets');
            }

            if (Schema::hasColumn('giant_bomb_games', 'media_payload_hash')) {
                $table->dropColumn('media_payload_hash');
            }
        });
    }
};
