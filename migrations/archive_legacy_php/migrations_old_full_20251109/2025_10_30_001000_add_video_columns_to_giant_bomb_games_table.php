<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('giant_bomb_games', function (Blueprint $table): void {
            $table->string('primary_video_name')->nullable()->after('image_original_url');
            $table->string('primary_video_high_url')->nullable()->after('primary_video_name');
            $table->string('primary_video_hd_url')->nullable()->after('primary_video_high_url');
            $table->unsignedInteger('video_count')->default(0)->after('primary_video_hd_url');
            $table->text('videos')->nullable()->after('video_count');
        });
    }

    public function down(): void
    {
        Schema::table('giant_bomb_games', function (Blueprint $table): void {
            $table->dropColumn([
                'primary_video_name',
                'primary_video_high_url',
                'primary_video_hd_url',
                'video_count',
                'videos',
            ]);
        });
    }
};
