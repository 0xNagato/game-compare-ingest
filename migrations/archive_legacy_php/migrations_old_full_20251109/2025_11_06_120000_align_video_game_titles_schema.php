<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (! Schema::hasTable('video_game_titles')) {
            return;
        }

        Schema::table('video_game_titles', function (Blueprint $table): void {
            // Add raw_title if missing to align with tests and sqlite snapshot
            if (! Schema::hasColumn('video_game_titles', 'raw_title')) {
                $table->string('raw_title')->nullable()->after('provider_item_id');
            }

            // Ensure normalized_title exists (already present in our base create)
            if (! Schema::hasColumn('video_game_titles', 'normalized_title')) {
                $table->string('normalized_title')->nullable()->after('raw_title');
            }

            // Add version_hint if missing
            if (! Schema::hasColumn('video_game_titles', 'version_hint')) {
                $table->string('version_hint')->nullable()->after('locale');
            }

            // Add metadata if missing (tests/snapshots used this name; we already had payload)
            if (! Schema::hasColumn('video_game_titles', 'metadata')) {
                $table->text('metadata')->nullable()->after('version_hint');
            }
        });

        // Best-effort backfill: copy title -> raw_title if raw_title is null and title exists
        try {
            if (Schema::hasColumn('video_game_titles', 'title') && Schema::hasColumn('video_game_titles', 'raw_title')) {
                DB::table('video_game_titles')
                    ->whereNull('raw_title')
                    ->update(['raw_title' => DB::raw('title')]);
            }
        } catch (\Throwable $e) {
            // no-op; best effort only
        }
    }

    public function down(): void
    {
        if (! Schema::hasTable('video_game_titles')) {
            return;
        }

        Schema::table('video_game_titles', function (Blueprint $table): void {
            if (Schema::hasColumn('video_game_titles', 'version_hint')) {
                try { $table->dropColumn('version_hint'); } catch (\Throwable) {}
            }
            if (Schema::hasColumn('video_game_titles', 'raw_title')) {
                try { $table->dropColumn('raw_title'); } catch (\Throwable) {}
            }
            if (Schema::hasColumn('video_game_titles', 'metadata')) {
                try { $table->dropColumn('metadata'); } catch (\Throwable) {}
            }
        });
    }
};
