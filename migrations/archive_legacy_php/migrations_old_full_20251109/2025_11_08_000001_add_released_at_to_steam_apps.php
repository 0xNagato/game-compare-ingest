<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        Schema::table('steam_apps', function (Blueprint $table) {
            if (! Schema::hasColumn('steam_apps', 'released_at')) {
                $table->dateTime('released_at')->nullable()->index();
            }
        });
    }

    public function down(): void
    {
        Schema::table('steam_apps', function (Blueprint $table) {
            if (Schema::hasColumn('steam_apps', 'released_at')) {
                $table->dropColumn('released_at');
            }
        });
    }
};
