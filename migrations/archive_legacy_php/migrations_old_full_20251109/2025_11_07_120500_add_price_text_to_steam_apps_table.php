<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (! Schema::hasTable('steam_apps')) {
            return;
        }

        Schema::table('steam_apps', function (Blueprint $table): void {
            if (! Schema::hasColumn('steam_apps', 'price_text')) {
                $table->string('price_text', 64)->nullable()->after('price_overview');
            }
        });
    }

    public function down(): void
    {
        if (! Schema::hasTable('steam_apps')) {
            return;
        }

        Schema::table('steam_apps', function (Blueprint $table): void {
            if (Schema::hasColumn('steam_apps', 'price_text')) {
                $table->dropColumn('price_text');
            }
        });
    }
};
