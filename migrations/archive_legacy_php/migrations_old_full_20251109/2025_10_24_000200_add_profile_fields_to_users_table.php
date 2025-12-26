<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        if (Schema::hasTable('users')) {
            Schema::table('users', function (Blueprint $table) {
                if (! Schema::hasColumn('users', 'discord_id')) {
                    $table->string('discord_id')->nullable()->after('email');
                }
                if (! Schema::hasColumn('users', 'timezone')) {
                    // If discord_id doesn't exist (guard above), placing after('discord_id') would fail.
                    // Use after('email') safely when discord_id missing.
                    $after = Schema::hasColumn('users', 'discord_id') ? 'discord_id' : 'email';
                    $table->string('timezone')->default('UTC')->after($after);
                }
            });
        }
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        if (Schema::hasTable('users')) {
            Schema::table('users', function (Blueprint $table) {
                $drops = [];
                foreach (['timezone','discord_id'] as $col) {
                    if (Schema::hasColumn('users', $col)) {
                        $drops[] = $col;
                    }
                }
                if ($drops) {
                    $table->dropColumn($drops);
                }
            });
        }
    }
};
