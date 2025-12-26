<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('alerts', function (Blueprint $table) {
            if (! Schema::hasColumn('alerts', 'offer_region_id')) {
                $table->foreignId('offer_region_id')->nullable()->constrained('offer_regions')->cascadeOnDelete()->after('offer_jurisdiction_id');
            }
        });
    }

    public function down(): void
    {
        Schema::table('alerts', function (Blueprint $table) {
            if (Schema::hasColumn('alerts', 'offer_region_id')) {
                $table->dropConstrainedForeignId('offer_region_id');
            }
        });
    }
};
