<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('igdb_query_cache', function (Blueprint $table): void {
            $table->id();
            $table->string('endpoint', 64);
            $table->unsignedBigInteger('resource_id')->nullable();
            $table->string('slug')->nullable();
            $table->string('query_hash', 64)->unique();
            $table->text('query');
            $table->json('response');
            $table->string('response_checksum', 64)->nullable();
            $table->string('etag', 128)->nullable();
            $table->unsignedSmallInteger('status')->default(200);
            $table->unsignedInteger('ttl')->default(0);
            $table->timestamp('fetched_at')->useCurrent();
            $table->timestamp('last_modified_at')->nullable();
            $table->timestamp('expires_at')->nullable()->index();
            $table->timestamps();

            $table->index(['endpoint', 'resource_id']);
            $table->index(['endpoint', 'slug']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('igdb_query_cache');
    }
};
