<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (Schema::hasTable('product_media')) {
            return;
        }

        Schema::create('product_media', function (Blueprint $table): void {
            $table->id();
            $table->foreignId('product_id')->constrained()->cascadeOnDelete();

            // linkage to Spatie media table (optional)
            // Avoid FK dependency to keep ordering flexible; index only
            $table->unsignedBigInteger('media_id')->nullable();

            $table->string('source', 64)->nullable();
            $table->string('external_id', 191)->nullable();

            $table->string('media_type', 16)->nullable(); // image | video
            $table->string('title')->nullable();
            $table->text('caption')->nullable();
            $table->text('url');
            $table->text('thumbnail_url')->nullable();
            $table->string('attribution')->nullable();
            $table->string('license')->nullable();
            $table->string('license_url')->nullable();
            $table->boolean('is_primary')->default(false);

            $table->unsignedInteger('width')->nullable();
            $table->unsignedInteger('height')->nullable();
            $table->float('quality_score')->nullable();

            $table->timestamp('fetched_at')->nullable();
            $table->json('metadata')->nullable();

            $table->timestamps();

            // Helpful indexes
            $table->index(['product_id', 'media_type']);
            $table->index(['product_id', 'source']);
            $table->index(['product_id', 'external_id']);
            $table->index('fetched_at');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('product_media');
    }
};
