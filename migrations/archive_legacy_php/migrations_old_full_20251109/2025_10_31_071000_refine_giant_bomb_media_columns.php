<?php

use Illuminate\Database\Migrations\Migration;

return new class extends Migration
{
    public function up(): void
    {
        // This migration previously handled column tweaks that were superseded by
        // the dedicated image and video tables introduced later in the stack.
        // We intentionally leave it as a no-op to preserve migration ordering
        // without attempting to mutate columns that no longer require changes.
    }

    public function down(): void
    {
        // No reversible operations were performed in the up() method, so the
        // down() method remains empty by design.
    }
};
