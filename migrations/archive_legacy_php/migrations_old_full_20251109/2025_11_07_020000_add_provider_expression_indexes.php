<?php

use Illuminate\Database\Migrations\Migration;

return new class extends Migration
{
	/**
	 * Legacy placeholder migration (no-op). Expression indexes are defined in
	 * 2025_11_07_020000_add_provider_expression_indexes_to_video_games.php.
	 */
	public function up(): void
	{
		// no-op to avoid class resolution errors when this file exists empty
	}

	public function down(): void
	{
		// no-op
	}
};

