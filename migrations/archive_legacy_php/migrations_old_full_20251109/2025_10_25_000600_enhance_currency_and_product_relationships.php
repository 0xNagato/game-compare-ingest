<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        if (! Schema::hasTable('currencies')) {
            Schema::create('currencies', function (Blueprint $table) {
                $table->id();
                $table->string('code', 6)->unique();
                $table->string('name');
                $table->string('symbol', 8)->nullable();
                $table->unsignedTinyInteger('decimals')->default(2);
                $table->boolean('is_crypto')->default(false);
                $table->json('metadata')->nullable();
                $table->timestamps();
            });
        }

        if (! Schema::hasTable('local_currencies')) {
            Schema::create('local_currencies', function (Blueprint $table) {
                $table->id();
                $table->foreignId('currency_id')->constrained()->cascadeOnDelete();
                $table->string('code', 12);
                $table->string('name')->nullable();
                $table->string('symbol', 8)->nullable();
                $table->json('metadata')->nullable();
                $table->timestamps();

                $table->unique(['currency_id', 'code']);
            });
        }

        if (! Schema::hasTable('countries')) {
            Schema::create('countries', function (Blueprint $table) {
                $table->id();
                $table->string('code', 8)->unique();
                $table->string('name');
                $table->foreignId('currency_id')->constrained()->cascadeOnDelete();
                $table->string('region')->nullable();
                $table->json('metadata')->nullable();
                $table->timestamps();
            });
        }

        if (! Schema::hasTable('consoles')) {
            Schema::create('consoles', function (Blueprint $table) {
                $table->id();
                $table->foreignId('product_id')->constrained()->cascadeOnDelete();
                $table->string('name');
                $table->string('manufacturer')->nullable();
                $table->date('release_date')->nullable();
                $table->json('metadata')->nullable();
                $table->timestamps();

                $table->index(['manufacturer', 'release_date']);
            });
        }

        if (! Schema::hasTable('video_games')) {
            Schema::create('video_games', function (Blueprint $table) {
                $table->id();
                $table->foreignId('product_id')->constrained()->cascadeOnDelete();
                $table->string('title');
                $table->string('genre')->nullable();
                $table->date('release_date')->nullable();
                $table->string('developer')->nullable();
                $table->json('metadata')->nullable();
                $table->timestamps();

                $table->index(['genre', 'release_date']);
            });
        }

        if (Schema::hasTable('sku_regions')) {
            Schema::table('sku_regions', function (Blueprint $table) {
                if (! Schema::hasColumn('sku_regions', 'country_id')) {
                    $table->foreignId('country_id')->nullable()->after('product_id')->constrained()->nullOnDelete();
                }
                if (! Schema::hasColumn('sku_regions', 'currency_id')) {
                    $table->foreignId('currency_id')->nullable()->after('currency')->constrained()->nullOnDelete();
                }
            });
        }

        if (Schema::hasTable('region_prices')) {
            Schema::table('region_prices', function (Blueprint $table) {
                if (! Schema::hasColumn('region_prices', 'currency_id')) {
                    $table->foreignId('currency_id')->nullable()->after('sku_region_id')->constrained()->nullOnDelete();
                }
                if (! Schema::hasColumn('region_prices', 'country_id')) {
                    $table->foreignId('country_id')->nullable()->after('currency_id')->constrained()->nullOnDelete();
                }
                if (! Schema::hasColumn('region_prices', 'local_amount')) {
                    $table->decimal('local_amount', 12, 2)->nullable()->after('fiat_amount');
                }
            });
        }

        DB::table('sku_regions')->orderBy('id')->chunkById(100, function ($regions): void {
            foreach ($regions as $region) {
                $currencyCode = strtoupper((string) ($region->currency ?? ''));
                $regionCode = strtoupper((string) ($region->region_code ?? ''));

                if ($currencyCode === '' || $regionCode === '') {
                    continue;
                }

                $currencyId = DB::table('currencies')->where('code', $currencyCode)->value('id');

                if ($currencyId === null) {
                    $currencyId = DB::table('currencies')->insertGetId([
                        'code' => $currencyCode,
                        'name' => $currencyCode,
                        'symbol' => null,
                        'decimals' => $currencyCode === 'JPY' ? 0 : 2,
                        'is_crypto' => $currencyCode === 'BTC',
                        'created_at' => now(),
                        'updated_at' => now(),
                    ]);
                }

                $countryId = DB::table('countries')->where('code', $regionCode)->value('id');

                if ($countryId === null) {
                    $countryId = DB::table('countries')->insertGetId([
                        'code' => $regionCode,
                        'name' => $regionCode,
                        'currency_id' => $currencyId,
                        'region' => null,
                        'created_at' => now(),
                        'updated_at' => now(),
                    ]);
                } else {
                    DB::table('countries')->where('id', $countryId)->update([
                        'currency_id' => $currencyId,
                        'updated_at' => now(),
                    ]);
                }

                $localKey = [
                    'currency_id' => $currencyId,
                    'code' => $regionCode.'_'.$currencyCode,
                ];

                $existingLocal = DB::table('local_currencies')->where($localKey)->first();

                if ($existingLocal) {
                    DB::table('local_currencies')->where($localKey)->update([
                        'name' => $regionCode.' '.$currencyCode,
                        'updated_at' => now(),
                    ]);
                } else {
                    DB::table('local_currencies')->insert([
                        'currency_id' => $currencyId,
                        'code' => $regionCode.'_'.$currencyCode,
                        'name' => $regionCode.' '.$currencyCode,
                        'created_at' => now(),
                        'updated_at' => now(),
                    ]);
                }

                DB::table('sku_regions')->where('id', $region->id)->update([
                    'currency_id' => $currencyId,
                    'country_id' => $countryId,
                    'updated_at' => now(),
                ]);
            }
        });

        DB::table('region_prices')->orderBy('id')->chunkById(100, function ($prices): void {
            foreach ($prices as $price) {
                $region = DB::table('sku_regions')
                    ->select('currency_id', 'country_id')
                    ->where('id', $price->sku_region_id)
                    ->first();

                DB::table('region_prices')->where('id', $price->id)->update([
                    'currency_id' => $region->currency_id ?? null,
                    'country_id' => $region->country_id ?? null,
                    'local_amount' => $price->fiat_amount,
                    'updated_at' => now(),
                ]);
            }
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('region_prices', function (Blueprint $table) {
            $table->dropColumn('local_amount');
            $table->dropConstrainedForeignId('country_id');
            $table->dropConstrainedForeignId('currency_id');
        });

        Schema::table('sku_regions', function (Blueprint $table) {
            $table->dropConstrainedForeignId('currency_id');
            $table->dropConstrainedForeignId('country_id');
        });

        Schema::dropIfExists('video_games');
        Schema::dropIfExists('consoles');
        Schema::dropIfExists('countries');
        Schema::dropIfExists('local_currencies');
        Schema::dropIfExists('currencies');
    }
};