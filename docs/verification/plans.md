# EXPLAIN Plans (captured 2025-11-11 UTC)

## media_by_vg_type

SQL:

```sql
SELECT id FROM public.provider_media_links
WHERE video_game_id=23 AND media_type='image'
ORDER BY created_at DESC
LIMIT 10;
```

Plan:

```text
Limit  (cost=0.14..3.00 rows=10 width=16) (actual time=0.633..0.641 rows=10 loops=1)
  Buffers: shared hit=6
  ->  Index Scan using provider_media_links_vg_type_created_idx on provider_media_links  (cost=0.14..7.28 rows=25 width=16) (actual time=0.632..0.638 rows=10 loops=1)
        Index Cond: ((video_game_id = 23) AND (media_type = 'image'::text))
        Buffers: shared hit=6
Planning:
  Buffers: shared hit=247
Planning Time: 0.786 ms
Execution Time: 0.686 ms
```

## media_by_item_url

SQL:

```sql
SELECT id FROM public.provider_media_links
WHERE provider_item_id=22 AND url='https://image.api.playstation.com/vulcan/ap/rnd/202510/1005/a09a71be0ef59273817c8f816a71cbdd1a417e169ec87719.png'
LIMIT 1;
```

Plan:

```text
Limit  (cost=0.27..2.49 rows=1 width=8) (actual time=2.914..2.915 rows=1 loops=1)
  Buffers: shared hit=6
  ->  Index Scan using uq_provider_media_links_item_url on provider_media_links  (cost=0.27..2.49 rows=1 width=8) (actual time=2.913..2.913 rows=1 loops=1)
        Index Cond: ((provider_item_id = 22) AND (url = 'https://image.api.playstation.com/vulcan/ap/rnd/202510/1005/a09a71be0ef59273817c8f816a71cbdd1a417e169ec87719.png'::text))
        Buffers: shared hit=6
Planning:
  Buffers: shared hit=252
Planning Time: 0.841 ms
Execution Time: 2.958 ms
```

## prices_series_30d

SQL:

```sql
SELECT recorded_at, amount_minor FROM public.prices
WHERE offer_jurisdiction_id=2 AND recorded_at >= now() - interval '30 days'
ORDER BY recorded_at;
```

Plan:

```text
Sort  (cost=41.18..41.23 rows=19 width=16) (actual time=0.743..0.744 rows=1 loops=1)
  Sort Key: prices.recorded_at
  Sort Method: quicksort  Memory: 25kB
  Buffers: shared hit=19
  ->  Append  (cost=0.15..40.78 rows=19 width=16) (actual time=0.613..0.705 rows=1 loops=1)
        Buffers: shared hit=16
        Subplans Removed: 11
        ->  Seq Scan on prices_2025_10 prices_1  (cost=0.00..1.06 rows=1 width=16) (actual time=0.593..0.593 rows=0 loops=1)
              Filter: ((offer_jurisdiction_id = 2) AND (recorded_at >= (now() - '30 days'::interval)))
              Rows Removed by Filter: 3
              Buffers: shared hit=1
        ->  Seq Scan on prices_2025_11 prices_2  (cost=0.00..1.88 rows=1 width=16) (actual time=0.019..0.023 rows=1 loops=1)
              Filter: ((offer_jurisdiction_id = 2) AND (recorded_at >= (now() - '30 days'::interval)))
              Rows Removed by Filter: 43
              Buffers: shared hit=1
        ->  Seq Scan on prices_2025_12 prices_3  (cost=0.00..1.06 rows=1 width=16) (actual time=0.016..0.016 rows=0 loops=1)
              Filter: ((offer_jurisdiction_id = 2) AND (recorded_at >= (now() - '30 days'::interval)))
              Rows Removed by Filter: 3
              Buffers: shared hit=1
        ->  Index Only Scan Backward using prices_2026_01_oj_recorded_at_desc on prices_2026_01 prices_4  (cost=0.15..2.37 rows=1 width=16) (actual time=0.039..0.039 rows=0 loops=1)
              Index Cond: ((offer_jurisdiction_id = 2) AND (recorded_at >= (now() - '30 days'::interval)))
              Heap Fetches: 0
              Buffers: shared hit=5
        ->  Index Only Scan Backward using prices_2026_02_oj_recorded_at_desc on prices_2026_02 prices_5  (cost=0.15..2.37 rows=1 width=16) (actual time=0.005..0.006 rows=0 loops=1)
              Index Cond: ((offer_jurisdiction_id = 2) AND (recorded_at >= (now() - '30 days'::interval)))
              Heap Fetches: 0
              Buffers: shared hit=2
        ->  Index Only Scan Backward using prices_2026_03_oj_recorded_at_desc on prices_2026_03 prices_6  (cost=0.15..2.37 rows=1 width=16) (actual time=0.007..0.007 rows=0 loops=1)
              Index Cond: ((offer_jurisdiction_id = 2) AND (recorded_at >= (now() - '30 days'::interval)))
              Heap Fetches: 0
              Buffers: shared hit=2
        ->  Index Only Scan Backward using prices_2026_04_oj_recorded_at_desc on prices_2026_04 prices_7  (cost=0.15..2.37 rows=1 width=16) (actual time=0.010..0.010 rows=0 loops=1)
              Index Cond: ((offer_jurisdiction_id = 2) AND (recorded_at >= (now() - '30 days'::interval)))
              Heap Fetches: 0
              Buffers: shared hit=2
        ->  Index Only Scan Backward using prices_2026_05_oj_recorded_at_desc on prices_2026_05 prices_8  (cost=0.15..2.37 rows=1 width=16) (actual time=0.007..0.007 rows=0 loops=1)
              Index Cond: ((offer_jurisdiction_id = 2) AND (recorded_at >= (now() - '30 days'::interval)))
              Heap Fetches: 0
              Buffers: shared hit=2
Planning:
  Buffers: shared hit=4334
Planning Time: 219.417 ms
Execution Time: 0.904 ms
```

## current_price_point

SQL:

```sql
SELECT amount_minor, recorded_at FROM public.current_price
WHERE offer_jurisdiction_id=2;
```

Plan:

```text
Seq Scan on current_price  (cost=0.00..1.05 rows=1 width=16) (actual time=0.620..0.621 rows=1 loops=1)
  Filter: (offer_jurisdiction_id = 2)
  Rows Removed by Filter: 12
  Buffers: shared hit=1 dirtied=1
Planning:
  Buffers: shared hit=89
Planning Time: 3.629 ms
Execution Time: 0.674 ms
```
