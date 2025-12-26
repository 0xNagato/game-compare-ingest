-- 0019_view_comments_and_latest.sql
-- Documentation comments for compatibility views and helper latest-price view.

SET search_path TO public;

-- Comments for sku_regions_vw
COMMENT ON VIEW public.sku_regions_vw IS 'Compatibility view exposing SKU per retailer per jurisdiction (aka sku_region). Backed by offer_jurisdictions.';
COMMENT ON COLUMN public.sku_regions_vw.sku_region_id IS 'Alias of offer_jurisdictions.id';
COMMENT ON COLUMN public.sku_regions_vw.region_code IS 'jurisdictions.region_code (NULL means national)';
COMMENT ON COLUMN public.sku_regions_vw.retailer IS 'retailers.slug if present else retailers.name';
COMMENT ON COLUMN public.sku_regions_vw.currency IS 'currencies.code (ISO 4217)';
COMMENT ON COLUMN public.sku_regions_vw.sku IS 'offers.sku provided by retailer';
COMMENT ON COLUMN public.sku_regions_vw.is_active IS 'Static true placeholder; offers.is_active not modeled in base schema';
COMMENT ON COLUMN public.sku_regions_vw.metadata IS 'Reserved for future metadata; NULL placeholder';
COMMENT ON COLUMN public.sku_regions_vw.created_at IS 'Reserved timestamp; not tracked in base schema';
COMMENT ON COLUMN public.sku_regions_vw.updated_at IS 'Reserved timestamp; not tracked in base schema';
COMMENT ON COLUMN public.sku_regions_vw.country_id IS 'jurisdictions.country_id';
COMMENT ON COLUMN public.sku_regions_vw.currency_id IS 'currencies.id';

-- Comments for region_prices_vw
COMMENT ON VIEW public.region_prices_vw IS 'Compatibility view for partitioned price series per sku_region with currency scaling.';
COMMENT ON COLUMN public.region_prices_vw.sku_region_id IS 'Alias of offer_jurisdictions.id';
COMMENT ON COLUMN public.region_prices_vw.recorded_at IS 'Price observation timestamp (partition key)';
COMMENT ON COLUMN public.region_prices_vw.fiat_amount IS 'amount_minor scaled by currencies.minor_unit';
COMMENT ON COLUMN public.region_prices_vw.btc_value IS 'Reserved placeholder; use btc_rate_snapshot when available';
COMMENT ON COLUMN public.region_prices_vw.tax_inclusive IS 'Whether the recorded amount includes tax';
COMMENT ON COLUMN public.region_prices_vw.fx_rate_snapshot IS 'Optional FX minor-per-unit scaled, if provided';
COMMENT ON COLUMN public.region_prices_vw.btc_rate_snapshot IS 'Optional BTC sats-per-unit as numeric';
COMMENT ON COLUMN public.region_prices_vw.raw_payload IS 'Original price metadata (json) as text';
COMMENT ON COLUMN public.region_prices_vw.created_at IS 'Echo of recorded_at for consumers expecting created/updated';
COMMENT ON COLUMN public.region_prices_vw.updated_at IS 'Echo of recorded_at for consumers expecting created/updated';
COMMENT ON COLUMN public.region_prices_vw.currency_id IS 'currencies.id';
COMMENT ON COLUMN public.region_prices_vw.country_id IS 'jurisdictions.country_id';
COMMENT ON COLUMN public.region_prices_vw.local_amount IS 'Synonym of fiat_amount';

-- Helper latest price per sku_region (joins current_price)
CREATE OR REPLACE VIEW public.latest_region_price_vw AS
SELECT
	oj.id::bigint                                                AS sku_region_id,
	cp.amount_minor                                              AS amount_minor,
	(cp.amount_minor::numeric / power(10::numeric, cur.minor_unit::int)) AS fiat_amount,
	cp.recorded_at                                               AS recorded_at,
	oj.currency_id                                               AS currency_id,
	j.country_id                                                 AS country_id
FROM public.current_price cp
JOIN public.offer_jurisdictions oj ON oj.id = cp.offer_jurisdiction_id
JOIN public.jurisdictions j        ON j.id = oj.jurisdiction_id
JOIN public.currencies cur         ON cur.id = oj.currency_id;

COMMENT ON VIEW public.latest_region_price_vw IS 'Latest price per sku_region using current_price for hot reads.';
COMMENT ON COLUMN public.latest_region_price_vw.sku_region_id IS 'Alias of offer_jurisdictions.id';
COMMENT ON COLUMN public.latest_region_price_vw.amount_minor IS 'Raw minor units (BIGINT)';
COMMENT ON COLUMN public.latest_region_price_vw.fiat_amount IS 'amount_minor scaled by currencies.minor_unit';
COMMENT ON COLUMN public.latest_region_price_vw.recorded_at IS 'Timestamp of latest price';
COMMENT ON COLUMN public.latest_region_price_vw.currency_id IS 'currencies.id';
COMMENT ON COLUMN public.latest_region_price_vw.country_id IS 'jurisdictions.country_id';

