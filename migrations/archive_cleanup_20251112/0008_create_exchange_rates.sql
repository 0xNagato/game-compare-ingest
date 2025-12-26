-- Create exchange_rates table for storing fiat/crypto provider rates
-- gamecompare schema removal: all objects reside in public

DO $$ BEGIN
    CREATE TABLE IF NOT EXISTS exchange_rates (
        id BIGSERIAL PRIMARY KEY,
        base_currency TEXT NOT NULL,
        quote_currency TEXT NOT NULL,
        rate DOUBLE PRECISION NOT NULL,
        provider TEXT NOT NULL,
        fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        UNIQUE (base_currency, quote_currency, provider)
    );
EXCEPTION WHEN duplicate_table THEN NULL; END $$;

-- Helpful index for lookups
DO $$ BEGIN
    CREATE INDEX IF NOT EXISTS idx_exchange_rates_lookup ON exchange_rates USING btree (base_currency, quote_currency, provider, fetched_at DESC);
END $$;
