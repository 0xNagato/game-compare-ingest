-- 0295_current_price.sql (squashed)
CREATE TABLE IF NOT EXISTS public.current_price (
  offer_jurisdiction_id  bigint PRIMARY KEY REFERENCES public.offer_jurisdictions(id) ON DELETE CASCADE,
  amount_minor           bigint NOT NULL,
  recorded_at            timestamptz NOT NULL
);
-- Upsert pattern reference:
-- INSERT INTO public.current_price(offer_jurisdiction_id, amount_minor, recorded_at)
-- VALUES ($1,$2,$3)
-- ON CONFLICT (offer_jurisdiction_id) DO UPDATE
--   SET amount_minor=EXCLUDED.amount_minor,
--       recorded_at=EXCLUDED.recorded_at;
