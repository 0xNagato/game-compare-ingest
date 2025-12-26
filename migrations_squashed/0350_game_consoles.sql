-- 0350_game_consoles.sql (squashed)
-- Game console models/variants with composite uniqueness
CREATE TABLE IF NOT EXISTS public.game_consoles (
  id          bigserial PRIMARY KEY,
  product_id  bigint NOT NULL UNIQUE REFERENCES public.hardware(product_id) ON DELETE CASCADE,
  model       text NOT NULL,
  variant     text
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_game_consoles_product_model_null
  ON public.game_consoles(product_id, model) WHERE variant IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_game_consoles_product_model_variant
  ON public.game_consoles(product_id, model, variant) WHERE variant IS NOT NULL;
