-- 0483_platforms_backup.sql
-- Create backup & mapping tables for platform rollback strategy.
-- Idempotent.

CREATE TABLE IF NOT EXISTS public.platforms_backup (
  backup_id       bigserial PRIMARY KEY,
  original_id     bigint NOT NULL,
  name            text NOT NULL,
  code            text,
  canonical_code  text,
  backed_up_at    timestamptz NOT NULL DEFAULT now()
);

-- Unique so we don't duplicate backups for same original row.
CREATE UNIQUE INDEX IF NOT EXISTS platforms_backup_original_id_uq ON public.platforms_backup(original_id);

-- Mapping of duplicate -> chosen canonical (after dedupe). Filled by dedupe binary when run.
CREATE TABLE IF NOT EXISTS public.platforms_dedupe_map (
  dupe_id       bigint PRIMARY KEY,
  canonical_id  bigint NOT NULL,
  deduped_at    timestamptz NOT NULL DEFAULT now()
);

-- Pre-populate backup snapshot for any rows not yet backed up.
INSERT INTO public.platforms_backup(original_id,name,code,canonical_code)
SELECT p.id, p.name, p.code, p.canonical_code
FROM public.platforms p
LEFT JOIN public.platforms_backup b ON b.original_id = p.id
WHERE b.original_id IS NULL;

ANALYZE public.platforms_backup;