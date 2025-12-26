-- 0480_current_price_agent_tiebreaker.sql
-- Purpose: Add agent and agent_priority to current_price for deterministic tie-breakers
-- Idempotent and safe to re-run.

ALTER TABLE IF EXISTS public.current_price
  ADD COLUMN IF NOT EXISTS agent text NOT NULL DEFAULT 'unknown';

ALTER TABLE IF EXISTS public.current_price
  ADD COLUMN IF NOT EXISTS agent_priority smallint NOT NULL DEFAULT 0;

-- Optional: backfill existing rows with defaults already covered by DEFAULT clauses.
-- Existing covering index remains valid; no changes needed.
