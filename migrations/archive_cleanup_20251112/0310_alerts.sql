-- 0310_alerts.sql (squashed)
CREATE TABLE IF NOT EXISTS public.alerts (
  id                    bigserial PRIMARY KEY,
  user_id               bigint NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  offer_jurisdiction_id bigint NOT NULL REFERENCES public.offer_jurisdictions(id) ON DELETE CASCADE,
  op                    cmp_op NOT NULL,
  threshold_minor       bigint NOT NULL,
  active                boolean NOT NULL DEFAULT true,
  last_triggered_at     timestamptz,
  settings              jsonb,
  created_at            timestamptz NOT NULL DEFAULT now(),
  updated_at            timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS alerts_active_oj_idx ON public.alerts (offer_jurisdiction_id) WHERE active;
CREATE INDEX IF NOT EXISTS alerts_user_active_idx ON public.alerts (user_id) WHERE active;
