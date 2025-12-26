-- 0511_platform_hardware_view.sql
-- Create a convenient view that exposes platform -> hardware mapping
create
or replace view public.platform_hardware_view as
select
    p.id                    as platform_id,
    p.code                  as platform_code,
    p.name                  as platform_name,
    phm.hardware_product_id as hardware_product_id,
    phm.created_at          as mapped_at
from
    public.platforms p
    left join public.platform_hardware_map phm on phm.platform_id=p.id;

-- Grant select to public (optional; left permissive for read-only APIs)
grant
select
    on public.platform_hardware_view to public;