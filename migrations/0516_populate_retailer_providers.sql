-- Migration 0516: Populate retailer_providers mappings
-- Maps provider APIs/sources to their corresponding retailers

DO $outer$
DECLARE
  ps_provider_id bigint;
  ps_retailer_id bigint;
  steam_provider_id bigint;
  steam_retailer_id bigint;
  ms_provider_id bigint;
  nexarda_provider_id bigint;
  itad_provider_id bigint;
BEGIN
  -- Map PlayStation Store provider to PlayStation retailer
  SELECT id INTO ps_provider_id FROM providers WHERE slug = 'ps-store' OR slug = 'playstation-store' LIMIT 1;
  SELECT id INTO ps_retailer_id FROM retailers WHERE slug = 'playstation';
  
  IF ps_provider_id IS NOT NULL AND ps_retailer_id IS NOT NULL THEN
    INSERT INTO retailer_providers (provider_id, retailer_id)
    VALUES (ps_provider_id, ps_retailer_id)
    ON CONFLICT (retailer_id, provider_id) DO NOTHING;
    
    RAISE NOTICE 'Mapped PlayStation Store provider (id: %) to PlayStation retailer (id: %)', ps_provider_id, ps_retailer_id;
  ELSE
    RAISE NOTICE 'PlayStation provider or retailer not found (provider: %, retailer: %)', ps_provider_id, ps_retailer_id;
  END IF;

  -- Map Steam Store provider to Steam retailer
  SELECT id INTO steam_provider_id FROM providers WHERE slug = 'steam-store';
  SELECT id INTO steam_retailer_id FROM retailers WHERE slug = 'steam';
  
  IF steam_provider_id IS NOT NULL AND steam_retailer_id IS NOT NULL THEN
    INSERT INTO retailer_providers (provider_id, retailer_id)
    VALUES (steam_provider_id, steam_retailer_id)
    ON CONFLICT (retailer_id, provider_id) DO NOTHING;
    
    RAISE NOTICE 'Mapped Steam Store provider (id: %) to Steam retailer (id: %)', steam_provider_id, steam_retailer_id;
  ELSE
    RAISE NOTICE 'Steam provider or retailer not found (provider: %, retailer: %)', steam_provider_id, steam_retailer_id;
  END IF;

  -- Map Microsoft Store provider to a Microsoft/Xbox retailer (if exists)
  SELECT id INTO ms_provider_id FROM providers WHERE slug = 'microsoft-store';
  
  IF ms_provider_id IS NOT NULL THEN
    -- Create Microsoft retailer if it doesn't exist, or get existing
    INSERT INTO retailers (slug, name)
    VALUES ('microsoft', 'Microsoft')
    ON CONFLICT (slug) DO NOTHING;
    
    -- Always fetch the id (whether just created or already existed)
    SELECT id INTO steam_retailer_id FROM retailers WHERE slug = 'microsoft';
    
    IF steam_retailer_id IS NOT NULL THEN
      INSERT INTO retailer_providers (provider_id, retailer_id)
      VALUES (ms_provider_id, steam_retailer_id)
      ON CONFLICT (retailer_id, provider_id) DO NOTHING;
      
      RAISE NOTICE 'Mapped Microsoft Store provider (id: %) to Microsoft retailer (id: %)', ms_provider_id, steam_retailer_id;
    END IF;
  END IF;

  -- Multi-retailer aggregators: Nexarda, IsThereAnyDeal
  -- These can map to multiple retailers, but we'll skip for now as they aggregate data
  
  SELECT id INTO nexarda_provider_id FROM providers WHERE slug = 'nexarda';
  SELECT id INTO itad_provider_id FROM providers WHERE slug = 'isthereanydeal';
  
  IF nexarda_provider_id IS NOT NULL THEN
    RAISE NOTICE 'Nexarda (id: %) is a multi-retailer aggregator; no single retailer mapping created', nexarda_provider_id;
  END IF;
  
  IF itad_provider_id IS NOT NULL THEN
    RAISE NOTICE 'IsThereAnyDeal (id: %) is a multi-retailer aggregator; no single retailer mapping created', itad_provider_id;
  END IF;

END $outer$;
