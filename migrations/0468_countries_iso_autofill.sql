
-- 0468_countries_iso_autofill.sql
-- Auto-fill iso2/iso3 on INSERT when only code2 is provided by ingestion.
-- Uses a small mapping table; extend as needed. Idempotent (create-if-missing, replace function).

CREATE TABLE IF NOT EXISTS country_iso_map (
  code2 char(2) PRIMARY KEY,
  iso2 char(2) NOT NULL,
  iso3 char(3) NOT NULL
);

-- Seed a core set (UPSERT)
INSERT INTO country_iso_map(code2, iso2, iso3) VALUES
  ('US','US','USA'),('GB','GB','GBR'),('CA','CA','CAN'),('DE','DE','DEU'),('FR','FR','FRA'),('ES','ES','ESP'),('IT','IT','ITA'),('NL','NL','NLD'),('BE','BE','BEL'),('PT','PT','PRT'),('IE','IE','IRL'),('FI','FI','FIN'),('GR','GR','GRC'),('AT','AT','AUT'),('SE','SE','SWE'),('NO','NO','NOR'),('DK','DK','DNK'),('CH','CH','CHE'),('JP','JP','JPN'),('KR','KR','KOR'),('AU','AU','AUS'),('NZ','NZ','NZL'),('BR','BR','BRA'),('MX','MX','MEX'),('AR','AR','ARG'),('ZA','ZA','ZAF'),('TR','TR','TUR'),('UA','UA','UKR'),('HK','HK','HKG'),('TW','TW','TWN'),('SG','SG','SGP'),('MY','MY','MYS'),('TH','TH','THA'),('ID','ID','IDN'),('PH','PH','PHL'),('VN','VN','VNM'),('CN','CN','CHN'),('IL','IL','ISR'),('CL','CL','CHL'),('CO','CO','COL'),('PE','PE','PER'),('UY','UY','URY'),('KZ','KZ','KAZ'),('SA','SA','SAU'),('AE','AE','ARE'),('EG','EG','EGY')
ON CONFLICT (code2) DO UPDATE SET iso2=EXCLUDED.iso2, iso3=EXCLUDED.iso3;

CREATE OR REPLACE FUNCTION countries_iso_autofill()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.code2 IS NOT NULL THEN
    -- If iso2 missing, fill from map
    IF NEW.iso2 IS NULL THEN
      SELECT m.iso2 INTO NEW.iso2 FROM country_iso_map m WHERE m.code2 = NEW.code2;
    END IF;
    -- If iso3 missing, fill from map
    IF NEW.iso3 IS NULL THEN
      SELECT m.iso3 INTO NEW.iso3 FROM country_iso_map m WHERE m.code2 = NEW.code2;
    END IF;
  END IF;
  RETURN NEW;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger WHERE tgname = 'countries_iso_autofill_trg'
  ) THEN
    CREATE TRIGGER countries_iso_autofill_trg
    BEFORE INSERT ON countries
    FOR EACH ROW EXECUTE FUNCTION countries_iso_autofill();
  END IF;
END $$;
