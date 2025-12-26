// Generated 2025-11-26 — Core GameCompare schema contracts for TypeScript clients

export type ProductKind = 'software' | 'hardware';

export interface Product {
  id: number;
  kind: ProductKind;
  slug: string;
  name: string;
  shortName?: string | null;
  metadata?: Record<string, unknown> | null;
  createdAt: string; // ISO 8601
  updatedAt: string; // ISO 8601
}

export interface Platform {
  id: number;
  code?: string | null;
  name: string;
  family?: string | null;
}

export interface ProductVersion {
  id: number;
  productId: number;
  platformId?: number | null;
  edition?: string | null;
  formFactor?: string | null;
  releaseDate?: string | null; // YYYY-MM-DD
  metadata?: Record<string, unknown> | null;
  createdAt: string;
  updatedAt: string;
  product?: Product;
  platform?: Platform | null;
}

export interface Retailer {
  id: number;
  slug: string;
  name: string;
  metadata?: Record<string, unknown> | null;
  createdAt: string;
  updatedAt: string;
}

export interface Offer {
  id: number;
  productVersionId: number;
  retailerId: number;
  sku?: string | null;
  isActive: boolean;
  metadata?: Record<string, unknown> | null;
  createdAt: string;
  updatedAt: string;
  productVersion?: ProductVersion;
  retailer?: Retailer;
}

export interface Jurisdiction {
  id: number;
  countryId: number;
  regionCode?: string | null;
}

export interface Currency {
  id: number;
  code: string;
  name: string;
  minorUnit: number;
}

export interface TaxRule {
  id: number;
  jurisdictionId: number;
  effectiveFrom: string;
  effectiveTo?: string | null;
  rateBasisPoints: number;
  inclusive: boolean;
  notes?: string | null;
}

export interface OfferRegion {
  id: number;
  offerId: number;
  jurisdictionId: number;
  currencyId: number;
  taxRuleId?: number | null;
  metadata?: Record<string, unknown> | null;
  offer?: Offer;
  jurisdiction?: Jurisdiction;
  currency?: Currency;
  taxRule?: TaxRule | null;
}

export interface Price {
  id: number;
  offerRegionId: number;
  recordedAt: string; // ISO 8601 timestamp
  amountMinor: string;
  taxInclusive: boolean;
  fxMinorPerUnit?: string | null;
  btcSatsPerUnit?: string | null;
  meta?: Record<string, unknown> | null;
}

export interface CurrentPrice {
  offerRegionId: number;
  amountMinor: string;
  recordedAt: string;
}

export type ProviderKind = 'retailer_api' | 'catalog' | 'media' | 'pricing_api';

export interface Provider {
  id: number;
  code: string;
  name: string;
  kind: ProviderKind;
  metadata?: Record<string, unknown> | null;
  createdAt: string;
  updatedAt: string;
}

export interface ProviderItem {
  id: number;
  providerId: number;
  externalId: string;
  productVersionId?: number | null;
  offerId?: number | null;
  lastSyncedAt?: string | null;
  payloadHash?: string | null;
  metadata?: Record<string, unknown> | null;
}

export type ProviderRunStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'partial';

export interface ProviderRun {
  id: number;
  providerId: number;
  startedAt: string;
  finishedAt?: string | null;
  status: ProviderRunStatus;
  itemCount?: number | null;
  errorSummary?: Record<string, unknown> | null;
}

export type AlertOperator = 'above' | 'below';

export interface Alert {
  id: number;
  userId: number;
  offerRegionId?: number | null;
  thresholdMinor: string;
  comparisonOperator: AlertOperator;
  channel: string;
  isActive: boolean;
  lastTriggeredAt?: string | null;
  settings?: Record<string, unknown> | null;
  createdAt: string;
  updatedAt: string;
}

export interface ExchangeRate {
  id: number;
  baseCurrency: string;
  quoteCurrency: string;
  rate: number;
  provider: string;
  fetchedAt: string;
  metadata?: Record<string, unknown> | null;
}

export interface PricePoint {
  recordedAt: string;
  amountMinor: string;
  taxInclusive: boolean;
  fxMinorPerUnit?: string | null;
  btcSatsPerUnit?: string | null;
}

export interface OfferRegionSeries {
  offerRegionId: number;
  points: PricePoint[];
}

export interface ChoroplethPoint {
  jurisdictionId: number;
  meanAmountMinor: number;
  observationCount: number;
  lastRecordedAt: string;
}

export interface SimplifiedSchemaSnapshot {
  generatedAt: string;
  products: Product[];
  offers: Offer[];
  offerRegions: OfferRegion[];
  providers: Provider[];
  providerItems: ProviderItem[];
}