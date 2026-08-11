/**
 * TypeScript mirrors of the Spring Boot data-product DTOs.
 * Field shapes follow ddl/02_data_product_tables.sql:
 *   CHAR(1) flags -> string, DECIMAL/INTEGER -> number, DATE/TIMESTAMP -> ISO string.
 * Nullable columns are modelled as `| null` because the SAS outputs contain blanks.
 */

export type Flag = string;

export interface CustomerSegment {
  customerId: number;
  segmentName: string | null;
  segmentId: number | null;
  subsegmentId: number | null;
  lifetimeValueScore: number | null;
  engagementScore: number | null;
  digitalAdoptionScore: number | null;
  productBreadthIndex: number | null;
  tenureGroup: string | null;
  ageGroup: string | null;
  balanceTier: string | null;
  channelPreference: string | null;
  crossSellFlag: Flag | null;
  upsellFlag: Flag | null;
  retentionRiskFlag: Flag | null;
  modelVersion: string | null;
  effectiveDate: string | null;
  loadTs: string | null;
}

export interface TransactionAnalytics {
  customerId: number;
  reportingPeriod: string | null;
  totalAccounts: number | null;
  activeAccounts: number | null;
  totalTransactions: number | null;
  totalDebitAmt: number | null;
  totalCreditAmt: number | null;
  totalFees: number | null;
  netCashFlow: number | null;
  avgTransactionSize: number | null;
  monthlySpendTrend: string | null;
  spendPercentile: number | null;
  topSpendCategory: string | null;
  digitalTxnPct: number | null;
  feeIncome: number | null;
  interestIncome: number | null;
  revenueContribution: number | null;
  anomalyFlag: Flag | null;
  modelVersion: string | null;
  effectiveDate: string | null;
  loadTs: string | null;
}

export interface CustomerRiskScore {
  customerId: number;
  compositeRiskScore: number | null;
  riskTier: string | null;
  probabilityOfDefault: number | null;
  creditRiskComponent: number | null;
  behaviourRiskComponent: number | null;
  velocityRiskComponent: number | null;
  bureauScoreComponent: number | null;
  paymentHistoryComponent: number | null;
  primaryRiskDriver: string | null;
  secondaryRiskDriver: string | null;
  scoreDelta30d: number | null;
  watchListFlag: Flag | null;
  reviewRequiredFlag: Flag | null;
  modelVersion: string | null;
  effectiveDate: string | null;
  loadTs: string | null;
}

export interface CustomerMasterProfile {
  customerId: number;
  fullName: string | null;
  age: number | null;
  stateCode: string | null;
  customerSince: string | null;
  tenureMonths: number | null;
  customerStatus: Flag | null;
  segmentName: string | null;
  lifetimeValueScore: number | null;
  engagementScore: number | null;
  totalAccounts: number | null;
  activeAccounts: number | null;
  totalBalance: number | null;
  totalCreditLimit: number | null;
  creditUtilizationPct: number | null;
  monthlyTransactions: number | null;
  monthlySpend: number | null;
  netCashFlow: number | null;
  topSpendCategory: string | null;
  digitalTxnPct: number | null;
  compositeRiskScore: number | null;
  riskTier: string | null;
  probabilityOfDefault: number | null;
  watchListFlag: Flag | null;
  crossSellFlag: Flag | null;
  upsellFlag: Flag | null;
  retentionRiskFlag: Flag | null;
  modelVersion: string | null;
  effectiveDate: string | null;
  loadTs: string | null;
}

/** Spring Data style page envelope returned by the list endpoints. */
export interface Page<T> {
  content: T[];
  page: number;
  size: number;
  totalElements: number;
  totalPages: number;
}

/** Shape produced by the API's @RestControllerAdvice. */
export interface ApiErrorBody {
  timestamp?: string;
  status?: number;
  error?: string;
  message?: string;
  path?: string;
  fieldErrors?: Record<string, string>;
}

export class ApiError extends Error {
  readonly status: number;
  readonly fieldErrors: Record<string, string>;

  constructor(status: number, message: string, fieldErrors: Record<string, string> = {}) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.fieldErrors = fieldErrors;
  }

  get isNotFound(): boolean {
    return this.status === 404;
  }

  get isValidation(): boolean {
    return this.status === 400;
  }
}
