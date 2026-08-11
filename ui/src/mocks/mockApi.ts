import {
  DataProductsApi,
  ListParams,
  RiskListParams,
  SegmentListParams,
} from '../api/dataProducts';
import {
  ApiError,
  CustomerMasterProfile,
  CustomerRiskScore,
  CustomerSegment,
  Page,
  TransactionAnalytics,
} from '../api/types';
import profiles from './customerMasterProfile.json';
import segments from './customerSegments.json';
import riskScores from './customerRiskScores.json';
import transactions from './transactionAnalytics.json';

const PROFILES = profiles as unknown as CustomerMasterProfile[];
const SEGMENTS = segments as unknown as CustomerSegment[];
const RISK_SCORES = riskScores as unknown as CustomerRiskScore[];
const TRANSACTIONS = transactions as unknown as TransactionAnalytics[];

const PERIOD_PATTERN = /^\d{4}-(0[1-9]|1[0-2])$/;

function parseCustomerId(raw: number | string): number {
  const value = typeof raw === 'number' ? raw : raw.trim();
  if (value === '' || !/^\d+$/.test(String(value))) {
    throw new ApiError(400, 'customerId must be a positive integer', {
      customerId: 'must be a positive integer',
    });
  }
  return Number(value);
}

function paginate<T>(rows: T[], { page = 0, size = 25 }: ListParams = {}): Page<T> {
  const start = page * size;
  return {
    content: rows.slice(start, start + size),
    page,
    size,
    totalElements: rows.length,
    totalPages: Math.max(1, Math.ceil(rows.length / size)),
  };
}

function findOrThrow<T extends { customerId: number }>(
  rows: T[],
  raw: number | string,
  product: string,
): T {
  const customerId = parseCustomerId(raw);
  const hit = rows.find((row) => row.customerId === customerId);
  if (!hit) {
    throw new ApiError(404, `No ${product} found for CUSTOMER_ID ${customerId}`);
  }
  return hit;
}

/** Fixture-backed implementation used when VITE_USE_MOCKS is enabled. */
export function createMockApi(): DataProductsApi {
  return {
    getProfile: async (customerId) =>
      findOrThrow(PROFILES, customerId, 'CUSTOMER_MASTER_PROFILE'),
    getSegment: async (customerId) => findOrThrow(SEGMENTS, customerId, 'CUSTOMER_SEGMENTS'),
    getRiskScore: async (customerId) =>
      findOrThrow(RISK_SCORES, customerId, 'CUSTOMER_RISK_SCORES'),
    getTransactions: async (customerId, reportingPeriod) => {
      const id = parseCustomerId(customerId);
      if (reportingPeriod && !PERIOD_PATTERN.test(reportingPeriod)) {
        throw new ApiError(400, 'reportingPeriod must match YYYY-MM', {
          reportingPeriod: 'must match YYYY-MM',
        });
      }
      const rows = TRANSACTIONS.filter(
        (row) =>
          row.customerId === id &&
          (!reportingPeriod || row.reportingPeriod === reportingPeriod),
      );
      if (rows.length === 0) {
        throw new ApiError(404, `No TRANSACTION_ANALYTICS found for CUSTOMER_ID ${id}`);
      }
      return rows;
    },
    listProfiles: async (params) => paginate(PROFILES, params),
    listSegments: async (params: SegmentListParams = {}) =>
      paginate(
        params.segmentName
          ? SEGMENTS.filter((row) => row.segmentName === params.segmentName)
          : SEGMENTS,
        params,
      ),
    listRiskScores: async (params: RiskListParams = {}) =>
      paginate(
        params.riskTier
          ? RISK_SCORES.filter((row) => row.riskTier === params.riskTier)
          : RISK_SCORES,
        params,
      ),
    health: async () => true,
  };
}

export const mockFixtures = {
  profiles: PROFILES,
  segments: SEGMENTS,
  riskScores: RISK_SCORES,
  transactions: TRANSACTIONS,
};
