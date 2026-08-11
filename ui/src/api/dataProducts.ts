import { AxiosInstance } from 'axios';
import { http, API_PREFIX } from './http';
import {
  CustomerMasterProfile,
  CustomerRiskScore,
  CustomerSegment,
  Page,
  TransactionAnalytics,
} from './types';

export interface ListParams {
  [param: string]: unknown;
  page?: number;
  size?: number;
}

export interface RiskListParams extends ListParams {
  riskTier?: string;
}

export interface SegmentListParams extends ListParams {
  segmentName?: string;
}

export interface DataProductsApi {
  getProfile(customerId: number | string): Promise<CustomerMasterProfile>;
  getSegment(customerId: number | string): Promise<CustomerSegment>;
  getRiskScore(customerId: number | string): Promise<CustomerRiskScore>;
  getTransactions(
    customerId: number | string,
    reportingPeriod?: string,
  ): Promise<TransactionAnalytics[]>;
  listProfiles(params?: ListParams): Promise<Page<CustomerMasterProfile>>;
  listSegments(params?: SegmentListParams): Promise<Page<CustomerSegment>>;
  listRiskScores(params?: RiskListParams): Promise<Page<CustomerRiskScore>>;
  health(): Promise<boolean>;
}

/** REST implementation backed by the Spring Boot service under /api/v1. */
export function createRestApi(client: AxiosInstance = http): DataProductsApi {
  const get = async <T>(url: string, params?: Record<string, unknown>): Promise<T> => {
    const { data } = await client.get<T>(`${API_PREFIX}${url}`, { params });
    return data;
  };

  return {
    getProfile: (customerId) => get<CustomerMasterProfile>(`/customers/${customerId}/profile`),
    getSegment: (customerId) => get<CustomerSegment>(`/customers/${customerId}/segment`),
    getRiskScore: (customerId) => get<CustomerRiskScore>(`/customers/${customerId}/risk-score`),
    getTransactions: (customerId, reportingPeriod) =>
      get<TransactionAnalytics[]>(
        `/customers/${customerId}/transactions`,
        reportingPeriod ? { reportingPeriod } : undefined,
      ),
    listProfiles: (params) => get<Page<CustomerMasterProfile>>('/profiles', params),
    listSegments: (params) => get<Page<CustomerSegment>>('/segments', params),
    listRiskScores: (params) => get<Page<CustomerRiskScore>>('/risk-scores', params),
    health: async () => {
      await client.get(`${API_PREFIX}/health`);
      return true;
    },
  };
}
