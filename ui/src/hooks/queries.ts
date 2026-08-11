import { useQuery } from '@tanstack/react-query';
import { api } from '../api';
import {
  CustomerMasterProfile,
  CustomerRiskScore,
  CustomerSegment,
  TransactionAnalytics,
} from '../api/types';

const options = { retry: false, refetchOnWindowFocus: false } as const;

export function useProfile(customerId: string | null) {
  return useQuery<CustomerMasterProfile>({
    queryKey: ['profile', customerId],
    queryFn: () => api.getProfile(customerId as string),
    enabled: Boolean(customerId),
    ...options,
  });
}

export function useSegment(customerId: string | null) {
  return useQuery<CustomerSegment>({
    queryKey: ['segment', customerId],
    queryFn: () => api.getSegment(customerId as string),
    enabled: Boolean(customerId),
    ...options,
  });
}

export function useRiskScore(customerId: string | null) {
  return useQuery<CustomerRiskScore>({
    queryKey: ['risk', customerId],
    queryFn: () => api.getRiskScore(customerId as string),
    enabled: Boolean(customerId),
    ...options,
  });
}

export function useTransactions(customerId: string | null, reportingPeriod?: string) {
  return useQuery<TransactionAnalytics[]>({
    queryKey: ['transactions', customerId, reportingPeriod ?? 'ALL'],
    queryFn: () => api.getTransactions(customerId as string, reportingPeriod),
    enabled: Boolean(customerId),
    ...options,
  });
}

export function useApiHealth() {
  return useQuery<boolean>({
    queryKey: ['health'],
    queryFn: () => api.health(),
    retry: false,
    refetchInterval: 30000,
    refetchOnWindowFocus: false,
  });
}
