import { describe, expect, it } from 'vitest';
import { createMockApi } from './mockApi';
import { ApiError } from '../api/types';

const api = createMockApi();

describe('mock data-products api', () => {
  it('returns the master profile for a known CUSTOMER_ID', async () => {
    const profile = await api.getProfile(3);
    expect(profile.customerId).toBe(3);
    expect(profile.segmentName).toBe('ENGAGED_MAINSTREAM');
  });

  it('returns segment and risk products for a known CUSTOMER_ID', async () => {
    await expect(api.getSegment(3)).resolves.toMatchObject({ customerId: 3 });
    await expect(api.getRiskScore(3)).resolves.toMatchObject({ customerId: 3, riskTier: 'LOW' });
  });

  it('filters transactions by REPORTING_PERIOD', async () => {
    const rows = await api.getTransactions(3, '2026-04');
    expect(rows.length).toBeGreaterThan(0);
    expect(rows.every((row) => row.reportingPeriod === '2026-04')).toBe(true);
  });

  it('raises a 404 ApiError for an unknown CUSTOMER_ID', async () => {
    await expect(api.getProfile(99999999)).rejects.toMatchObject({ status: 404 });
  });

  it('raises a 400 ApiError for a malformed CUSTOMER_ID', async () => {
    const error = await api.getProfile('abc').catch((e) => e);
    expect(error).toBeInstanceOf(ApiError);
    expect(error.isValidation).toBe(true);
  });

  it('raises a 400 ApiError for a malformed REPORTING_PERIOD', async () => {
    await expect(api.getTransactions(3, '2026-13')).rejects.toMatchObject({
      status: 400,
      fieldErrors: { reportingPeriod: 'must match YYYY-MM' },
    });
  });

  it('paginates and filters list endpoints', async () => {
    const page = await api.listRiskScores({ page: 0, size: 5, riskTier: 'LOW' });
    expect(page.content).toHaveLength(5);
    expect(page.content.every((row) => row.riskTier === 'LOW')).toBe(true);
    expect(page.totalElements).toBeGreaterThan(5);
  });
});
