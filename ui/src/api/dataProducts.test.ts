import { describe, expect, it, vi } from 'vitest';
import type { AxiosInstance } from 'axios';
import { createRestApi } from './dataProducts';
import { toApiError } from './http';
import { ApiError } from './types';

function fakeClient(get: ReturnType<typeof vi.fn>) {
  return { get } as unknown as AxiosInstance;
}

describe('REST data-products client', () => {
  it('calls the profile endpoint under /api/v1', async () => {
    const get = vi.fn().mockResolvedValue({ data: { customerId: 42 } });
    const api = createRestApi(fakeClient(get));

    await expect(api.getProfile(42)).resolves.toEqual({ customerId: 42 });
    expect(get).toHaveBeenCalledWith('/api/v1/customers/42/profile', { params: undefined });
  });

  it('passes reportingPeriod as a query param', async () => {
    const get = vi.fn().mockResolvedValue({ data: [] });
    const api = createRestApi(fakeClient(get));

    await api.getTransactions(7, '2026-04');
    expect(get).toHaveBeenCalledWith('/api/v1/customers/7/transactions', {
      params: { reportingPeriod: '2026-04' },
    });
  });

  it('passes list filters and pagination', async () => {
    const get = vi.fn().mockResolvedValue({ data: { content: [] } });
    const api = createRestApi(fakeClient(get));

    await api.listSegments({ segmentName: 'VALUE_BASIC', page: 1, size: 10 });
    expect(get).toHaveBeenCalledWith('/api/v1/segments', {
      params: { segmentName: 'VALUE_BASIC', page: 1, size: 10 },
    });
  });
});

describe('toApiError', () => {
  it('surfaces field errors from the @RestControllerAdvice payload', () => {
    const error = toApiError({
      message: 'Request failed',
      response: {
        status: 400,
        data: { fieldErrors: { reportingPeriod: 'must match YYYY-MM' } },
      },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any);

    expect(error).toBeInstanceOf(ApiError);
    expect(error.isValidation).toBe(true);
    expect(error.message).toBe('reportingPeriod: must match YYYY-MM');
  });

  it('falls back to the message field for 404s', () => {
    const error = toApiError({
      message: 'Request failed',
      response: { status: 404, data: { message: 'Customer 9 not found' } },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any);

    expect(error.isNotFound).toBe(true);
    expect(error.message).toBe('Customer 9 not found');
  });

  it('reports an unreachable API when there is no response', () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const error = toApiError({ message: 'Network Error' } as any);
    expect(error.status).toBe(0);
    expect(error.message).toContain('API unreachable');
  });
});
