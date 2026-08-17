/**
 * Typed API client. When VITE_POS_API_BASE_URL is set the service layer talks to the
 * real POS backend; otherwise the in-memory mock adapter is used.
 */

export const apiBaseUrl: string | undefined = import.meta.env.VITE_POS_API_BASE_URL;

export const useMockApi = !apiBaseUrl;

export class ApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }
}

export async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${apiBaseUrl}${path}`, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...init?.headers,
    },
  });
  if (!response.ok) {
    throw new ApiError(`POS API ${init?.method ?? 'GET'} ${path} failed`, response.status);
  }
  return (await response.json()) as T;
}

/** Simulates network latency for the mock adapter. */
export function mockResponse<T>(value: T, delayMs = 120): Promise<T> {
  return new Promise((resolve) => setTimeout(() => resolve(value), delayMs));
}
