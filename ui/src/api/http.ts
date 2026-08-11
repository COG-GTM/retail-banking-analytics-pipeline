import axios, { AxiosError, AxiosInstance } from 'axios';
import { ApiError, ApiErrorBody } from './types';

export const API_PREFIX = '/api/v1';

export function createHttpClient(baseURL = ''): AxiosInstance {
  const instance = axios.create({ baseURL, timeout: 15000 });

  instance.interceptors.response.use(
    (response) => response,
    (error: AxiosError<ApiErrorBody>) => {
      throw toApiError(error);
    },
  );

  return instance;
}

export function toApiError(error: AxiosError<ApiErrorBody>): ApiError {
  const response = error.response;
  if (!response) {
    return new ApiError(0, `API unreachable: ${error.message}`);
  }

  const body = response.data ?? {};
  const fieldErrors = body.fieldErrors ?? {};
  const fieldSummary = Object.entries(fieldErrors)
    .map(([field, msg]) => `${field}: ${msg}`)
    .join('; ');

  const message =
    fieldSummary ||
    body.message ||
    body.error ||
    defaultMessageForStatus(response.status);

  return new ApiError(response.status, message, fieldErrors);
}

function defaultMessageForStatus(status: number): string {
  if (status === 404) return 'Not found';
  if (status === 400) return 'Invalid request';
  return `Request failed with status ${status}`;
}

export const http = createHttpClient();
