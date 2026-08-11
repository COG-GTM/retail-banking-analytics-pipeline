import { createRestApi, DataProductsApi } from './dataProducts';
import { createMockApi } from '../mocks/mockApi';

export const USE_MOCKS = import.meta.env.VITE_USE_MOCKS === 'true';
export const OPERATOR = import.meta.env.VITE_OPERATOR ?? 'OP-0001';
export const ENVIRONMENT = import.meta.env.VITE_ENVIRONMENT ?? 'DEV';

export const api: DataProductsApi = USE_MOCKS ? createMockApi() : createRestApi();

export * from './types';
export type { DataProductsApi } from './dataProducts';
