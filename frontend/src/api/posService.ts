import type {
  JobAccount,
  Product,
  Quote,
  RegisterContext,
  Tender,
  Transaction,
} from '../types/pos';
import { mockResponse, request, useMockApi } from './client';
import {
  employee,
  jobAccount,
  products,
  quotes,
  register,
  services,
  store,
  type ServiceDefinition,
} from './mockData';

/** GET /stores/{storeId}/registers/{registerId}/context */
export function getRegisterContext(): Promise<RegisterContext> {
  if (useMockApi) return mockResponse({ store, register, employee });
  return request<RegisterContext>(
    `/stores/${store.storeId}/registers/${register.registerId}/context`,
  );
}

/** GET /products/{sku} */
export async function lookupProduct(sku: string): Promise<Product | null> {
  const term = sku.trim().toLowerCase();
  if (!term) return null;
  if (useMockApi) {
    const match =
      products.find((p) => p.sku === term) ??
      products.find((p) => p.productDescription.toLowerCase().includes(term)) ??
      null;
    return mockResponse(match);
  }
  try {
    return await request<Product>(`/products/${encodeURIComponent(term)}`);
  } catch {
    return null;
  }
}

/** GET /products?search= */
export function searchProducts(term: string): Promise<Product[]> {
  const needle = term.trim().toLowerCase();
  if (useMockApi) {
    const matches = needle
      ? products.filter(
          (p) =>
            p.sku.includes(needle) || p.productDescription.toLowerCase().includes(needle),
        )
      : products;
    return mockResponse(matches.slice(0, 6), 60);
  }
  return request<Product[]>(`/products?search=${encodeURIComponent(needle)}`);
}

/** GET /service-items */
export function getServiceCatalog(): Promise<ServiceDefinition[]> {
  if (useMockApi) return mockResponse(services, 0);
  return request<ServiceDefinition[]>('/service-items');
}

/** GET /job-accounts/{jobAccountId} */
export function getJobAccount(jobAccountId: string): Promise<JobAccount | null> {
  if (useMockApi) {
    return mockResponse(jobAccount.jobAccountId === jobAccountId ? jobAccount : null);
  }
  return request<JobAccount>(`/job-accounts/${encodeURIComponent(jobAccountId)}`);
}

/** GET /quotes?jobAccountId= */
export function getQuotes(jobAccountId?: string | null): Promise<Quote[]> {
  if (useMockApi) {
    const matches = jobAccountId
      ? quotes.filter((q) => q.jobAccountId === jobAccountId)
      : quotes;
    return mockResponse(matches);
  }
  const query = jobAccountId ? `?jobAccountId=${encodeURIComponent(jobAccountId)}` : '';
  return request<Quote[]>(`/quotes${query}`);
}

/** GET /quotes/{quoteId} */
export function getQuote(quoteId: string): Promise<Quote | null> {
  if (useMockApi) return mockResponse(quotes.find((q) => q.quoteId === quoteId) ?? null);
  return request<Quote>(`/quotes/${encodeURIComponent(quoteId)}`);
}

/** POST /transactions — persists the transaction header, lines and tenders. */
export function postTransaction(transaction: Transaction): Promise<Transaction> {
  if (useMockApi) return mockResponse({ ...transaction, status: 'COMPLETED' as const }, 250);
  return request<Transaction>('/transactions', {
    method: 'POST',
    body: JSON.stringify(transaction),
  });
}

/** POST /transactions/{transactionId}/tenders */
export function postTender(transactionId: string, tender: Tender): Promise<Tender> {
  if (useMockApi) return mockResponse(tender, 150);
  return request<Tender>(`/transactions/${encodeURIComponent(transactionId)}/tenders`, {
    method: 'POST',
    body: JSON.stringify(tender),
  });
}

export type { ServiceDefinition };
