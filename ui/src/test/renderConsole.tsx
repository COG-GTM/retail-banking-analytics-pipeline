import { ReactElement } from 'react';
import { render } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { CustomerProvider } from '../state/CustomerContext';

export function renderWithProviders(
  ui: ReactElement,
  { route = '/', customerId = null as string | null } = {},
) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });

  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[route]}>
        <CustomerProvider initialCustomerId={customerId}>{ui}</CustomerProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}
