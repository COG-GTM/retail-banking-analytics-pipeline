import { afterEach, describe, expect, it, vi } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { App } from '../App';
import { api } from '../api';
import { ApiError } from '../api/types';
import { renderWithProviders } from '../test/renderConsole';

afterEach(() => {
  vi.restoreAllMocks();
});

describe('operator console shell', () => {
  it('renders header, action panel, summary panel and function-key bar', () => {
    renderWithProviders(<App />);

    expect(screen.getByText(/OPERATOR CONSOLE/)).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /MASTER PROFILE/ })).toBeInTheDocument();
    expect(screen.getByLabelText('Customer summary')).toBeInTheDocument();
    expect(screen.getByLabelText('Function keys')).toBeInTheDocument();
    expect(screen.getByTestId('header-customer')).toHaveTextContent('CUSTOMER_ID --------');
  });

  it('prompts for a customer before any lookup', () => {
    renderWithProviders(<App />);
    expect(screen.getByText(/NO CUSTOMER SELECTED/)).toBeInTheDocument();
  });

  it('requests the profile for the entered CUSTOMER_ID and renders fixture data', async () => {
    const getProfile = vi.spyOn(api, 'getProfile');
    const user = userEvent.setup();
    renderWithProviders(<App />);

    await user.type(screen.getByLabelText('CUSTOMER_ID'), '3');
    await user.click(screen.getByRole('button', { name: 'ENTER' }));

    await waitFor(() => expect(getProfile).toHaveBeenCalledWith('3'));
    expect(await screen.findByLabelText('CUSTOMER_MASTER_PROFILE')).toBeInTheDocument();
    expect((await screen.findAllByText('ENGAGED_MAINSTREAM')).length).toBeGreaterThan(0);
    expect(screen.getByTestId('total-balance')).toHaveTextContent('2,336.95');
  });

  it('rejects a non-numeric CUSTOMER_ID before calling the API', async () => {
    const getProfile = vi.spyOn(api, 'getProfile');
    const user = userEvent.setup();
    renderWithProviders(<App />);

    await user.type(screen.getByLabelText('CUSTOMER_ID'), 'ABC');
    await user.click(screen.getByRole('button', { name: 'ENTER' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('positive integer');
    expect(getProfile).not.toHaveBeenCalled();
  });

  it('switches panels with function keys', async () => {
    const user = userEvent.setup();
    renderWithProviders(<App />, { customerId: '3' });

    expect(await screen.findByLabelText('CUSTOMER_MASTER_PROFILE')).toBeInTheDocument();

    await user.keyboard('{F2}');
    expect(await screen.findByLabelText('CUSTOMER_SEGMENTS')).toBeInTheDocument();

    await user.keyboard('{F3}');
    expect(await screen.findByLabelText('CUSTOMER_RISK_SCORES')).toBeInTheDocument();

    await user.keyboard('{F4}');
    expect(await screen.findByLabelText('TRANSACTION_ANALYTICS')).toBeInTheDocument();

    await user.keyboard('{F1}');
    expect(await screen.findByLabelText('CUSTOMER_MASTER_PROFILE')).toBeInTheDocument();
  });

  it('clears the customer context with F12', async () => {
    const user = userEvent.setup();
    renderWithProviders(<App />, { customerId: '3' });

    expect(await screen.findByLabelText('CUSTOMER_MASTER_PROFILE')).toBeInTheDocument();
    await user.keyboard('{F12}');

    expect(await screen.findByText(/NO CUSTOMER SELECTED/)).toBeInTheDocument();
    expect(screen.getByTestId('header-customer')).toHaveTextContent('CUSTOMER_ID --------');
  });

  it('renders a 404 state for an unknown customer', async () => {
    renderWithProviders(<App />, { customerId: '99999999' });

    expect(await screen.findByText(/NOT FOUND \(404\)/)).toBeInTheDocument();
  });

  it('renders a 400 validation state with the API field errors', async () => {
    vi.spyOn(api, 'getProfile').mockRejectedValue(
      new ApiError(400, 'customerId must be a positive integer', {
        customerId: 'must be a positive integer',
      }),
    );

    renderWithProviders(<App />, { customerId: '3' });

    expect(await screen.findByText(/VALIDATION ERROR \(400\)/)).toBeInTheDocument();
    expect(screen.getByText(/customerId: must be a positive integer/)).toBeInTheDocument();
  });

  it('filters the transactions panel by REPORTING_PERIOD', async () => {
    const getTransactions = vi.spyOn(api, 'getTransactions');
    const user = userEvent.setup();
    renderWithProviders(<App />, { route: '/transactions', customerId: '3' });

    await user.type(screen.getByLabelText('REPORTING_PERIOD'), '2026-04');
    await user.click(screen.getByRole('button', { name: 'FILTER' }));

    await waitFor(() => expect(getTransactions).toHaveBeenCalledWith('3', '2026-04'));
    expect(await screen.findByLabelText('TRANSACTION_ANALYTICS')).toBeInTheDocument();
  });
});
