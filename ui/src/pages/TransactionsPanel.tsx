import { FormEvent, useState } from 'react';
import { PanelState } from '../components/PanelState';
import { RowGrid } from '../components/RowGrid';
import { useTransactions } from '../hooks/queries';
import { useCustomer } from '../state/CustomerContext';
import { TransactionAnalytics } from '../api/types';

const COLUMNS: Array<keyof TransactionAnalytics & string> = [
  'reportingPeriod',
  'totalTransactions',
  'totalDebitAmt',
  'totalCreditAmt',
  'netCashFlow',
  'avgTransactionSize',
  'monthlySpendTrend',
  'spendPercentile',
  'topSpendCategory',
  'digitalTxnPct',
  'revenueContribution',
  'anomalyFlag',
];

export function TransactionsPanel() {
  const { customerId } = useCustomer();
  const [draftPeriod, setDraftPeriod] = useState('');
  const [period, setPeriod] = useState<string | undefined>(undefined);
  const { data, isLoading, error } = useTransactions(customerId, period);

  function applyPeriod(event: FormEvent) {
    event.preventDefault();
    setPeriod(draftPeriod.trim() === '' ? undefined : draftPeriod.trim());
  }

  return (
    <div className="txn-panel">
      <form className="period-form" onSubmit={applyPeriod}>
        <label className="search-label" htmlFor="reporting-period-input">
          REPORTING_PERIOD
        </label>
        <input
          id="reporting-period-input"
          className="search-input"
          placeholder="YYYY-MM"
          autoComplete="off"
          value={draftPeriod}
          onChange={(event) => setDraftPeriod(event.target.value)}
        />
        <button type="submit" className="search-submit">
          FILTER
        </button>
        <button
          type="button"
          className="search-submit"
          onClick={() => {
            setDraftPeriod('');
            setPeriod(undefined);
          }}
        >
          ALL
        </button>
      </form>
      <PanelState isLoading={isLoading} error={error} hasCustomer={Boolean(customerId)}>
        {data && <RowGrid rows={data} columns={COLUMNS} caption="TRANSACTION_ANALYTICS" />}
      </PanelState>
    </div>
  );
}
