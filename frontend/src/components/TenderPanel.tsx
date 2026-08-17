import { useState } from 'react';
import { useTransactionStore } from '../store/transactionStore';
import type { TenderType, TransactionTotals } from '../types/pos';
import { formatCurrency } from '../utils/format';

const TENDER_LABELS: Record<TenderType, string> = {
  COMMERCIAL_REVOLVING: 'Commercial revolving charge',
  CREDIT_DEBIT: 'Credit / debit',
  CASH: 'Cash',
};

interface TenderPanelProps {
  totals: TransactionTotals;
}

export function TenderPanel({ totals }: TenderPanelProps) {
  const tenders = useTransactionStore((state) => state.tenders);
  const addTender = useTransactionStore((state) => state.addTender);
  const removeTender = useTransactionStore((state) => state.removeTender);
  const completeTransaction = useTransactionStore((state) => state.completeTransaction);
  const newTransaction = useTransactionStore((state) => state.newTransaction);
  const status = useTransactionStore((state) => state.status);
  const [amount, setAmount] = useState('');
  const [busy, setBusy] = useState(false);

  const balanceDue = totals.balanceDue;
  const requested = amount === '' ? balanceDue : Number(amount);
  const canTender = totals.total > 0 && requested > 0 && balanceDue > 0;

  function tender(tenderType: TenderType) {
    const applied = Math.min(requested, balanceDue);
    addTender(tenderType, applied);
    setAmount('');
  }

  async function complete() {
    setBusy(true);
    try {
      await completeTransaction();
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className="panel tender">
      <h2>Tender</h2>
      <div className="tender-amount">
        <label htmlFor="tender-amount">Amount</label>
        <input
          id="tender-amount"
          type="number"
          min={0}
          step="0.01"
          placeholder={balanceDue.toFixed(2)}
          value={amount}
          onChange={(event) => setAmount(event.target.value)}
        />
        <span className="hint">Leave blank to tender the full balance (split tender supported)</span>
      </div>

      <div className="tender-buttons">
        {(Object.keys(TENDER_LABELS) as TenderType[]).map((type) => (
          <button
            key={type}
            type="button"
            className="btn btn-tender"
            disabled={!canTender}
            onClick={() => tender(type)}
          >
            {TENDER_LABELS[type]}
          </button>
        ))}
      </div>

      {tenders.length > 0 && (
        <ul className="tender-rows">
          {tenders.map((row) => (
            <li key={row.tenderId}>
              <span>{TENDER_LABELS[row.tenderType]}</span>
              {row.cardLastFour && <span className="tender-meta">•••• {row.cardLastFour}</span>}
              <span className="tender-value">{formatCurrency(row.amount)}</span>
              <button
                type="button"
                className="btn btn-remove"
                onClick={() => removeTender(row.tenderId)}
                aria-label={`Remove ${TENDER_LABELS[row.tenderType]} tender`}
              >
                ✕
              </button>
            </li>
          ))}
        </ul>
      )}

      <div className="tender-summary">
        <div>
          <span>Tendered</span>
          <strong>{formatCurrency(totals.tenderedAmount)}</strong>
        </div>
        <div>
          <span>Balance due</span>
          <strong className={balanceDue <= 0 && totals.total > 0 ? 'paid' : ''}>
            {formatCurrency(balanceDue)}
          </strong>
        </div>
      </div>

      <div className="tender-actions">
        <button
          type="button"
          className="btn btn-primary btn-block"
          disabled={busy || totals.total <= 0 || balanceDue > 0 || status === 'COMPLETED'}
          onClick={() => void complete()}
        >
          {status === 'COMPLETED' ? 'Transaction complete' : 'Complete transaction'}
        </button>
        <button type="button" className="btn btn-ghost btn-block" onClick={newTransaction}>
          New transaction
        </button>
      </div>
    </section>
  );
}
