import { useTransactionStore } from '../store/transactionStore';
import type { TransactionTotals } from '../types/pos';
import { formatCurrency, formatPercent } from '../utils/format';

interface TotalsPanelProps {
  totals: TransactionTotals;
  taxRate: number;
}

export function TotalsPanel({ totals, taxRate }: TotalsPanelProps) {
  const taxExempt = useTransactionStore((state) => state.taxExempt);
  const setTaxExempt = useTransactionStore((state) => state.setTaxExempt);

  return (
    <section className="panel totals">
      <h2>Totals</h2>
      <dl>
        <div>
          <dt>Subtotal</dt>
          <dd>{formatCurrency(totals.subtotal)}</dd>
        </div>
        <div>
          <dt>Discounts</dt>
          <dd className="negative">{formatCurrency(totals.discountTotal)}</dd>
        </div>
        <div>
          <dt>Sales tax {taxExempt ? '(exempt)' : `(${formatPercent(taxRate)})`}</dt>
          <dd>{formatCurrency(totals.taxAmount)}</dd>
        </div>
        <div className="grand">
          <dt>Total</dt>
          <dd>{formatCurrency(totals.total)}</dd>
        </div>
      </dl>
      <label className="toggle">
        <input
          type="checkbox"
          checked={taxExempt}
          onChange={(event) => setTaxExempt(event.target.checked)}
        />
        Apply tax exempt
      </label>
    </section>
  );
}
