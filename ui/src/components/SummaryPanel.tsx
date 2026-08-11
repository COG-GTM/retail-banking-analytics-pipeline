import { useProfile } from '../hooks/queries';
import { useCustomer } from '../state/CustomerContext';
import { formatMoney, formatValue } from './format';

/** Right-hand "running total" panel — headline KPIs from CUSTOMER_MASTER_PROFILE. */
export function SummaryPanel() {
  const { customerId } = useCustomer();
  const { data, isLoading, error } = useProfile(customerId);

  const lines: Array<[string, string]> = data
    ? [
        ['NAME', formatValue(data.fullName)],
        ['SEGMENT', formatValue(data.segmentName)],
        ['RISK TIER', formatValue(data.riskTier)],
        ['RISK SCORE', formatValue(data.compositeRiskScore)],
        ['TENURE (MO)', formatValue(data.tenureMonths)],
        ['ACCOUNTS', `${formatValue(data.activeAccounts)} / ${formatValue(data.totalAccounts)}`],
      ]
    : [];

  return (
    <aside className="summary" aria-label="Customer summary">
      <div className="summary-head">CUSTOMER SUMMARY</div>
      {!customerId && <div className="summary-empty">NO CUSTOMER</div>}
      {customerId && isLoading && <div className="summary-empty">LOADING&hellip;</div>}
      {customerId && error && (
        <div className="summary-empty summary-error">NO PROFILE AVAILABLE</div>
      )}
      {data && (
        <>
          <dl className="summary-list">
            {lines.map(([label, value]) => (
              <div key={label} className="summary-row">
                <dt>{label}</dt>
                <dd>{value}</dd>
              </div>
            ))}
          </dl>
          <div className="summary-total">
            <div className="summary-total-label">TOTAL BALANCE</div>
            <div className="summary-total-value" data-testid="total-balance">
              {formatMoney(data.totalBalance)}
            </div>
          </div>
          <div className="summary-total summary-total-alt">
            <div className="summary-total-label">NET CASH FLOW</div>
            <div className="summary-total-value" data-testid="net-cash-flow">
              {formatMoney(data.netCashFlow)}
            </div>
          </div>
        </>
      )}
    </aside>
  );
}
