import { FieldGrid } from '../components/FieldGrid';
import { PanelState } from '../components/PanelState';
import { useRiskScore } from '../hooks/queries';
import { useCustomer } from '../state/CustomerContext';

export function RiskPanel() {
  const { customerId } = useCustomer();
  const { data, isLoading, error } = useRiskScore(customerId);

  return (
    <PanelState isLoading={isLoading} error={error} hasCustomer={Boolean(customerId)}>
      {data && <FieldGrid record={data} caption="CUSTOMER_RISK_SCORES" />}
    </PanelState>
  );
}
