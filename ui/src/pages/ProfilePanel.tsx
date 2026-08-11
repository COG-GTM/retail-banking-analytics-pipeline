import { FieldGrid } from '../components/FieldGrid';
import { PanelState } from '../components/PanelState';
import { useProfile } from '../hooks/queries';
import { useCustomer } from '../state/CustomerContext';

export function ProfilePanel() {
  const { customerId } = useCustomer();
  const { data, isLoading, error } = useProfile(customerId);

  return (
    <PanelState isLoading={isLoading} error={error} hasCustomer={Boolean(customerId)}>
      {data && <FieldGrid record={data} caption="CUSTOMER_MASTER_PROFILE" />}
    </PanelState>
  );
}
