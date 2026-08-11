import { ENVIRONMENT, OPERATOR, USE_MOCKS } from '../api';
import { useApiHealth } from '../hooks/queries';
import { useCustomer } from '../state/CustomerContext';

export function StatusBar() {
  const { customerId } = useCustomer();
  const { data: healthy, isLoading, isError } = useApiHealth();

  const connection = USE_MOCKS
    ? 'MOCK FIXTURES'
    : isLoading
      ? 'CHECKING'
      : isError || !healthy
        ? 'OFFLINE'
        : 'ONLINE';

  return (
    <header className="statusbar">
      <span className="statusbar-title">RETAIL BANKING ANALYTICS &mdash; OPERATOR CONSOLE</span>
      <span className="statusbar-field">OPERATOR {OPERATOR}</span>
      <span className="statusbar-field">ENV {ENVIRONMENT}</span>
      <span className="statusbar-field" data-testid="api-status">
        API {connection}
      </span>
      <span className="statusbar-field statusbar-customer" data-testid="header-customer">
        CUSTOMER_ID {customerId ?? '--------'}
      </span>
    </header>
  );
}
