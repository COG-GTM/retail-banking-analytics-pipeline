import { forwardRef, FormEvent, useState } from 'react';
import { useCustomer } from '../state/CustomerContext';

interface CustomerSearchProps {
  onSubmitted?: (customerId: string) => void;
}

export const CustomerSearch = forwardRef<HTMLInputElement, CustomerSearchProps>(
  function CustomerSearch({ onSubmitted }, ref) {
    const { customerId, setCustomerId } = useCustomer();
    const [value, setValue] = useState(customerId ?? '');
    const [localError, setLocalError] = useState<string | null>(null);

    function handleSubmit(event: FormEvent) {
      event.preventDefault();
      const trimmed = value.trim();
      if (!/^\d+$/.test(trimmed)) {
        setLocalError('CUSTOMER_ID must be a positive integer');
        return;
      }
      setLocalError(null);
      setCustomerId(trimmed);
      onSubmitted?.(trimmed);
    }

    return (
      <form className="search" onSubmit={handleSubmit}>
        <label className="search-label" htmlFor="customer-id-input">
          CUSTOMER_ID
        </label>
        <input
          id="customer-id-input"
          ref={ref}
          className="search-input"
          inputMode="numeric"
          autoComplete="off"
          value={value}
          onChange={(event) => setValue(event.target.value)}
        />
        <button type="submit" className="search-submit">
          ENTER
        </button>
        {localError && (
          <span className="search-error" role="alert">
            {localError}
          </span>
        )}
      </form>
    );
  },
);
