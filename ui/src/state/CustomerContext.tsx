import { createContext, ReactNode, useCallback, useContext, useMemo, useState } from 'react';

interface CustomerContextValue {
  customerId: string | null;
  setCustomerId: (id: string | null) => void;
}

const CustomerContext = createContext<CustomerContextValue | undefined>(undefined);

export function CustomerProvider({
  children,
  initialCustomerId = null,
}: {
  children: ReactNode;
  initialCustomerId?: string | null;
}) {
  const [customerId, setCustomerIdState] = useState<string | null>(initialCustomerId);

  const setCustomerId = useCallback((id: string | null) => {
    setCustomerIdState(id && id.trim() !== '' ? id.trim() : null);
  }, []);

  const value = useMemo(() => ({ customerId, setCustomerId }), [customerId, setCustomerId]);

  return <CustomerContext.Provider value={value}>{children}</CustomerContext.Provider>;
}

export function useCustomer(): CustomerContextValue {
  const ctx = useContext(CustomerContext);
  if (!ctx) throw new Error('useCustomer must be used inside a CustomerProvider');
  return ctx;
}
