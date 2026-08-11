import { useCallback, useEffect, useRef, useState } from 'react';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import { ActionPanel, ACTIONS } from '../components/ActionPanel';
import { CustomerSearch } from '../components/CustomerSearch';
import { FunctionKeyBar } from '../components/FunctionKeyBar';
import { StatusBar } from '../components/StatusBar';
import { SummaryPanel } from '../components/SummaryPanel';
import { USE_MOCKS } from '../api';
import { useCustomer } from '../state/CustomerContext';

const HELP_MESSAGE = 'F1-F4 PANELS | F5 REFRESH | F6 LOOKUP | F7/F8 CYCLE | F9 PERIOD | F12 CLEAR';

export function OperatorConsole() {
  const navigate = useNavigate();
  const location = useLocation();
  const queryClient = useQueryClient();
  const { customerId, setCustomerId } = useCustomer();
  const searchRef = useRef<HTMLInputElement>(null);
  const [message, setMessage] = useState(HELP_MESSAGE);

  const panelIndex = Math.max(
    0,
    ACTIONS.findIndex((action) => action.to === location.pathname),
  );

  const goToPanel = useCallback(
    (index: number) => {
      const target = ACTIONS[(index + ACTIONS.length) % ACTIONS.length];
      navigate(target.to);
      setMessage(`PANEL ${target.label}`);
    },
    [navigate],
  );

  useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      const handlers: Record<string, () => void> = {
        F1: () => goToPanel(0),
        F2: () => goToPanel(1),
        F3: () => goToPanel(2),
        F4: () => goToPanel(3),
        F5: () => {
          queryClient.invalidateQueries();
          setMessage('REFRESHED DATA PRODUCTS');
        },
        F6: () => {
          searchRef.current?.focus();
          searchRef.current?.select();
          setMessage('ENTER CUSTOMER_ID');
        },
        F7: () => goToPanel(panelIndex - 1),
        F8: () => goToPanel(panelIndex + 1),
        F9: () => {
          navigate('/transactions');
          setMessage('SET REPORTING_PERIOD (YYYY-MM)');
        },
        F10: () => setMessage(USE_MOCKS ? 'SOURCE: MOCK FIXTURES' : 'SOURCE: SPRING BOOT API'),
        F11: () => setMessage(HELP_MESSAGE),
        F12: () => {
          setCustomerId(null);
          setMessage('CUSTOMER CONTEXT CLEARED');
        },
      };

      const handler = handlers[event.key];
      if (handler) {
        event.preventDefault();
        handler();
      }
    }

    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [goToPanel, navigate, panelIndex, queryClient, setCustomerId]);

  return (
    <div className="console">
      <StatusBar />
      <div className="console-body">
        <ActionPanel
          onLookup={() => {
            searchRef.current?.focus();
            setMessage('ENTER CUSTOMER_ID');
          }}
        />
        <main className="main">
          <CustomerSearch
            ref={searchRef}
            onSubmitted={(id) => setMessage(`CUSTOMER_ID ${id} SELECTED`)}
          />
          <div className="main-grid">
            <Outlet />
          </div>
        </main>
        <SummaryPanel />
      </div>
      <FunctionKeyBar message={customerId ? message : 'AWAITING CUSTOMER LOOKUP — PRESS F6'} />
    </div>
  );
}
