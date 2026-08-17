import { useEffect, useState } from 'react';
import {
  getJobAccount,
  getQuotes,
  getRegisterContext,
  getServiceCatalog,
  type ServiceDefinition,
} from './api/posService';
import { HeaderBar } from './components/HeaderBar';
import { ItemEntry } from './components/ItemEntry';
import { LineItemGrid } from './components/LineItemGrid';
import { QuoteDialog } from './components/QuoteDialog';
import { ServiceButtons } from './components/ServiceButtons';
import { TenderPanel } from './components/TenderPanel';
import { TotalsPanel } from './components/TotalsPanel';
import { useTransactionStore } from './store/transactionStore';
import { computeTotals } from './store/totals';
import type { Quote } from './types/pos';

const DEFAULT_JOB_ACCOUNT_ID = 'PX-448120';

export default function App() {
  const [services, setServices] = useState<ServiceDefinition[]>([]);
  const [quotes, setQuotes] = useState<Quote[]>([]);
  const [quoteDialogOpen, setQuoteDialogOpen] = useState(false);

  const setContext = useTransactionStore((state) => state.setContext);
  const context = useTransactionStore((state) => state.context);
  const jobAccount = useTransactionStore((state) => state.jobAccount);
  const transactionId = useTransactionStore((state) => state.transactionId);
  const lines = useTransactionStore((state) => state.lines);
  const tenders = useTransactionStore((state) => state.tenders);
  const taxExempt = useTransactionStore((state) => state.taxExempt);

  useEffect(() => {
    async function bootstrap() {
      const [registerContext, account, serviceCatalog, openQuotes] = await Promise.all([
        getRegisterContext(),
        getJobAccount(DEFAULT_JOB_ACCOUNT_ID),
        getServiceCatalog(),
        getQuotes(DEFAULT_JOB_ACCOUNT_ID),
      ]);
      setContext(registerContext, account);
      setServices(serviceCatalog);
      setQuotes(openQuotes);
    }
    void bootstrap();
  }, [setContext]);

  const taxRate = context?.store.taxRate ?? 0;
  const totals = computeTotals(lines, tenders, taxRate, taxExempt);

  return (
    <div className="app">
      <HeaderBar
        context={context}
        jobAccount={jobAccount}
        transactionId={transactionId}
        taxExempt={taxExempt}
      />

      <main className="layout">
        <div className="column-main">
          <ItemEntry />
          <LineItemGrid />
          <ServiceButtons services={services} onConvertQuote={() => setQuoteDialogOpen(true)} />
        </div>
        <aside className="column-side">
          <TotalsPanel totals={totals} taxRate={taxRate} />
          <TenderPanel totals={totals} />
        </aside>
      </main>

      {quoteDialogOpen && (
        <QuoteDialog quotes={quotes} onClose={() => setQuoteDialogOpen(false)} />
      )}
    </div>
  );
}
