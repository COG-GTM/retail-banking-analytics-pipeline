import { useTransactionStore } from '../store/transactionStore';
import type { Quote } from '../types/pos';
import { formatCurrency } from '../utils/format';

interface QuoteDialogProps {
  quotes: Quote[];
  onClose: () => void;
}

export function QuoteDialog({ quotes, onClose }: QuoteDialogProps) {
  const loadQuote = useTransactionStore((state) => state.loadQuote);

  function convert(quote: Quote) {
    loadQuote(quote);
    onClose();
  }

  return (
    <div className="modal-backdrop" role="dialog" aria-modal="true" aria-label="Convert quote">
      <div className="modal">
        <header className="modal-header">
          <h2>Convert quote</h2>
          <button type="button" className="btn btn-remove" onClick={onClose} aria-label="Close">
            ✕
          </button>
        </header>
        <ul className="quote-list">
          {quotes.length === 0 && <li className="empty">No open quotes for this job account.</li>}
          {quotes.map((quote) => {
            const total = quote.lines.reduce((sum, line) => sum + line.extendedAmount, 0);
            return (
              <li key={quote.quoteId}>
                <div>
                  <div className="quote-id">{quote.quoteId}</div>
                  <div className="quote-desc">{quote.description}</div>
                  <div className="quote-meta">
                    {quote.lines.length} lines · expires {quote.expiresDate}
                  </div>
                </div>
                <div className="quote-total">{formatCurrency(total)}</div>
                <button type="button" className="btn btn-primary" onClick={() => convert(quote)}>
                  Load into transaction
                </button>
              </li>
            );
          })}
        </ul>
      </div>
    </div>
  );
}
