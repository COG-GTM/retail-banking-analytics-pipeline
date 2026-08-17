import { useTransactionStore } from '../store/transactionStore';
import { formatCurrency } from '../utils/format';

export function LineItemGrid() {
  const lines = useTransactionStore((state) => state.lines);
  const updateQuantity = useTransactionStore((state) => state.updateQuantity);
  const removeLine = useTransactionStore((state) => state.removeLine);

  return (
    <section className="panel line-items">
      <h2>Transaction lines</h2>
      <table className="grid">
        <thead>
          <tr>
            <th className="col-num">#</th>
            <th>Description</th>
            <th className="col-sku">SKU</th>
            <th className="col-qty">Qty</th>
            <th className="col-uom">UOM</th>
            <th className="col-money">Unit price</th>
            <th className="col-money">Extended</th>
            <th className="col-action" />
          </tr>
        </thead>
        <tbody>
          {lines.length === 0 && (
            <tr>
              <td colSpan={8} className="empty">
                No items — scan a SKU or convert a quote to begin.
              </td>
            </tr>
          )}
          {lines.map((line) => (
            <tr key={line.lineId} className={line.lineType === 'DISCOUNT' ? 'row-discount' : ''}>
              <td className="col-num">{line.lineNumber}</td>
              <td>
                <div className="line-desc">{line.description}</div>
                {line.lineType !== 'MERCHANDISE' && (
                  <span className="line-type">{line.lineType.replace('_', ' ')}</span>
                )}
              </td>
              <td className="col-sku">{line.sku ?? '—'}</td>
              <td className="col-qty">
                <input
                  type="number"
                  min={line.lineType === 'DISCOUNT' ? 1 : 0}
                  step={1}
                  value={line.quantity}
                  disabled={line.lineType === 'DISCOUNT'}
                  onChange={(event) =>
                    updateQuantity(line.lineId, Math.max(0, Number(event.target.value)))
                  }
                  aria-label={`Quantity for ${line.description}`}
                />
              </td>
              <td className="col-uom">{line.unitOfMeasure}</td>
              <td className="col-money">{formatCurrency(line.unitPrice)}</td>
              <td className="col-money">{formatCurrency(line.extendedAmount)}</td>
              <td className="col-action">
                <button
                  type="button"
                  className="btn btn-remove"
                  onClick={() => removeLine(line.lineId)}
                  aria-label={`Remove ${line.description}`}
                >
                  ✕
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}
