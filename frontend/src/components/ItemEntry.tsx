import { useState } from 'react';
import { lookupProduct, searchProducts } from '../api/posService';
import { useTransactionStore } from '../store/transactionStore';
import type { Product } from '../types/pos';
import { formatCurrency } from '../utils/format';

export function ItemEntry() {
  const [term, setTerm] = useState('');
  const [quantity, setQuantity] = useState(1);
  const [product, setProduct] = useState<Product | null>(null);
  const [suggestions, setSuggestions] = useState<Product[]>([]);
  const [message, setMessage] = useState<string | null>(null);
  const addProduct = useTransactionStore((state) => state.addProduct);
  const addWarranty = useTransactionStore((state) => state.addWarranty);

  async function handleLookup(value: string) {
    setTerm(value);
    setMessage(null);
    setSuggestions(value.trim().length >= 2 ? await searchProducts(value) : []);
  }

  async function handleSubmit(event: React.FormEvent) {
    event.preventDefault();
    const found = await lookupProduct(term);
    if (!found) {
      setMessage(`No product found for "${term}"`);
      setProduct(null);
      return;
    }
    select(found);
  }

  function select(found: Product) {
    setProduct(found);
    setSuggestions([]);
    setTerm(found.sku);
    setMessage(null);
  }

  function handleAdd() {
    if (!product) return;
    addProduct(product, quantity);
    setMessage(`Added ${quantity} ${product.unitOfMeasure} — ${product.productDescription}`);
    setTerm('');
    setQuantity(1);
  }

  return (
    <section className="panel item-entry">
      <h2>Item entry</h2>
      <form className="entry-row" onSubmit={handleSubmit}>
        <input
          className="sku-input"
          placeholder="Scan or enter SKU / item lookup"
          value={term}
          onChange={(event) => void handleLookup(event.target.value)}
          aria-label="SKU or item lookup"
        />
        <input
          className="qty-input"
          type="number"
          min={1}
          step={1}
          value={quantity}
          onChange={(event) => setQuantity(Math.max(1, Number(event.target.value)))}
          aria-label="Quantity"
        />
        <button type="submit" className="btn btn-secondary">
          Look up
        </button>
        <button type="button" className="btn btn-primary" disabled={!product} onClick={handleAdd}>
          Add to cart
        </button>
      </form>

      {suggestions.length > 0 && (
        <ul className="suggestions">
          {suggestions.map((item) => (
            <li key={item.sku}>
              <button type="button" onClick={() => select(item)}>
                <span className="sug-sku">{item.sku}</span>
                <span className="sug-desc">{item.productDescription}</span>
                <span className="sug-price">
                  {formatCurrency(item.unitPrice)} / {item.unitOfMeasure}
                </span>
              </button>
            </li>
          ))}
        </ul>
      )}

      {product && (
        <div className="product-card">
          <div className="product-main">
            <div className="product-desc">{product.productDescription}</div>
            <div className="product-meta">
              SKU {product.sku} · {formatCurrency(product.unitPrice)} / {product.unitOfMeasure}
            </div>
          </div>
          <div className="location">
            {product.aisle} · bay {product.bay}
          </div>
          {product.warrantyEligible && (
            <button type="button" className="btn btn-ghost" onClick={() => addWarranty(product)}>
              + 2-year protection plan
              {product.warrantyPrice ? ` (${formatCurrency(product.warrantyPrice)})` : ''}
            </button>
          )}
        </div>
      )}

      {message && <p className="entry-message">{message}</p>}
    </section>
  );
}
