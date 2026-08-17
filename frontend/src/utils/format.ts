const currency = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' });

export function formatCurrency(value: number): string {
  return currency.format(value);
}

export function formatQuantity(value: number): string {
  return Number.isInteger(value) ? String(value) : value.toFixed(2);
}

export function formatPercent(rate: number): string {
  return `${(rate * 100).toFixed(3)}%`;
}
