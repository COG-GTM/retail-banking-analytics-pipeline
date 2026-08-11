/** Turns a camelCase DTO field into the SQL column name used by the data products. */
export function toColumnName(field: string): string {
  return field.replace(/([a-z0-9])([A-Z])/g, '$1_$2').toUpperCase();
}

export function formatValue(value: unknown): string {
  if (value === null || value === undefined || value === '') return '--';
  if (typeof value === 'number') {
    return Number.isInteger(value) ? String(value) : value.toFixed(2);
  }
  return String(value);
}

export function formatMoney(value: number | null | undefined): string {
  if (value === null || value === undefined) return '--';
  return value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export function toFieldRows(record: object): Array<[string, string]> {
  return Object.entries(record).map(([key, value]) => [toColumnName(key), formatValue(value)]);
}
