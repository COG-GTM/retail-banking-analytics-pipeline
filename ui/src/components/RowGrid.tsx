import { formatValue, toColumnName } from './format';

interface RowGridProps<T extends { customerId: number }> {
  rows: T[];
  columns: Array<keyof T & string>;
  caption: string;
}

/** Dense row-per-record grid used for multi-row data products (transactions). */
export function RowGrid<T extends { customerId: number }>({
  rows,
  columns,
  caption,
}: RowGridProps<T>) {
  return (
    <table className="grid" aria-label={caption}>
      <caption className="grid-caption">{caption}</caption>
      <thead>
        <tr>
          {columns.map((column) => (
            <th key={column} scope="col">
              {toColumnName(column)}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, index) => (
          <tr key={`${String(row.customerId)}-${index}`}>
            {columns.map((column) => (
              <td key={column} className="value">
                {formatValue((row as Record<string, unknown>)[column])}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
