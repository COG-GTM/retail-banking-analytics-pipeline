import { toFieldRows } from './format';

/** Dense two-column column/value grid — the "scanned items" area of the POS metaphor. */
export function FieldGrid({ record, caption }: { record: object; caption: string }) {
  const rows = toFieldRows(record);
  return (
    <table className="grid" aria-label={caption}>
      <caption className="grid-caption">{caption}</caption>
      <thead>
        <tr>
          <th scope="col">COLUMN</th>
          <th scope="col">VALUE</th>
        </tr>
      </thead>
      <tbody>
        {rows.map(([column, value]) => (
          <tr key={column}>
            <th scope="row">{column}</th>
            <td className="value">{value}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
