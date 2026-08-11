export interface FunctionKey {
  key: string;
  label: string;
}

export const FUNCTION_KEYS: FunctionKey[] = [
  { key: 'F1', label: 'PROFILE' },
  { key: 'F2', label: 'SEGMENT' },
  { key: 'F3', label: 'RISK' },
  { key: 'F4', label: 'TXN' },
  { key: 'F5', label: 'REFRESH' },
  { key: 'F6', label: 'LOOKUP' },
  { key: 'F7', label: 'PREV PANEL' },
  { key: 'F8', label: 'NEXT PANEL' },
  { key: 'F9', label: 'PERIOD' },
  { key: 'F10', label: 'MOCK/API' },
  { key: 'F11', label: 'HELP' },
  { key: 'F12', label: 'CLEAR' },
];

export function FunctionKeyBar({ message }: { message: string }) {
  return (
    <footer className="fkeybar">
      <div className="fkeys" aria-label="Function keys">
        {FUNCTION_KEYS.map((fk) => (
          <span key={fk.key} className="fkey">
            <b>{fk.key}</b> {fk.label}
          </span>
        ))}
      </div>
      <div className="fkey-message" role="status" data-testid="status-message">
        {message}
      </div>
    </footer>
  );
}
