import { NavLink } from 'react-router-dom';

export interface ActionItem {
  key: string;
  label: string;
  to: string;
}

export const ACTIONS: ActionItem[] = [
  { key: 'F1', label: 'MASTER PROFILE', to: '/profile' },
  { key: 'F2', label: 'SEGMENTS', to: '/segment' },
  { key: 'F3', label: 'RISK', to: '/risk' },
  { key: 'F4', label: 'TRANSACTIONS', to: '/transactions' },
];

export function ActionPanel({ onLookup }: { onLookup: () => void }) {
  return (
    <nav className="actions" aria-label="Primary actions">
      <button type="button" className="action action-primary" onClick={onLookup}>
        <span className="action-key">F6</span>
        <span className="action-label">LOOKUP CUSTOMER</span>
      </button>
      {ACTIONS.map((action) => (
        <NavLink
          key={action.to}
          to={action.to}
          className={({ isActive }) => `action${isActive ? ' action-active' : ''}`}
        >
          <span className="action-key">{action.key}</span>
          <span className="action-label">{action.label}</span>
        </NavLink>
      ))}
    </nav>
  );
}
