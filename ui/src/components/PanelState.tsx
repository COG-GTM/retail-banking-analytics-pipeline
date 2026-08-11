import { ApiError } from '../api/types';

interface PanelStateProps {
  isLoading: boolean;
  error: unknown;
  hasCustomer: boolean;
  children: React.ReactNode;
}

/** Renders the loading / no-customer / 400 / 404 / generic-error states of a panel. */
export function PanelState({ isLoading, error, hasCustomer, children }: PanelStateProps) {
  if (!hasCustomer) {
    return (
      <p className="panel-msg" role="status">
        NO CUSTOMER SELECTED &mdash; ENTER A CUSTOMER_ID (F6) TO BEGIN
      </p>
    );
  }
  if (isLoading) {
    return (
      <p className="panel-msg" role="status">
        LOADING&hellip;
      </p>
    );
  }
  if (error) {
    return <ErrorBox error={error} />;
  }
  return <>{children}</>;
}

export function ErrorBox({ error }: { error: unknown }) {
  if (error instanceof ApiError) {
    const label = error.isNotFound
      ? 'NOT FOUND (404)'
      : error.isValidation
        ? 'VALIDATION ERROR (400)'
        : `API ERROR (${error.status || 'OFFLINE'})`;
    return (
      <div className="panel-error" role="alert">
        <div className="panel-error-label">{label}</div>
        <div className="panel-error-msg">{error.message}</div>
        {Object.entries(error.fieldErrors).length > 0 && (
          <ul className="panel-error-fields">
            {Object.entries(error.fieldErrors).map(([field, msg]) => (
              <li key={field}>
                {field}: {msg}
              </li>
            ))}
          </ul>
        )}
      </div>
    );
  }

  return (
    <div className="panel-error" role="alert">
      <div className="panel-error-label">UNEXPECTED ERROR</div>
      <div className="panel-error-msg">{error instanceof Error ? error.message : String(error)}</div>
    </div>
  );
}
