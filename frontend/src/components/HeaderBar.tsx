import type { JobAccount, RegisterContext } from '../types/pos';

interface HeaderBarProps {
  context: RegisterContext | null;
  jobAccount: JobAccount | null;
  transactionId: string;
  taxExempt: boolean;
}

export function HeaderBar({ context, jobAccount, transactionId, taxExempt }: HeaderBarProps) {
  return (
    <header className="header">
      <div className="header-top">
        <div className="brand">
          <span className="brand-mark">POS</span>
          <span className="brand-name">Store Checkout</span>
        </div>
        <div className="header-context">
          {context ? (
            <>
              <span>
                Store <strong>{context.store.storeId}</strong>
              </span>
              <span className="dot">·</span>
              <span>
                reg <strong>{context.register.registerId}</strong>
              </span>
              <span className="dot">·</span>
              <span>{context.register.registerType.toLowerCase()}</span>
              <span className="dot">·</span>
              <span>
                cashier <strong>{context.employee.displayName}</strong>
              </span>
            </>
          ) : (
            <span>Loading register context…</span>
          )}
        </div>
        <div className="header-txn">
          <span className="label">Transaction</span>
          <span className="txn-id">{transactionId}</span>
        </div>
      </div>

      {jobAccount && (
        <div className="job-banner">
          <span className="pill">{jobAccount.programName}</span>
          <strong>{jobAccount.companyName}</strong>
          <span className="dot">·</span>
          <span>
            PO/job <strong>{jobAccount.jobName}</strong> ({jobAccount.poNumber})
          </span>
          <span className="job-account-id">Account {jobAccount.jobAccountId}</span>
          {taxExempt && <span className="pill pill-exempt">TAX EXEMPT</span>}
        </div>
      )}
    </header>
  );
}
