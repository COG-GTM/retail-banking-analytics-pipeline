/**
 * Typed models mirroring the POS backend schema.
 * Entity names map 1:1 to backend tables: STORE, REGISTER, EMPLOYEE, PRODUCT/SKU,
 * CUSTOMER/JOB_ACCOUNT, TRANSACTION, TRANSACTION_LINE, TENDER, QUOTE.
 */

export type UnitOfMeasure = 'EA' | 'GAL' | 'BOX' | 'BAG' | 'LF' | 'CS';

export type LineType =
  | 'MERCHANDISE'
  | 'LUMBER_CUT'
  | 'PAINT_MIX'
  | 'TOOL_RENTAL'
  | 'SPECIAL_ORDER'
  | 'WILL_CALL'
  | 'DELIVERY'
  | 'WARRANTY'
  | 'DISCOUNT';

export type TenderType = 'COMMERCIAL_REVOLVING' | 'CREDIT_DEBIT' | 'CASH';

export type TransactionStatus = 'OPEN' | 'TENDERED' | 'COMPLETED' | 'VOIDED';

/** STORE */
export interface Store {
  storeId: string;
  storeName: string;
  taxRate: number;
  addressLine1: string;
  city: string;
  state: string;
  postalCode: string;
}

/** REGISTER */
export interface Register {
  registerId: string;
  storeId: string;
  registerType: string;
}

/** EMPLOYEE */
export interface Employee {
  employeeId: string;
  storeId: string;
  displayName: string;
  role: string;
}

/** PRODUCT / SKU */
export interface Product {
  sku: string;
  productDescription: string;
  unitOfMeasure: UnitOfMeasure;
  unitPrice: number;
  aisle: string;
  bay: string;
  taxable: boolean;
  warrantyEligible: boolean;
  warrantyPrice?: number;
}

/** CUSTOMER / JOB ACCOUNT */
export interface JobAccount {
  jobAccountId: string;
  customerId: string;
  programName: string;
  companyName: string;
  poNumber: string;
  jobName: string;
  taxExempt: boolean;
  taxExemptCertificate?: string;
}

/** TRANSACTION_LINE */
export interface TransactionLine {
  lineId: string;
  lineNumber: number;
  lineType: LineType;
  sku: string | null;
  description: string;
  quantity: number;
  unitOfMeasure: UnitOfMeasure;
  unitPrice: number;
  extendedAmount: number;
  taxable: boolean;
}

/** TENDER */
export interface Tender {
  tenderId: string;
  tenderType: TenderType;
  amount: number;
  authCode?: string;
  cardLastFour?: string;
}

/** TRANSACTION header */
export interface Transaction {
  transactionId: string;
  storeId: string;
  registerId: string;
  employeeId: string;
  jobAccountId: string | null;
  status: TransactionStatus;
  businessDate: string;
  taxExempt: boolean;
  taxRate: number;
  lines: TransactionLine[];
  tenders: Tender[];
}

/** QUOTE */
export interface Quote {
  quoteId: string;
  storeId: string;
  jobAccountId: string | null;
  createdDate: string;
  expiresDate: string;
  description: string;
  lines: Omit<TransactionLine, 'lineId'>[];
}

/** Derived totals (computed client-side, mirrors backend TRANSACTION totals columns). */
export interface TransactionTotals {
  subtotal: number;
  discountTotal: number;
  taxableBase: number;
  taxAmount: number;
  total: number;
  tenderedAmount: number;
  balanceDue: number;
}

export interface RegisterContext {
  store: Store;
  register: Register;
  employee: Employee;
}
