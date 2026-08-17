import { create } from 'zustand';
import type { ServiceDefinition } from '../api/posService';
import { postTransaction } from '../api/posService';
import type {
  JobAccount,
  Product,
  Quote,
  RegisterContext,
  Tender,
  TenderType,
  Transaction,
  TransactionLine,
  TransactionTotals,
} from '../types/pos';
import { computeTotals, round2 } from './totals';

let sequence = 0;
const nextId = (prefix: string) => `${prefix}-${Date.now().toString(36)}-${sequence++}`;

interface TransactionState {
  context: RegisterContext | null;
  jobAccount: JobAccount | null;
  transactionId: string;
  businessDate: string;
  lines: TransactionLine[];
  tenders: Tender[];
  taxExempt: boolean;
  status: Transaction['status'];
  loadedQuoteId: string | null;
  setContext: (context: RegisterContext, jobAccount: JobAccount | null) => void;
  addProduct: (product: Product, quantity?: number) => void;
  addServiceLine: (service: ServiceDefinition, quantity?: number) => void;
  addWarranty: (product: Product) => void;
  addVolumeDiscount: () => void;
  updateQuantity: (lineId: string, quantity: number) => void;
  removeLine: (lineId: string) => void;
  setTaxExempt: (taxExempt: boolean) => void;
  addTender: (tenderType: TenderType, amount: number) => void;
  removeTender: (tenderId: string) => void;
  loadQuote: (quote: Quote) => void;
  completeTransaction: () => Promise<void>;
  newTransaction: () => void;
  totals: () => TransactionTotals;
  toTransaction: () => Transaction;
}

const VOLUME_DISCOUNT_RATE = 0.05;

function renumber(lines: TransactionLine[]): TransactionLine[] {
  return lines.map((line, index) => ({ ...line, lineNumber: index + 1 }));
}

function makeTransactionId(storeId: string, registerId: string): string {
  const sequenceNumber = 84291 + Math.floor(Math.random() * 9);
  return `${storeId}/${registerId}/${sequenceNumber}`;
}

export const useTransactionStore = create<TransactionState>((set, get) => ({
  context: null,
  jobAccount: null,
  transactionId: '—',
  businessDate: new Date().toISOString().slice(0, 10),
  lines: [],
  tenders: [],
  taxExempt: false,
  status: 'OPEN',
  loadedQuoteId: null,

  setContext: (context, jobAccount) =>
    set((state) => ({
      context,
      jobAccount,
      transactionId:
        state.transactionId === '—'
          ? makeTransactionId(context.store.storeId, context.register.registerId)
          : state.transactionId,
    })),

  addProduct: (product, quantity = 1) =>
    set((state) => {
      const existing = state.lines.find(
        (line) => line.lineType === 'MERCHANDISE' && line.sku === product.sku,
      );
      if (existing) {
        const newQuantity = round2(existing.quantity + quantity);
        return {
          lines: state.lines.map((line) =>
            line.lineId === existing.lineId
              ? {
                  ...line,
                  quantity: newQuantity,
                  extendedAmount: round2(newQuantity * line.unitPrice),
                }
              : line,
          ),
        };
      }
      const line: TransactionLine = {
        lineId: nextId('line'),
        lineNumber: state.lines.length + 1,
        lineType: 'MERCHANDISE',
        sku: product.sku,
        description: product.productDescription,
        quantity,
        unitOfMeasure: product.unitOfMeasure,
        unitPrice: product.unitPrice,
        extendedAmount: round2(quantity * product.unitPrice),
        taxable: product.taxable,
      };
      return { lines: [...state.lines, line] };
    }),

  addServiceLine: (service, quantity = 1) =>
    set((state) => ({
      lines: [
        ...state.lines,
        {
          lineId: nextId('line'),
          lineNumber: state.lines.length + 1,
          lineType: service.lineType,
          sku: service.code,
          description: service.description,
          quantity,
          unitOfMeasure: service.unitOfMeasure,
          unitPrice: service.unitPrice,
          extendedAmount: round2(quantity * service.unitPrice),
          taxable: service.taxable,
        },
      ],
    })),

  addWarranty: (product) =>
    set((state) => {
      const price = product.warrantyPrice ?? round2(product.unitPrice * 0.125);
      return {
        lines: [
          ...state.lines,
          {
            lineId: nextId('line'),
            lineNumber: state.lines.length + 1,
            lineType: 'WARRANTY',
            sku: `WTY-${product.sku}`,
            description: `2-year protection plan — ${product.productDescription}`,
            quantity: 1,
            unitOfMeasure: 'EA',
            unitPrice: price,
            extendedAmount: price,
            taxable: false,
          },
        ],
      };
    }),

  addVolumeDiscount: () =>
    set((state) => {
      const merchandiseTotal = state.lines
        .filter((line) => line.lineType === 'MERCHANDISE')
        .reduce((sum, line) => sum + line.extendedAmount, 0);
      if (merchandiseTotal <= 0) return state;
      const amount = round2(-merchandiseTotal * VOLUME_DISCOUNT_RATE);
      const existing = state.lines.find((line) => line.lineType === 'DISCOUNT');
      if (existing) {
        return {
          lines: state.lines.map((line) =>
            line.lineId === existing.lineId
              ? { ...line, unitPrice: amount, extendedAmount: amount }
              : line,
          ),
        };
      }
      return {
        lines: [
          ...state.lines,
          {
            lineId: nextId('line'),
            lineNumber: state.lines.length + 1,
            lineType: 'DISCOUNT',
            sku: 'DISC-VOL',
            description: 'Pro volume pricing (5% merchandise)',
            quantity: 1,
            unitOfMeasure: 'EA',
            unitPrice: amount,
            extendedAmount: amount,
            taxable: true,
          },
        ],
      };
    }),

  updateQuantity: (lineId, quantity) =>
    set((state) => ({
      lines: state.lines.map((line) =>
        line.lineId === lineId
          ? {
              ...line,
              quantity,
              extendedAmount: round2(quantity * line.unitPrice),
            }
          : line,
      ),
    })),

  removeLine: (lineId) =>
    set((state) => ({
      lines: renumber(state.lines.filter((line) => line.lineId !== lineId)),
    })),

  setTaxExempt: (taxExempt) => set({ taxExempt }),

  addTender: (tenderType, amount) =>
    set((state) => ({
      tenders: [
        ...state.tenders,
        {
          tenderId: nextId('tndr'),
          tenderType,
          amount: round2(amount),
          authCode:
            tenderType === 'CASH' ? undefined : Math.random().toString(36).slice(2, 8).toUpperCase(),
          cardLastFour: tenderType === 'CASH' ? undefined : '4417',
        },
      ],
    })),

  removeTender: (tenderId) =>
    set((state) => ({ tenders: state.tenders.filter((t) => t.tenderId !== tenderId) })),

  loadQuote: (quote) =>
    set((state) => {
      const offset = state.lines.length;
      const quoteLines: TransactionLine[] = quote.lines.map((line, index) => ({
        ...line,
        lineId: nextId('line'),
        lineNumber: offset + index + 1,
      }));
      return { lines: [...state.lines, ...quoteLines], loadedQuoteId: quote.quoteId };
    }),

  completeTransaction: async () => {
    const saved = await postTransaction(get().toTransaction());
    set({ status: saved.status });
  },

  newTransaction: () =>
    set((state) => ({
      lines: [],
      tenders: [],
      taxExempt: false,
      status: 'OPEN',
      loadedQuoteId: null,
      transactionId: state.context
        ? makeTransactionId(state.context.store.storeId, state.context.register.registerId)
        : state.transactionId,
    })),

  totals: () => {
    const { lines, tenders, context, taxExempt } = get();
    return computeTotals(lines, tenders, context?.store.taxRate ?? 0, taxExempt);
  },

  toTransaction: () => {
    const state = get();
    return {
      transactionId: state.transactionId,
      storeId: state.context?.store.storeId ?? '',
      registerId: state.context?.register.registerId ?? '',
      employeeId: state.context?.employee.employeeId ?? '',
      jobAccountId: state.jobAccount?.jobAccountId ?? null,
      status: state.status,
      businessDate: state.businessDate,
      taxExempt: state.taxExempt,
      taxRate: state.context?.store.taxRate ?? 0,
      lines: state.lines,
      tenders: state.tenders,
    };
  },
}));
