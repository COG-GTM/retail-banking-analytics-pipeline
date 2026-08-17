import { describe, expect, it } from 'vitest';
import type { Tender, TransactionLine } from '../types/pos';
import { computeTotals } from './totals';

const lines: TransactionLine[] = [
  {
    lineId: 'l1',
    lineNumber: 1,
    lineType: 'MERCHANDISE',
    sku: '1000412774',
    description: 'Paint',
    quantity: 10,
    unitOfMeasure: 'GAL',
    unitPrice: 34.98,
    extendedAmount: 349.8,
    taxable: true,
  },
  {
    lineId: 'l2',
    lineNumber: 2,
    lineType: 'WARRANTY',
    sku: 'WTY-1',
    description: '2-year protection plan',
    quantity: 1,
    unitOfMeasure: 'EA',
    unitPrice: 34.97,
    extendedAmount: 34.97,
    taxable: false,
  },
  {
    lineId: 'l3',
    lineNumber: 3,
    lineType: 'DISCOUNT',
    sku: 'DISC-VOL',
    description: 'Volume pricing',
    quantity: 1,
    unitOfMeasure: 'EA',
    unitPrice: -17.49,
    extendedAmount: -17.49,
    taxable: true,
  },
];

describe('computeTotals', () => {
  it('sums merchandise, warranty and discount lines with tax on taxable lines', () => {
    const totals = computeTotals(lines, [], 0.08625, false);
    expect(totals.subtotal).toBe(384.77);
    expect(totals.discountTotal).toBe(-17.49);
    expect(totals.taxableBase).toBe(332.31);
    expect(totals.taxAmount).toBe(28.66);
    expect(totals.total).toBe(395.94);
  });

  it('zeroes tax when the job account is tax exempt', () => {
    const totals = computeTotals(lines, [], 0.08625, true);
    expect(totals.taxAmount).toBe(0);
    expect(totals.total).toBe(367.28);
  });

  it('tracks balance due across a split tender', () => {
    const tenders: Tender[] = [
      { tenderId: 't1', tenderType: 'COMMERCIAL_REVOLVING', amount: 300 },
      { tenderId: 't2', tenderType: 'CASH', amount: 95.94 },
    ];
    const totals = computeTotals(lines, tenders, 0.08625, false);
    expect(totals.tenderedAmount).toBe(395.94);
    expect(totals.balanceDue).toBe(0);
  });
});
