import type { TransactionLine, TransactionTotals, Tender } from '../types/pos';

export function round2(value: number): number {
  return Math.round((value + Number.EPSILON) * 100) / 100;
}

export function computeTotals(
  lines: TransactionLine[],
  tenders: Tender[],
  taxRate: number,
  taxExempt: boolean,
): TransactionTotals {
  const subtotal = round2(
    lines
      .filter((line) => line.lineType !== 'DISCOUNT')
      .reduce((sum, line) => sum + line.extendedAmount, 0),
  );
  const discountTotal = round2(
    lines
      .filter((line) => line.lineType === 'DISCOUNT')
      .reduce((sum, line) => sum + line.extendedAmount, 0),
  );
  const taxableBase = taxExempt
    ? 0
    : round2(
        lines
          .filter((line) => line.taxable)
          .reduce((sum, line) => sum + line.extendedAmount, 0),
      );
  const taxAmount = round2(taxableBase * taxRate);
  const total = round2(subtotal + discountTotal + taxAmount);
  const tenderedAmount = round2(tenders.reduce((sum, tender) => sum + tender.amount, 0));

  return {
    subtotal,
    discountTotal,
    taxableBase,
    taxAmount,
    total,
    tenderedAmount,
    balanceDue: round2(total - tenderedAmount),
  };
}
