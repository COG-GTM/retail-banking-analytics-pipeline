import { describe, expect, it } from 'vitest';
import { formatMoney, formatValue, toColumnName, toFieldRows } from './format';

describe('formatting helpers', () => {
  it('maps DTO fields back to SQL column names', () => {
    expect(toColumnName('compositeRiskScore')).toBe('COMPOSITE_RISK_SCORE');
    expect(toColumnName('scoreDelta30d')).toBe('SCORE_DELTA30D');
  });

  it('renders nulls as -- and fixes decimals', () => {
    expect(formatValue(null)).toBe('--');
    expect(formatValue(12)).toBe('12');
    expect(formatValue(12.3456)).toBe('12.35');
    expect(formatMoney(17603.62)).toBe('17,603.62');
  });

  it('converts a record into column/value rows', () => {
    expect(toFieldRows({ customerId: 3, riskTier: 'LOW' })).toEqual([
      ['CUSTOMER_ID', '3'],
      ['RISK_TIER', 'LOW'],
    ]);
  });
});
