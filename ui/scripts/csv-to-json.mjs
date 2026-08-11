#!/usr/bin/env node
/*
 * Converts the SAS data-product CSV fixtures in data/03_sas_data_products/
 * into JSON fixtures under ui/src/mocks/ so the console can run without a
 * live API or Teradata connection.
 *
 * Usage: npm run fixtures [-- --limit 50]
 */
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, '..', '..');
const srcDir = resolve(repoRoot, 'data', '03_sas_data_products');
const outDir = resolve(here, '..', 'src', 'mocks');

const limitArg = process.argv.indexOf('--limit');
const limit = limitArg > -1 ? Number(process.argv[limitArg + 1]) : 60;

const FILES = {
  'customer_master_profile.csv': 'customerMasterProfile.json',
  'customer_segments.csv': 'customerSegments.json',
  'customer_risk_scores.csv': 'customerRiskScores.json',
  'transaction_analytics.csv': 'transactionAnalytics.json',
};

/** Minimal RFC4180-ish CSV line splitter (handles quoted fields with commas). */
function splitLine(line) {
  const out = [];
  let field = '';
  let quoted = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (quoted) {
      if (ch === '"' && line[i + 1] === '"') {
        field += '"';
        i += 1;
      } else if (ch === '"') {
        quoted = false;
      } else {
        field += ch;
      }
    } else if (ch === '"') {
      quoted = true;
    } else if (ch === ',') {
      out.push(field);
      field = '';
    } else {
      field += ch;
    }
  }
  out.push(field);
  return out;
}

function toCamel(header) {
  return header.toLowerCase().replace(/_([a-z0-9])/g, (_, c) => c.toUpperCase());
}

const NUMERIC = /^-?\d+(\.\d+)?([eE][-+]?\d+)?$/;

function coerce(value) {
  if (value === '') return null;
  if (NUMERIC.test(value)) return Number(value);
  return value;
}

function parseCsv(text) {
  const lines = text.split(/\r?\n/).filter((l) => l.trim() !== '');
  const headers = splitLine(lines[0]).map(toCamel);
  return lines.slice(1).map((line) => {
    const cells = splitLine(line);
    const row = {};
    headers.forEach((h, i) => {
      row[h] = coerce(cells[i] ?? '');
    });
    return row;
  });
}

mkdirSync(outDir, { recursive: true });

for (const [csvName, jsonName] of Object.entries(FILES)) {
  const rows = parseCsv(readFileSync(resolve(srcDir, csvName), 'utf8'));
  const sliced = Number.isFinite(limit) && limit > 0 ? rows.slice(0, limit) : rows;
  writeFileSync(resolve(outDir, jsonName), `${JSON.stringify(sliced, null, 2)}\n`);
  console.log(`${csvName} -> src/mocks/${jsonName} (${sliced.length} of ${rows.length} rows)`);
}
