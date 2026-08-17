import type {
  Employee,
  JobAccount,
  LineType,
  Product,
  Quote,
  Register,
  Store,
  UnitOfMeasure,
} from '../types/pos';

export const store: Store = {
  storeId: '6631',
  storeName: 'San Francisco #6631',
  taxRate: 0.08625,
  addressLine1: '2 Colma Blvd',
  city: 'Colma',
  state: 'CA',
  postalCode: '94014',
};

export const register: Register = {
  registerId: '12',
  storeId: '6631',
  registerType: 'FRONT END',
};

export const employee: Employee = {
  employeeId: '84021',
  storeId: '6631',
  displayName: 'M. Reyes',
  role: 'CASHIER',
};

export const jobAccount: JobAccount = {
  jobAccountId: 'PX-448120',
  customerId: 'CUST-90311',
  programName: 'Pro Xtra',
  companyName: 'Bay Area Renovations LLC',
  poNumber: 'PO-24TH-118',
  jobName: '24th St remodel',
  taxExempt: false,
  taxExemptCertificate: 'CA-EX-77120',
};

export const products: Product[] = [
  {
    sku: '1000412774',
    productDescription: 'BEHR PREMIUM PLUS Interior Eggshell — Ultra Pure White',
    unitOfMeasure: 'GAL',
    unitPrice: 34.98,
    aisle: 'Aisle 30',
    bay: '004',
    taxable: true,
    warrantyEligible: false,
  },
  {
    sku: '2049183306',
    productDescription: '2 in. x 4 in. x 96 in. Premium Kiln-Dried Whitewood Stud',
    unitOfMeasure: 'EA',
    unitPrice: 4.28,
    aisle: 'Aisle 12',
    bay: '018',
    taxable: true,
    warrantyEligible: false,
  },
  {
    sku: '3067741188',
    productDescription: 'Quikrete 60 lb. Concrete Mix',
    unitOfMeasure: 'BAG',
    unitPrice: 6.48,
    aisle: 'Aisle 07',
    bay: '002',
    taxable: true,
    warrantyEligible: false,
  },
  {
    sku: '4118820055',
    productDescription: 'DEWALT 20V MAX XR Hammer Drill Kit',
    unitOfMeasure: 'EA',
    unitPrice: 279.0,
    aisle: 'Aisle 21',
    bay: '011',
    taxable: true,
    warrantyEligible: true,
    warrantyPrice: 34.97,
  },
  {
    sku: '5220913744',
    productDescription: 'Grip-Rite #9 x 3 in. Exterior Screws (5 lb.)',
    unitOfMeasure: 'BOX',
    unitPrice: 42.97,
    aisle: 'Aisle 14',
    bay: '007',
    taxable: true,
    warrantyEligible: false,
  },
  {
    sku: '6390022471',
    productDescription: 'Owens Corning R-13 Kraft Faced Insulation Batt',
    unitOfMeasure: 'EA',
    unitPrice: 58.42,
    aisle: 'Aisle 26',
    bay: '021',
    taxable: true,
    warrantyEligible: false,
  },
];

export interface ServiceDefinition {
  code: string;
  label: string;
  lineType: LineType;
  description: string;
  unitOfMeasure: UnitOfMeasure;
  unitPrice: number;
  taxable: boolean;
}

export const services: ServiceDefinition[] = [
  {
    code: 'SVC-CUT',
    label: 'Lumber cut charge',
    lineType: 'LUMBER_CUT',
    description: 'Lumber cut charge (per cut)',
    unitOfMeasure: 'EA',
    unitPrice: 0.75,
    taxable: true,
  },
  {
    code: 'SVC-MIX',
    label: 'Paint mix',
    lineType: 'PAINT_MIX',
    description: 'Custom paint tint / mix',
    unitOfMeasure: 'EA',
    unitPrice: 4.5,
    taxable: true,
  },
  {
    code: 'SVC-RENT',
    label: 'Tool rental',
    lineType: 'TOOL_RENTAL',
    description: 'Tool rental — 4 hour block',
    unitOfMeasure: 'EA',
    unitPrice: 48.0,
    taxable: true,
  },
  {
    code: 'SVC-SPO',
    label: 'Special order',
    lineType: 'SPECIAL_ORDER',
    description: 'Special order handling fee',
    unitOfMeasure: 'EA',
    unitPrice: 25.0,
    taxable: true,
  },
  {
    code: 'SVC-WC',
    label: 'Will call',
    lineType: 'WILL_CALL',
    description: 'Will call / pro desk pickup',
    unitOfMeasure: 'EA',
    unitPrice: 0,
    taxable: false,
  },
  {
    code: 'SVC-DEL',
    label: 'Delivery',
    lineType: 'DELIVERY',
    description: 'Local flatbed delivery',
    unitOfMeasure: 'EA',
    unitPrice: 79.0,
    taxable: true,
  },
];

export const quotes: Quote[] = [
  {
    quoteId: 'Q-6631-20418',
    storeId: '6631',
    jobAccountId: 'PX-448120',
    createdDate: '2026-08-10',
    expiresDate: '2026-09-09',
    description: '24th St remodel — framing package',
    lines: [
      {
        lineNumber: 1,
        lineType: 'MERCHANDISE',
        sku: '2049183306',
        description: '2 in. x 4 in. x 96 in. Premium Kiln-Dried Whitewood Stud',
        quantity: 48,
        unitOfMeasure: 'EA',
        unitPrice: 4.28,
        extendedAmount: 205.44,
        taxable: true,
      },
      {
        lineNumber: 2,
        lineType: 'MERCHANDISE',
        sku: '5220913744',
        description: 'Grip-Rite #9 x 3 in. Exterior Screws (5 lb.)',
        quantity: 2,
        unitOfMeasure: 'BOX',
        unitPrice: 42.97,
        extendedAmount: 85.94,
        taxable: true,
      },
      {
        lineNumber: 3,
        lineType: 'DELIVERY',
        sku: null,
        description: 'Local flatbed delivery',
        quantity: 1,
        unitOfMeasure: 'EA',
        unitPrice: 79.0,
        extendedAmount: 79.0,
        taxable: true,
      },
    ],
  },
  {
    quoteId: 'Q-6631-20502',
    storeId: '6631',
    jobAccountId: 'PX-448120',
    createdDate: '2026-08-14',
    expiresDate: '2026-09-13',
    description: '24th St remodel — paint package',
    lines: [
      {
        lineNumber: 1,
        lineType: 'MERCHANDISE',
        sku: '1000412774',
        description: 'BEHR PREMIUM PLUS Interior Eggshell — Ultra Pure White',
        quantity: 12,
        unitOfMeasure: 'GAL',
        unitPrice: 34.98,
        extendedAmount: 419.76,
        taxable: true,
      },
      {
        lineNumber: 2,
        lineType: 'PAINT_MIX',
        sku: null,
        description: 'Custom paint tint / mix',
        quantity: 12,
        unitOfMeasure: 'EA',
        unitPrice: 4.5,
        extendedAmount: 54.0,
        taxable: true,
      },
    ],
  },
];
