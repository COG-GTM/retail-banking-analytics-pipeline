import type { ServiceDefinition } from '../api/posService';
import { useTransactionStore } from '../store/transactionStore';
import { formatCurrency } from '../utils/format';

interface ServiceButtonsProps {
  services: ServiceDefinition[];
  onConvertQuote: () => void;
}

export function ServiceButtons({ services, onConvertQuote }: ServiceButtonsProps) {
  const addServiceLine = useTransactionStore((state) => state.addServiceLine);
  const addVolumeDiscount = useTransactionStore((state) => state.addVolumeDiscount);

  return (
    <section className="panel services">
      <h2>Non-scan &amp; service items</h2>
      <div className="service-grid">
        {services.map((service) => (
          <button
            key={service.code}
            type="button"
            className="btn btn-service"
            onClick={() => addServiceLine(service)}
          >
            <span className="service-label">{service.label}</span>
            <span className="service-price">{formatCurrency(service.unitPrice)}</span>
          </button>
        ))}
      </div>
      <div className="service-actions">
        <button type="button" className="btn btn-secondary" onClick={addVolumeDiscount}>
          Apply volume pricing
        </button>
        <button type="button" className="btn btn-secondary" onClick={onConvertQuote}>
          Convert quote
        </button>
      </div>
    </section>
  );
}
