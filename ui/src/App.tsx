import { Navigate, Route, Routes } from 'react-router-dom';
import { OperatorConsole } from './pages/OperatorConsole';
import { ProfilePanel } from './pages/ProfilePanel';
import { SegmentPanel } from './pages/SegmentPanel';
import { RiskPanel } from './pages/RiskPanel';
import { TransactionsPanel } from './pages/TransactionsPanel';

export function App() {
  return (
    <Routes>
      <Route path="/" element={<OperatorConsole />}>
        <Route index element={<Navigate to="/profile" replace />} />
        <Route path="profile" element={<ProfilePanel />} />
        <Route path="segment" element={<SegmentPanel />} />
        <Route path="risk" element={<RiskPanel />} />
        <Route path="transactions" element={<TransactionsPanel />} />
        <Route path="*" element={<Navigate to="/profile" replace />} />
      </Route>
    </Routes>
  );
}
