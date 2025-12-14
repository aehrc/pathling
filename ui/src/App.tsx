/**
 * Main application component with routing.
 *
 * @author John Grimes
 */

import { Routes, Route, Navigate } from "react-router";
import { useSettings } from "./contexts/SettingsContext";
import { Layout } from "./components/layout/Layout";
import { Dashboard } from "./pages/Dashboard";
import { Settings } from "./pages/Settings";
import { Callback } from "./pages/Callback";

export default function App() {
  const { fhirBaseUrl } = useSettings();

  return (
    <Routes>
      <Route path="/callback" element={<Callback />} />
      <Route element={<Layout />}>
        <Route
          path="/"
          element={
            fhirBaseUrl ? <Dashboard /> : <Navigate to="/settings" replace />
          }
        />
        <Route path="/settings" element={<Settings />} />
      </Route>
    </Routes>
  );
}
