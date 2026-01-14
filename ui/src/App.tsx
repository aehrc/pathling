/**
 * Main application component with routing.
 *
 * @author John Grimes
 */

import { Routes, Route } from "react-router";

import { Layout } from "./components/layout/Layout";
import { BulkSubmit } from "./pages/BulkSubmit";
import { Callback } from "./pages/Callback";
import { Dashboard } from "./pages/Dashboard";
import { Export } from "./pages/Export";
import { Import } from "./pages/Import";
import { Resources } from "./pages/Resources";
import { SqlOnFhir } from "./pages/SqlOnFhir";

/**
 *
 */
export default function App() {
  return (
    <Routes>
      <Route path="/callback" element={<Callback />} />
      <Route element={<Layout />}>
        <Route path="/" element={<Dashboard />} />
        <Route path="/export" element={<Export />} />
        <Route path="/import" element={<Import />} />
        <Route path="/bulk-submit" element={<BulkSubmit />} />
        <Route path="/resources" element={<Resources />} />
        <Route path="/sql-on-fhir" element={<SqlOnFhir />} />
      </Route>
    </Routes>
  );
}
