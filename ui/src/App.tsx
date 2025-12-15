/**
 * Main application component with routing.
 *
 * @author John Grimes
 */

import { Routes, Route } from "react-router";
import { Layout } from "./components/layout/Layout";
import { Dashboard } from "./pages/Dashboard";
import { Export } from "./pages/Export";
import { Import } from "./pages/Import";
import { Callback } from "./pages/Callback";

export default function App() {
  return (
    <Routes>
      <Route path="/callback" element={<Callback />} />
      <Route element={<Layout />}>
        <Route path="/" element={<Dashboard />} />
        <Route path="/export" element={<Export />} />
        <Route path="/import" element={<Import />} />
      </Route>
    </Routes>
  );
}
