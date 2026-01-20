/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * Main application component with route definitions.
 *
 * @returns The application component with routing.
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
