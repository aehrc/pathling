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
 * Page listing the caller's asynchronous jobs, backed directly by the server's $jobs operation with
 * automatic refresh and per-row cancellation.
 *
 * @author John Grimes
 */

import { CapabilityGuard } from "../components/auth/CapabilityGuard";
import { JobsContent } from "../components/jobs/JobsContent";

/**
 * Page component listing the caller's jobs with live refresh and cancellation.
 *
 * @returns The jobs page component.
 */
export function Jobs() {
  return <CapabilityGuard>{() => <JobsContent />}</CapabilityGuard>;
}
