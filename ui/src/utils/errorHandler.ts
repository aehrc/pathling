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
 * Global error handlers for uncaught JavaScript errors and unhandled Promise
 * rejections. Shows toast notifications when errors occur.
 *
 * @author John Grimes
 */

import { getGlobalShowToast } from "../contexts/ToastContext";

/**
 * Sets up global error handlers that show toast notifications for uncaught
 * errors. Should be called once after the ToastProvider has mounted.
 */
export function setupGlobalErrorHandlers(): void {
  // Handle uncaught JavaScript errors.
  window.addEventListener("error", (event) => {
    const showToast = getGlobalShowToast();
    if (showToast) {
      showToast("An unexpected error occurred", event.message);
    }
  });

  // Handle unhandled Promise rejections.
  window.addEventListener("unhandledrejection", (event) => {
    const showToast = getGlobalShowToast();
    if (showToast) {
      const message =
        event.reason instanceof Error
          ? event.reason.message
          : "An unhandled error occurred";
      showToast("An unexpected error occurred", message);
    }
  });
}
