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
