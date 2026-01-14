/**
 * React Error Boundary that catches render errors and shows toast notifications.
 * Allows the app to recover gracefully from component crashes.
 *
 * @author John Grimes
 */

import { Component, type ReactNode } from "react";

import { getGlobalShowToast } from "../contexts/ToastContext";

interface Props {
  children: ReactNode;
}

interface State {
  hasError: boolean;
}

export class ErrorBoundary extends Component<Props, State> {
  /**
   * Creates a new ErrorBoundary instance.
   *
   * @param props - The component props.
   */
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false };
  }

  /**
   * Updates state when an error is caught.
   *
   * @returns The new state with hasError set to true.
   */
  static getDerivedStateFromError(): State {
    return { hasError: true };
  }

  /**
   * Handles caught errors by showing a toast notification.
   *
   * @param error - The error that was caught.
   */
  componentDidCatch(error: Error): void {
    const showToast = getGlobalShowToast();
    if (showToast) {
      showToast("Something went wrong", error.message);
    }
  }

  /**
   * Renders the children or resets on error.
   *
   * @returns The children components.
   */
  render(): ReactNode {
    if (this.state.hasError) {
      // Reset error state to allow recovery on next render.
      this.setState({ hasError: false });
    }
    return this.props.children;
  }
}
