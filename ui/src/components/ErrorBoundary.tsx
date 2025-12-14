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
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError(): State {
    return { hasError: true };
  }

  componentDidCatch(error: Error): void {
    const showToast = getGlobalShowToast();
    if (showToast) {
      showToast("Something went wrong", error.message);
    }
  }

  render(): ReactNode {
    if (this.state.hasError) {
      // Reset error state to allow recovery on next render.
      this.setState({ hasError: false });
    }
    return this.props.children;
  }
}
