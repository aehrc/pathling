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
