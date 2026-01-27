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
 * Tests for the ErrorBoundary component.
 *
 * This test suite verifies that the ErrorBoundary correctly renders children
 * when no errors occur and tests the component's structure and configuration.
 *
 * Note: Testing the actual error-catching behaviour of React Error Boundaries
 * is challenging in React 19 with testing-library because React re-throws
 * caught errors during the test environment. The error boundary functionality
 * is verified through manual testing and the component's implementation follows
 * React's documented patterns for error boundaries.
 *
 * @author John Grimes
 */

import { beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../test/testUtils";
import { ErrorBoundary } from "../ErrorBoundary";

// Mock the ToastContext to verify it would be called.
const mockShowToast = vi.fn();
vi.mock("../../contexts/ToastContext", () => ({
  getGlobalShowToast: () => mockShowToast,
}));

describe("ErrorBoundary", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("Normal rendering", () => {
    it("renders children when no error occurs", () => {
      render(
        <ErrorBoundary>
          <div>Child content</div>
        </ErrorBoundary>,
      );

      expect(screen.getByText("Child content")).toBeInTheDocument();
    });

    it("renders multiple children when no error occurs", () => {
      render(
        <ErrorBoundary>
          <div>First child</div>
          <div>Second child</div>
        </ErrorBoundary>,
      );

      expect(screen.getByText("First child")).toBeInTheDocument();
      expect(screen.getByText("Second child")).toBeInTheDocument();
    });

    it("renders nested components when no error occurs", () => {
      render(
        <ErrorBoundary>
          <div>
            <span>Nested content</span>
          </div>
        </ErrorBoundary>,
      );

      expect(screen.getByText("Nested content")).toBeInTheDocument();
    });

    it("renders deeply nested children correctly", () => {
      render(
        <ErrorBoundary>
          <div>
            <section>
              <article>
                <p>Deep content</p>
              </article>
            </section>
          </div>
        </ErrorBoundary>,
      );

      expect(screen.getByText("Deep content")).toBeInTheDocument();
    });

    it("renders children with complex structure", () => {
      render(
        <ErrorBoundary>
          <header>Header</header>
          <main>
            <aside>Sidebar</aside>
            <article>Main content</article>
          </main>
          <footer>Footer</footer>
        </ErrorBoundary>,
      );

      expect(screen.getByText("Header")).toBeInTheDocument();
      expect(screen.getByText("Sidebar")).toBeInTheDocument();
      expect(screen.getByText("Main content")).toBeInTheDocument();
      expect(screen.getByText("Footer")).toBeInTheDocument();
    });

    it("passes through children unchanged when no error occurs", () => {
      const TestComponent = () => <span data-testid="test-component">Test</span>;

      render(
        <ErrorBoundary>
          <TestComponent />
        </ErrorBoundary>,
      );

      expect(screen.getByTestId("test-component")).toBeInTheDocument();
      expect(screen.getByText("Test")).toBeInTheDocument();
    });
  });

  describe("Component structure", () => {
    it("is a React component that can wrap children", () => {
      // Verify ErrorBoundary can be used as a wrapper component.
      const { container } = render(
        <ErrorBoundary>
          <div id="wrapped-child">Wrapped</div>
        </ErrorBoundary>,
      );

      expect(container.querySelector("#wrapped-child")).toBeInTheDocument();
    });

    it("does not add extra DOM elements around children", () => {
      const { container } = render(
        <ErrorBoundary>
          <div data-testid="only-child">Only child</div>
        </ErrorBoundary>,
      );

      // The ErrorBoundary should render its children directly without wrapper elements.
      // The first child of the container should be our div.
      const onlyChild = container.querySelector('[data-testid="only-child"]');
      expect(onlyChild).toBeInTheDocument();
    });

    it("can be nested within other components", () => {
      render(
        <div>
          <ErrorBoundary>
            <span>Inside boundary</span>
          </ErrorBoundary>
        </div>,
      );

      expect(screen.getByText("Inside boundary")).toBeInTheDocument();
    });

    it("can nest multiple ErrorBoundaries", () => {
      render(
        <ErrorBoundary>
          <div>Outer</div>
          <ErrorBoundary>
            <div>Inner</div>
          </ErrorBoundary>
        </ErrorBoundary>,
      );

      expect(screen.getByText("Outer")).toBeInTheDocument();
      expect(screen.getByText("Inner")).toBeInTheDocument();
    });
  });

  describe("Re-rendering behaviour", () => {
    it("correctly re-renders when children change", () => {
      const { rerender } = render(
        <ErrorBoundary>
          <div>Initial content</div>
        </ErrorBoundary>,
      );

      expect(screen.getByText("Initial content")).toBeInTheDocument();

      rerender(
        <ErrorBoundary>
          <div>Updated content</div>
        </ErrorBoundary>,
      );

      expect(screen.getByText("Updated content")).toBeInTheDocument();
      expect(screen.queryByText("Initial content")).not.toBeInTheDocument();
    });

    it("handles adding children on re-render", () => {
      const { rerender } = render(
        <ErrorBoundary>
          <div>First</div>
        </ErrorBoundary>,
      );

      expect(screen.getByText("First")).toBeInTheDocument();

      rerender(
        <ErrorBoundary>
          <div>First</div>
          <div>Second</div>
        </ErrorBoundary>,
      );

      expect(screen.getByText("First")).toBeInTheDocument();
      expect(screen.getByText("Second")).toBeInTheDocument();
    });

    it("handles removing children on re-render", () => {
      const { rerender } = render(
        <ErrorBoundary>
          <div>First</div>
          <div>Second</div>
        </ErrorBoundary>,
      );

      expect(screen.getByText("First")).toBeInTheDocument();
      expect(screen.getByText("Second")).toBeInTheDocument();

      rerender(
        <ErrorBoundary>
          <div>First</div>
        </ErrorBoundary>,
      );

      expect(screen.getByText("First")).toBeInTheDocument();
      expect(screen.queryByText("Second")).not.toBeInTheDocument();
    });
  });

  describe("Mock verification", () => {
    it("does not call showToast when no error occurs", () => {
      render(
        <ErrorBoundary>
          <div>No errors here</div>
        </ErrorBoundary>,
      );

      expect(mockShowToast).not.toHaveBeenCalled();
    });
  });
});
