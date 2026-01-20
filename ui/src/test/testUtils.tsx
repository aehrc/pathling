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
 * Test utilities for rendering React components with required providers.
 *
 * @author John Grimes
 */

import { Theme } from "@radix-ui/themes";
import { render } from "@testing-library/react";

import type { RenderOptions, RenderResult } from "@testing-library/react";
import type { ReactElement, ReactNode } from "react";

interface WrapperProps {
  children: ReactNode;
}

/**
 * Wrapper component that provides all necessary context providers for testing
 * Radix UI components.
 *
 * @param props - The wrapper props.
 * @param props.children - The children to render within the providers.
 * @returns The children wrapped in all required providers.
 */
function AllTheProviders({ children }: Readonly<WrapperProps>): ReactElement {
  return <Theme>{children}</Theme>;
}

/**
 * Custom render function that wraps components in required providers.
 *
 * @param ui - The React element to render.
 * @param options - Additional render options.
 * @returns The render result from @testing-library/react.
 */
function customRender(ui: ReactElement, options?: Omit<RenderOptions, "wrapper">): RenderResult {
  return render(ui, { wrapper: AllTheProviders, ...options });
}

// Re-export everything from @testing-library/react.
export * from "@testing-library/react";

// Override the render function with our custom one.
export { customRender as render };
