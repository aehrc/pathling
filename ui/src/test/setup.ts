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
 * Vitest setup file for testing React components.
 *
 * @author John Grimes
 */

import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach, vi } from "vitest";

// Clean up after each test to ensure test isolation.
afterEach(() => {
  cleanup();
});

// Mock window.matchMedia for jsdom environment.
Object.defineProperty(window, "matchMedia", {
  writable: true,
  value: vi.fn().mockImplementation((query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })),
});

// Mock ResizeObserver for Radix UI ScrollArea component.
class ResizeObserverMock {
  observe = vi.fn();
  unobserve = vi.fn();
  disconnect = vi.fn();
}

window.ResizeObserver = ResizeObserverMock;

// Mock PointerEvent for Radix UI Select and other pointer-based components.
// This is necessary because jsdom does not support PointerEvent by default.
class PointerEventMock extends Event {
  button: number;
  pointerType: string;
  ctrlKey: boolean;

  constructor(type: string, props?: PointerEventInit) {
    super(type, props);
    this.button = props?.button ?? 0;
    this.pointerType = props?.pointerType ?? "mouse";
    this.ctrlKey = props?.ctrlKey ?? false;
  }
}

window.PointerEvent = PointerEventMock as unknown as typeof PointerEvent;

// Mock Element.hasPointerCapture and related methods.
Element.prototype.hasPointerCapture = vi.fn().mockReturnValue(false);
Element.prototype.setPointerCapture = vi.fn();
Element.prototype.releasePointerCapture = vi.fn();

// Mock Element.scrollIntoView for Radix UI Select component.
Element.prototype.scrollIntoView = vi.fn();
