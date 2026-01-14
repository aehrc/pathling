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
 * Context for managing toast notifications using Radix UI Toast.
 * Provides a global showToast function that can be called from anywhere.
 *
 * @author John Grimes
 */

import { Cross2Icon } from "@radix-ui/react-icons";
import * as Toast from "@radix-ui/react-toast";
import { IconButton, Text } from "@radix-ui/themes";
import {
  createContext,
  use,
  useState,
  useCallback,
  useEffect,
  useRef,
  type ReactNode,
} from "react";
import "./ToastContext.css";

interface ToastData {
  id: string;
  title: string;
  description?: string;
}

interface ToastContextValue {
  showToast: (title: string, description?: string) => void;
}

const ToastContext = createContext<ToastContextValue | null>(null);

// Module-level reference for error handlers to access showToast without hooks.
let globalShowToast: ((title: string, description?: string) => void) | null = null;

/**
 * Returns the global showToast function for use in non-React contexts.
 *
 * @returns The showToast function or null if provider not mounted.
 */
export function getGlobalShowToast() {
  return globalShowToast;
}

/**
 * Provider component for toast notifications.
 *
 * @param root0 - The component props.
 * @param root0.children - The child components to render.
 * @returns The provider component wrapping children.
 */
export function ToastProvider({ children }: Readonly<{ children: ReactNode }>) {
  const [toasts, setToasts] = useState<ToastData[]>([]);
  const toastIdCounter = useRef(0);

  const showToast = useCallback((title: string, description?: string) => {
    const id = `toast-${++toastIdCounter.current}`;
    setToasts((prev) => [...prev, { id, title, description }]);
  }, []);

  const removeToast = (id: string) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  };

  // Set up global reference for error handlers.
  useEffect(() => {
    globalShowToast = showToast;
    return () => {
      globalShowToast = null;
    };
  }, [showToast]);

  return (
    <ToastContext value={{ showToast }}>
      <Toast.Provider swipeDirection="right">
        {children}
        {toasts.map((toast) => (
          <Toast.Root
            key={toast.id}
            className="toast-root"
            onOpenChange={(open) => {
              if (!open) removeToast(toast.id);
            }}
          >
            <Toast.Title asChild>
              <Text weight="medium">{toast.title}</Text>
            </Toast.Title>
            {toast.description && (
              <Toast.Description asChild>
                <Text size="2" color="gray">
                  {toast.description}
                </Text>
              </Toast.Description>
            )}
            <Toast.Close asChild>
              <IconButton size="1" variant="ghost" color="gray" className="toast-close">
                <Cross2Icon />
              </IconButton>
            </Toast.Close>
          </Toast.Root>
        ))}
        <Toast.Viewport className="toast-viewport" />
      </Toast.Provider>
    </ToastContext>
  );
}

/**
 * Hook for accessing the toast context.
 *
 * @returns The toast context value.
 * @throws Error if used outside of a ToastProvider.
 */
export function useToast(): ToastContextValue {
  const context = use(ToastContext);
  if (!context) {
    throw new Error("useToast must be used within a ToastProvider");
  }
  return context;
}
