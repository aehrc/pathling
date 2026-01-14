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
 *
 */
export function getGlobalShowToast() {
  return globalShowToast;
}

/**
 *
 * @param root0
 * @param root0.children
 */
export function ToastProvider({ children }: { children: ReactNode }) {
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
 *
 */
export function useToast(): ToastContextValue {
  const context = use(ToastContext);
  if (!context) {
    throw new Error("useToast must be used within a ToastProvider");
  }
  return context;
}
