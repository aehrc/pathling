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
 * Hook for moving keyboard focus to a row the user has just added.
 *
 * @author John Grimes
 */

import { useCallback, useRef } from "react";

/** The functions returned by {@link useFocusOnAdd}. */
export interface FocusOnAdd<K> {
  /** Marks the row with the given key to take focus when it is rendered. */
  focusOnRender: (key: K) => void;
  /** Returns a ref for the first control of the row with the given key. */
  focusRef: (key: K) => (element: HTMLElement | null) => void;
}

/**
 * Moves keyboard focus to the first control of a newly added row. The add
 * handler marks the new row's key, and the row's first control takes focus
 * through its ref once it is rendered. Rows that were not marked, such as
 * those present when the form first renders, never take focus.
 *
 * @returns The functions for marking a row and attaching its first control.
 * @example
 * const { focusOnRender, focusRef } = useFocusOnAdd<string>();
 * const handleAdd = () => {
 *   const rowId = crypto.randomUUID();
 *   focusOnRender(rowId);
 *   setRows([...rows, { rowId, label: "" }]);
 * };
 * // In the row: <TextField.Root ref={focusRef(row.rowId)} ... />
 */
export function useFocusOnAdd<K>(): FocusOnAdd<K> {
  const pendingKey = useRef<K | null>(null);

  const focusOnRender = useCallback((key: K) => {
    pendingKey.current = key;
  }, []);

  const focusRef = useCallback(
    (key: K) => (element: HTMLElement | null) => {
      if (element && pendingKey.current === key) {
        pendingKey.current = null;
        element.focus();
      }
    },
    [],
  );

  return { focusOnRender, focusRef };
}
