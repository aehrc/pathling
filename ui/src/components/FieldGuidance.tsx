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
 * Reusable field guidance component for rendering helper text below form
 * fields.
 *
 * @author John Grimes
 */

import { Text } from "@radix-ui/themes";

import type { ReactNode } from "react";

interface FieldGuidanceProps {
  /** The guidance text to display. */
  children: ReactNode;
  /** Top margin spacing. Defaults to "1". */
  mt?: "0" | "1" | "2";
}

/**
 * Renders small, grey helper text below a form field.
 *
 * @param props - The component props.
 * @param props.children - The guidance text to display.
 * @param props.mt - Top margin spacing. Defaults to "1".
 * @returns The field guidance text element.
 */
export function FieldGuidance({ children, mt = "1" }: Readonly<FieldGuidanceProps>) {
  return (
    <Text size="1" color="gray" mt={mt}>
      {children}
    </Text>
  );
}
