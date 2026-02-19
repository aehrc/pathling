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
 * Reusable field label component for rendering consistent form field labels.
 *
 * @author John Grimes
 */

import { Text } from "@radix-ui/themes";

import type { ReactNode } from "react";

interface FieldLabelProps {
  /** The label text to display. */
  children: ReactNode;
  /** Whether to append an "(optional)" suffix. */
  optional?: boolean;
  /** Bottom margin spacing. Defaults to "1". */
  mb?: "0" | "1" | "2";
}

/**
 * Renders a form field label with standard typography.
 *
 * @param props - The component props.
 * @param props.children - The label text to display.
 * @param props.optional - Whether to append an "(optional)" suffix.
 * @param props.mb - Bottom margin spacing. Defaults to "1".
 * @returns The field label element.
 */
export function FieldLabel({ children, optional, mb = "1" }: Readonly<FieldLabelProps>) {
  return (
    <Text as="label" size="2" weight="medium" mb={mb} style={{ display: "block" }}>
      {children}
      {optional && (
        <>
          {" "}
          <Text size="1" color="gray">
            (optional)
          </Text>
        </>
      )}
    </Text>
  );
}
