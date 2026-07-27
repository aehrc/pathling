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
 * The shared presentation for every error callout in the admin UI. Always red,
 * always carrying the warning icon, and always announced as an alert, so that
 * errors look and behave the same wherever they appear.
 *
 * @author John Grimes
 */

import { ExclamationTriangleIcon } from "@radix-ui/react-icons";
import { Callout, Flex, Text } from "@radix-ui/themes";

import type { ReactNode } from "react";

interface ErrorCalloutProps {
  /** The error message shown to the user. */
  message: string;
  /** An optional heading rendered in bold above the message. */
  title?: string;
  /** The callout size; defaults to the Radix default when omitted. */
  size?: "1" | "2" | "3";
  /** An optional top margin, for callouts that follow a form control. */
  mt?: "1" | "2" | "3" | "4";
  /** Optional recovery actions rendered below the message. */
  children?: ReactNode;
}

/**
 * Displays an error message in the shared callout presentation.
 *
 * @param props - The component props.
 * @param props.message - The error message shown to the user.
 * @param props.title - An optional heading rendered in bold above the message.
 * @param props.size - The callout size, when the default is not wanted.
 * @param props.mt - An optional top margin.
 * @param props.children - Optional recovery actions rendered below the message.
 * @returns The error callout component.
 * @example
 * <ErrorCallout message="Could not load jobs.">
 *   <Button onClick={onRetry}>Retry</Button>
 * </ErrorCallout>
 */
export function ErrorCallout({ message, title, size, mt, children }: Readonly<ErrorCalloutProps>) {
  return (
    <Callout.Root color="red" role="alert" size={size} mt={mt}>
      <Callout.Icon>
        <ExclamationTriangleIcon />
      </Callout.Icon>
      <Flex direction="column" gap="2" align="start">
        {title && (
          <Callout.Text>
            <Text weight="bold">{title}</Text>
          </Callout.Text>
        )}
        <Callout.Text>{message}</Callout.Text>
        {children}
      </Flex>
    </Callout.Root>
  );
}
