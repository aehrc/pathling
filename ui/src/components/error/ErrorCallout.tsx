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
import { Badge, Callout, Flex, Text } from "@radix-ui/themes";

import { severityColour, severityLabel } from "./errorPresentation";

import type { DisplayIssue } from "./errorPresentation";
import type { ReactNode } from "react";

interface ErrorCalloutBaseProps {
  /** An optional heading rendered in bold above the body. */
  title?: string;
  /** The callout size; defaults to the Radix default when omitted. */
  size?: "1" | "2" | "3";
  /** An optional top margin, for callouts that follow a form control. */
  mt?: "1" | "2" | "3" | "4";
  /** Optional recovery actions rendered below the body. */
  children?: ReactNode;
}

/**
 * The body is either a message or a list of issues, never both and never
 * neither, so that an empty or ambiguous callout cannot be expressed.
 */
type ErrorCalloutProps = ErrorCalloutBaseProps &
  ({ message: string; issues?: never } | { issues: DisplayIssue[]; message?: never });

/**
 * Displays an error in the shared callout presentation, either as a single
 * message or as a list of issues carrying their own severities.
 *
 * @param props - The component props.
 * @param props.message - The error message shown to the user, for the message form.
 * @param props.issues - The issues shown to the user, for the issues form.
 * @param props.title - An optional heading rendered in bold above the body.
 * @param props.size - The callout size, when the default is not wanted.
 * @param props.mt - An optional top margin.
 * @param props.children - Optional recovery actions rendered below the body.
 * @returns The error callout component.
 * @example
 * <ErrorCallout message="Could not load jobs.">
 *   <Button onClick={onRetry}>Retry</Button>
 * </ErrorCallout>
 * @example
 * <ErrorCallout issues={toDisplayIssues(error)} size="1" />
 */
export function ErrorCallout({
  message,
  issues,
  title,
  size,
  mt,
  children,
}: Readonly<ErrorCalloutProps>) {
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
        {issues ? (
          <Flex direction="column" gap="1">
            {issues.map((issue, index) => (
              // eslint-disable-next-line @eslint-react/no-array-index-key -- Derived issues have no stable identifier.
              <Flex key={`${issue.severity}-${index}`} align="start" gap="2">
                <Badge size="1" color={severityColour(issue.severity)}>
                  {severityLabel(issue.severity)}
                </Badge>
                <Text
                  size="2"
                  color={severityColour(issue.severity)}
                  style={{ overflowWrap: "anywhere" }}
                >
                  {issue.text}
                </Text>
              </Flex>
            ))}
          </Flex>
        ) : (
          <Callout.Text>{message}</Callout.Text>
        )}
        {children}
      </Flex>
    </Callout.Root>
  );
}
