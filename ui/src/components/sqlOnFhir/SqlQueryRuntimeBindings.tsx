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
 * Runtime parameter bindings for the SQL query form.
 *
 * @author John Grimes
 */

import { Box, Code, Flex, Text } from "@radix-ui/themes";

import { FieldGuidance } from "../FieldGuidance";
import { ParameterValueInput } from "./ParameterValueInput";

import type { SqlQueryParameterType, SqlQueryRuntimeBindings } from "../../types/sqlQuery";

interface SqlQueryRuntimeBindingsProps {
  /** Declared parameters for the active Library. */
  parameters: Array<{ name: string; type: SqlQueryParameterType }>;
  /** Current runtime bindings, keyed by parameter name. */
  bindings: SqlQueryRuntimeBindings;
  /** Callback fired when a binding changes. */
  onChange: (name: string, value: string) => void;
  /** Whether the inputs should be disabled. */
  disabled?: boolean;
}

/**
 * Renders one row per declared parameter, pairing the parameter's name and
 * declared type - read-only, since the stored Library defines them - with a
 * value input typed against that type.
 *
 * Every declared parameter must be bound to execute, so an empty value is
 * marked as required. If no parameters are declared, a short hint is shown
 * instead.
 *
 * @param props - The component props.
 * @param props.parameters - Declared parameters for the active Library.
 * @param props.bindings - Current runtime bindings, keyed by parameter name.
 * @param props.onChange - Callback fired when a binding changes.
 * @param props.disabled - Whether the inputs should be disabled.
 * @returns The runtime bindings panel.
 */
export function SqlQueryRuntimeBindings({
  parameters,
  bindings,
  onChange,
  disabled = false,
}: Readonly<SqlQueryRuntimeBindingsProps>) {
  if (parameters.length === 0) {
    return <FieldGuidance>This Library declares no runtime parameters.</FieldGuidance>;
  }

  return (
    <Flex direction="column" gap="2">
      {parameters.map((param) => (
        <Flex key={param.name} align="start" gap="3" wrap="wrap">
          <Box style={{ width: "10rem", paddingTop: "0.4rem" }}>
            <Code size="2">{param.name}</Code>
            <Text size="1" color="gray" as="div">
              {param.type}
            </Text>
          </Box>
          <Box style={{ flex: 1, minWidth: "12rem" }}>
            <ParameterValueInput
              type={param.type}
              value={bindings[param.name] ?? ""}
              onChange={(value) => onChange(param.name, value)}
              disabled={disabled}
              required
              ariaLabel={`Runtime value for ${param.name}`}
            />
          </Box>
        </Flex>
      ))}
    </Flex>
  );
}
