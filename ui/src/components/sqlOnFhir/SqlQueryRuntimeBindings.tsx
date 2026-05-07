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

import { Box, Code, Flex, Switch, Text, TextField } from "@radix-ui/themes";

import { FieldGuidance } from "../FieldGuidance";
import { isRuntimeValueValid } from "./sqlQueryFormHelpers";

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
 * Renders one input per declared parameter, typed against the parameter's
 * declared FHIR primitive type.
 *
 * If no parameters are declared, a short hint is shown instead.
 *
 * @param props - The component props.
 * @param props.parameters
 * @param props.bindings
 * @param props.onChange
 * @param props.disabled
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
      {parameters.map((param) => {
        const value = bindings[param.name] ?? "";
        const valid = value === "" || isRuntimeValueValid(value, param.type);
        return (
          <Flex key={param.name} align="start" gap="3" wrap="wrap">
            <Box style={{ width: "10rem", paddingTop: "0.4rem" }}>
              <Code size="2">{param.name}</Code>
              <Text size="1" color="gray" as="div">
                {param.type}
              </Text>
            </Box>
            <Box style={{ flex: 1, minWidth: "12rem" }}>
              {param.type === "boolean" ? (
                <Flex align="center" gap="2" pt="2">
                  <Switch
                    checked={value === "true"}
                    onCheckedChange={(checked) => onChange(param.name, checked ? "true" : "false")}
                    disabled={disabled}
                    aria-label={`Runtime value for ${param.name}`}
                  />
                  <Text size="2" color="gray">
                    {value === "true" ? "true" : "false"}
                  </Text>
                </Flex>
              ) : (
                <TextField.Root
                  value={value}
                  placeholder={placeholderForType(param.type)}
                  onChange={(e) => onChange(param.name, e.target.value)}
                  disabled={disabled}
                  aria-label={`Runtime value for ${param.name}`}
                  color={valid ? undefined : "red"}
                />
              )}
              {!valid && (
                <Text size="1" color="red" as="div" mt="1">
                  Expected a {describeType(param.type)} value.
                </Text>
              )}
            </Box>
          </Flex>
        );
      })}
    </Flex>
  );
}

/**
 * Returns placeholder text appropriate for a parameter type.
 * @param type
 */
function placeholderForType(type: SqlQueryParameterType): string {
  switch (type) {
    case "string":
    case "code":
      return "";
    case "integer":
      return "e.g. 42";
    case "decimal":
      return "e.g. 1.5";
    case "boolean":
      return "";
    case "date":
      return "YYYY-MM-DD";
    case "dateTime":
      return "YYYY-MM-DDTHH:MM:SSZ";
  }
}

/**
 * Returns a human-readable label used in error messages.
 * @param type
 */
function describeType(type: SqlQueryParameterType): string {
  switch (type) {
    case "integer":
      return "integer";
    case "decimal":
      return "decimal";
    case "boolean":
      return "boolean";
    case "date":
      return "ISO 8601 date (YYYY-MM-DD)";
    case "dateTime":
      return "ISO 8601 dateTime";
    case "string":
    case "code":
      return "string";
  }
}
