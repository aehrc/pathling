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
 * Shared type-aware value control for a declared SQL query parameter.
 *
 * @author John Grimes
 */

import { Flex, Switch, Text, TextField } from "@radix-ui/themes";

import { isRuntimeValueValid } from "./sqlQueryFormHelpers";

import type { SqlQueryParameterType } from "../../types/sqlQuery";

interface ParameterValueInputProps {
  /** The declared type the value is validated against. */
  type: SqlQueryParameterType;
  /** The current value, as a string. */
  value: string;
  /** Callback fired with the new raw value. */
  onChange: (value: string) => void;
  /** Whether the control should be disabled. */
  disabled?: boolean;
  /** Whether an empty value should be marked as required. */
  required?: boolean;
  /** Accessible name for the control. */
  ariaLabel: string;
}

/**
 * Renders the value control for one declared parameter, typed against the
 * parameter's declared FHIR primitive type.
 *
 * A boolean parameter is a switch: it has two states and no unbound state, so
 * an empty value reads as false. Every other type is a text field validated as
 * the user types, marked with a message naming the expected form when the value
 * does not parse. An empty value on a required parameter is marked as required,
 * because an unbound parameter cannot be submitted.
 *
 * The raw string is reported to `onChange`; coercion to the declared type
 * happens when the request is assembled.
 *
 * @param props - The component props.
 * @param props.type - The declared type the value is validated against.
 * @param props.value - The current value, as a string.
 * @param props.onChange - Callback fired with the new raw value.
 * @param props.disabled - Whether the control should be disabled.
 * @param props.required - Whether an empty value should be marked as required.
 * @param props.ariaLabel - Accessible name for the control.
 * @returns The value control.
 */
export function ParameterValueInput({
  type,
  value,
  onChange,
  disabled = false,
  required = false,
  ariaLabel,
}: Readonly<ParameterValueInputProps>) {
  if (type === "boolean") {
    return (
      <Flex align="center" gap="2" pt="2">
        <Switch
          checked={value === "true"}
          onCheckedChange={(checked) => onChange(checked ? "true" : "false")}
          disabled={disabled}
          aria-label={ariaLabel}
        />
        <Text size="2" color="gray">
          {value === "true" ? "true" : "false"}
        </Text>
      </Flex>
    );
  }

  const valid = value === "" || isRuntimeValueValid(value, type);
  const missing = required && value === "";
  return (
    <>
      <TextField.Root
        value={value}
        placeholder={placeholderForType(type)}
        onChange={(e) => onChange(e.target.value)}
        disabled={disabled}
        required={missing}
        aria-label={ariaLabel}
        color={valid && !missing ? undefined : "red"}
      />
      {!valid && (
        <Text size="1" color="red" as="div" mt="1">
          Expected a {describeType(type)} value.
        </Text>
      )}
    </>
  );
}

/**
 * Returns placeholder text appropriate for a parameter type.
 *
 * @param type - The declared parameter type.
 * @returns A short example string used as the input's placeholder.
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
 *
 * @param type - The declared parameter type.
 * @returns A short label naming the expected value form.
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
