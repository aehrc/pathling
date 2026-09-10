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
 * Tests for the ParameterValueInput component.
 *
 * Verifies the shared type-aware value control used by both the inline
 * parameter rows and the stored query's parameters section: a switch for
 * boolean parameters and a validated text field otherwise, the expected-type
 * message on an unparseable value, the required marking on an empty value, and
 * the raw string reported to the change handler.
 *
 * @author John Grimes
 */

import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { ParameterValueInput } from "../ParameterValueInput";

const onChange = vi.fn();

describe("ParameterValueInput", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // A boolean parameter has two states and no unbound state, so it is
  // presented as a switch rather than a text field.
  it("renders a switch for a boolean parameter", () => {
    render(
      <ParameterValueInput
        type="boolean"
        value="true"
        onChange={onChange}
        ariaLabel="Runtime value for active"
      />,
    );

    const control = screen.getByRole("switch", {
      name: "Runtime value for active",
    });
    expect(control).toBeChecked();
    expect(screen.queryByRole("textbox", { name: "Runtime value for active" })).toBeNull();
  });

  // Every other type is entered as free text, validated as the user types.
  it("renders a text field for a non-boolean parameter", () => {
    render(
      <ParameterValueInput
        type="date"
        value="2025-06-30"
        onChange={onChange}
        ariaLabel="Runtime value for period_end"
      />,
    );

    const control = screen.getByRole("textbox", {
      name: "Runtime value for period_end",
    });
    expect(control).toHaveValue("2025-06-30");
    expect(control).toHaveAttribute("placeholder", "YYYY-MM-DD");
  });

  // A value that does not parse as its declared type is marked with a message
  // naming the expected form, so the user can correct it.
  it("shows the expected-type message for an unparseable value", () => {
    render(
      <ParameterValueInput
        type="integer"
        value="abc"
        onChange={onChange}
        ariaLabel="Runtime value for count"
      />,
    );

    expect(screen.getByText("Expected a integer value.")).toBeInTheDocument();
  });

  // The message names the ISO 8601 form for date types, which is not simply
  // the type code.
  it("names the ISO 8601 form for an invalid date", () => {
    render(
      <ParameterValueInput
        type="date"
        value="30 June"
        onChange={onChange}
        ariaLabel="Runtime value for period_end"
      />,
    );

    expect(screen.getByText("Expected a ISO 8601 date (YYYY-MM-DD) value.")).toBeInTheDocument();
  });

  // The placeholder is the user's only cue to the form a value must take, so
  // each entered type carries its own example.
  it.each([
    ["integer" as const, "e.g. 42"],
    ["decimal" as const, "e.g. 1.5"],
    ["date" as const, "YYYY-MM-DD"],
    ["dateTime" as const, "YYYY-MM-DDTHH:MM:SSZ"],
    ["string" as const, ""],
    ["code" as const, ""],
  ])("places a %s example in the placeholder", (type, placeholder) => {
    render(
      <ParameterValueInput
        type={type}
        value=""
        onChange={onChange}
        ariaLabel="Runtime value for arg"
      />,
    );

    expect(screen.getByRole("textbox", { name: "Runtime value for arg" })).toHaveAttribute(
      "placeholder",
      placeholder,
    );
  });

  // The message names the expected form per type, so each type that can fail
  // validation reports its own wording.
  it.each([
    ["decimal" as const, "Expected a decimal value."],
    ["dateTime" as const, "Expected a ISO 8601 dateTime value."],
  ])("names the expected form for an invalid %s", (type, message) => {
    render(
      <ParameterValueInput
        type={type}
        value="nope"
        onChange={onChange}
        ariaLabel="Runtime value for arg"
      />,
    );

    expect(screen.getByText(message)).toBeInTheDocument();
  });

  // A valid value carries no message.
  it("shows no message for a valid value", () => {
    render(
      <ParameterValueInput
        type="integer"
        value="42"
        onChange={onChange}
        ariaLabel="Runtime value for count"
      />,
    );

    expect(screen.queryByText(/^Expected a/)).toBeNull();
  });

  // An empty value on a required parameter is the case that produces the
  // unbound-parameter server failure, so the input is marked.
  it("marks an empty required value as required", () => {
    render(
      <ParameterValueInput
        type="string"
        value=""
        onChange={onChange}
        required
        ariaLabel="Runtime value for patient_id"
      />,
    );

    expect(screen.getByRole("textbox", { name: "Runtime value for patient_id" })).toBeRequired();
  });

  // A required parameter that has a value is satisfied, and so is not marked.
  it("does not mark a required value that has been supplied", () => {
    render(
      <ParameterValueInput
        type="string"
        value="pat-1"
        onChange={onChange}
        required
        ariaLabel="Runtime value for patient_id"
      />,
    );

    expect(
      screen.getByRole("textbox", { name: "Runtime value for patient_id" }),
    ).not.toBeRequired();
  });

  // An empty value that is not required is not marked, which is the case for
  // an inline row that has not been named yet.
  it("does not mark an empty value that is not required", () => {
    render(
      <ParameterValueInput
        type="string"
        value=""
        onChange={onChange}
        ariaLabel="Value for parameter 1"
      />,
    );

    expect(screen.getByRole("textbox", { name: "Value for parameter 1" })).not.toBeRequired();
  });

  // The text field reports the raw string, leaving coercion to the request
  // assembly.
  it("reports the raw value typed into the text field", async () => {
    const user = userEvent.setup();
    render(
      <ParameterValueInput
        type="integer"
        value=""
        onChange={onChange}
        ariaLabel="Runtime value for count"
      />,
    );

    await user.type(screen.getByRole("textbox", { name: "Runtime value for count" }), "7");

    expect(onChange).toHaveBeenCalledWith("7");
  });

  // The switch reports the FHIR boolean literals, so the value is bound
  // whichever way the switch is thrown.
  it("reports true when the switch is turned on", async () => {
    const user = userEvent.setup();
    render(
      <ParameterValueInput
        type="boolean"
        value=""
        onChange={onChange}
        ariaLabel="Runtime value for active"
      />,
    );

    await user.click(screen.getByRole("switch", { name: "Runtime value for active" }));

    expect(onChange).toHaveBeenCalledWith("true");
  });

  it("reports false when the switch is turned off", async () => {
    const user = userEvent.setup();
    render(
      <ParameterValueInput
        type="boolean"
        value="true"
        onChange={onChange}
        ariaLabel="Runtime value for active"
      />,
    );

    await user.click(screen.getByRole("switch", { name: "Runtime value for active" }));

    expect(onChange).toHaveBeenCalledWith("false");
  });

  // A disabled control cannot be edited, which is how the form locks values
  // while a query is running.
  it("disables the control when disabled", () => {
    render(
      <ParameterValueInput
        type="string"
        value="pat-1"
        onChange={onChange}
        disabled
        ariaLabel="Runtime value for patient_id"
      />,
    );

    expect(screen.getByRole("textbox", { name: "Runtime value for patient_id" })).toBeDisabled();
  });
});
