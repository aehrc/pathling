/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

import { escapeSearchParameter } from "./search";

describe("escapeSearchParameter", () => {
  it("should escape \\", () => {
    expect(escapeSearchParameter("something\\somewhere")).toBe(
      "something\\\\somewhere"
    );
  });

  it("should escape $", () => {
    expect(escapeSearchParameter("something$somewhere")).toBe(
      "something\\$somewhere"
    );
  });

  it("should escape ,", () => {
    expect(escapeSearchParameter("something,somewhere")).toBe(
      "something\\,somewhere"
    );
  });

  it("should escape |", () => {
    expect(escapeSearchParameter("something|somewhere")).toBe(
      "something\\|somewhere"
    );
  });
});
