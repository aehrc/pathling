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

import { describe, expect, it } from "vitest";

import { decodeSql } from "../../../utils/sqlBase64";
import {
  areRuntimeBindingsValid,
  buildInlineSqlQueryLibrary,
  buildParameterTypes,
  canExecuteInlineForm,
  canSaveInlineForm,
  decodeViewReferenceValue,
  encodeViewReferenceValue,
  isRuntimeValueValid,
} from "../sqlQueryFormHelpers";

describe("encodeViewReferenceValue / decodeViewReferenceValue", () => {
  // The codec round-trips a view-definition reference.
  it("round-trips a view-definition reference", () => {
    const value = encodeViewReferenceValue("view-definition", "vd-1");
    expect(value).toBe("view-definition:vd-1");
    expect(decodeViewReferenceValue(value)).toEqual({
      referenceType: "view-definition",
      referenceId: "vd-1",
    });
  });

  // The codec round-trips a sql-view reference.
  it("round-trips a sql-view reference", () => {
    const value = encodeViewReferenceValue("sql-view", "lib-1");
    expect(value).toBe("sql-view:lib-1");
    expect(decodeViewReferenceValue(value)).toEqual({
      referenceType: "sql-view",
      referenceId: "lib-1",
    });
  });

  // Splitting on the first colon keeps ids that themselves contain colons
  // intact, so the two id namespaces never collide.
  it("preserves ids that contain colons", () => {
    const value = encodeViewReferenceValue("sql-view", "urn:uuid:abc:def");
    expect(decodeViewReferenceValue(value)).toEqual({
      referenceType: "sql-view",
      referenceId: "urn:uuid:abc:def",
    });
  });
});

describe("buildInlineSqlQueryLibrary", () => {
  // The assembled Library carries the SQL on FHIR profile, the
  // sql-query type code and the SQL both Base64-encoded and as plain
  // text via the `sql-text` extension.
  it("assembles a Library conforming to the SQLQuery profile", () => {
    const library = buildInlineSqlQueryLibrary({
      title: "patients-by-condition",
      sql: "SELECT 1",
      tables: [
        {
          rowId: "r1",
          label: "patients",
          referenceType: "view-definition",
          referenceId: "vd-patients",
        },
      ],
      parameters: [{ rowId: "p1", name: "patient_id", type: "string" }],
    });

    expect(library.resourceType).toBe("Library");
    expect(library.status).toBe("active");
    expect(library.meta?.profile).toContain(
      "https://sql-on-fhir.org/ig/StructureDefinition/SQLQuery",
    );
    expect(library.type.coding[0].code).toBe("sql-query");
    expect(library.title).toBe("patients-by-condition");
    expect(library.name).toBe("patients-by-condition");
    expect(library.content[0].contentType).toBe("application/sql");
    expect(decodeSql(library.content[0].data)).toBe("SELECT 1");
    const sqlExt = library.content[0].extension?.find((e) =>
      e.url.endsWith("/sql-text"),
    );
    expect(sqlExt?.valueString).toBe("SELECT 1");
    expect(library.relatedArtifact).toEqual([
      {
        type: "depends-on",
        label: "patients",
        resource: "ViewDefinition/vd-patients",
      },
    ]);
    expect(library.parameter).toEqual([
      { name: "patient_id", use: "in", type: "string" },
    ]);
  });

  // A view row backed by a SQLView emits a Library/<id> reference, while a
  // ViewDefinition row emits ViewDefinition/<id>.
  it("emits the correct reference prefix per source kind", () => {
    const library = buildInlineSqlQueryLibrary({
      sql: "SELECT 1",
      tables: [
        {
          rowId: "r1",
          label: "patients",
          referenceType: "view-definition",
          referenceId: "patient-demographics",
        },
        {
          rowId: "r2",
          label: "active",
          referenceType: "sql-view",
          referenceId: "active-patients",
        },
      ],
      parameters: [],
    });

    expect(library.relatedArtifact).toEqual([
      {
        type: "depends-on",
        label: "patients",
        resource: "ViewDefinition/patient-demographics",
      },
      {
        type: "depends-on",
        label: "active",
        resource: "Library/active-patients",
      },
    ]);
  });

  // Empty title and url do not introduce empty slots on the resource.
  it("omits empty title and url", () => {
    const library = buildInlineSqlQueryLibrary({
      sql: "SELECT 1",
      tables: [],
      parameters: [],
    });
    expect(library.title).toBeUndefined();
    expect(library.name).toBeUndefined();
    expect(library.url).toBeUndefined();
    expect(library.relatedArtifact).toBeUndefined();
    expect(library.parameter).toBeUndefined();
  });

  // Title with whitespace is normalised into the `name` slot using
  // hyphens and lower-case, while the original string is preserved on
  // the `title` slot.
  it("normalises the title into a slug for the name slot", () => {
    const library = buildInlineSqlQueryLibrary({
      title: "Patients By Condition",
      sql: "SELECT 1",
      tables: [],
      parameters: [],
    });
    expect(library.title).toBe("Patients By Condition");
    expect(library.name).toBe("patients-by-condition");
  });
});

describe("canExecuteInlineForm", () => {
  // Empty SQL prevents execution.
  it("returns false when SQL is blank", () => {
    expect(
      canExecuteInlineForm({
        sql: "   ",
        tables: [
          {
            rowId: "r1",
            label: "patients",
            referenceType: "view-definition",
            referenceId: "vd1",
          },
        ],
        parameters: [],
      }),
    ).toBe(false);
  });

  // Zero views prevents execution because the server requires at least
  // one related artefact.
  it("returns false when there are no views", () => {
    expect(
      canExecuteInlineForm({
        sql: "SELECT 1",
        tables: [],
        parameters: [],
      }),
    ).toBe(false);
  });

  // A view row with a blank label is incomplete.
  it("returns false when a view row has no label", () => {
    expect(
      canExecuteInlineForm({
        sql: "SELECT 1",
        tables: [
          {
            rowId: "r1",
            label: "",
            referenceType: "view-definition",
            referenceId: "vd1",
          },
        ],
        parameters: [],
      }),
    ).toBe(false);
  });

  // A view row with no source picked (no referenceType / referenceId) is
  // incomplete.
  it("returns false when a view row has no source selected", () => {
    expect(
      canExecuteInlineForm({
        sql: "SELECT 1",
        tables: [{ rowId: "r1", label: "patients", referenceId: "" }],
        parameters: [],
      }),
    ).toBe(false);
  });

  // Minimum valid input has SQL and at least one well-formed view row.
  it("returns true for the minimum valid input", () => {
    expect(
      canExecuteInlineForm({
        sql: "SELECT 1",
        tables: [
          {
            rowId: "r1",
            label: "patients",
            referenceType: "sql-view",
            referenceId: "lib1",
          },
        ],
        parameters: [],
      }),
    ).toBe(true);
  });
});

describe("canSaveInlineForm", () => {
  // Save additionally requires a non-empty title.
  it("returns false without a title", () => {
    expect(
      canSaveInlineForm({
        sql: "SELECT 1",
        tables: [
          {
            rowId: "r1",
            label: "patients",
            referenceType: "view-definition",
            referenceId: "vd1",
          },
        ],
        parameters: [],
      }),
    ).toBe(false);
  });

  it("returns true when execute is valid and a title is supplied", () => {
    expect(
      canSaveInlineForm({
        title: "patients-by-condition",
        sql: "SELECT 1",
        tables: [
          {
            rowId: "r1",
            label: "patients",
            referenceType: "view-definition",
            referenceId: "vd1",
          },
        ],
        parameters: [],
      }),
    ).toBe(true);
  });
});

describe("isRuntimeValueValid", () => {
  // Strings always pass.
  it("accepts any string for type=string", () => {
    expect(isRuntimeValueValid("foo bar", "string")).toBe(true);
  });

  // Integer rejects non-integer input.
  it("rejects non-integer input for type=integer", () => {
    expect(isRuntimeValueValid("42", "integer")).toBe(true);
    expect(isRuntimeValueValid("-3", "integer")).toBe(true);
    expect(isRuntimeValueValid("3.14", "integer")).toBe(false);
    expect(isRuntimeValueValid("abc", "integer")).toBe(false);
  });

  // Decimal accepts integer and decimal forms.
  it("accepts decimal input for type=decimal", () => {
    expect(isRuntimeValueValid("3.14", "decimal")).toBe(true);
    expect(isRuntimeValueValid("3", "decimal")).toBe(true);
    expect(isRuntimeValueValid("abc", "decimal")).toBe(false);
  });

  // Boolean only accepts the string "true" or "false".
  it("only accepts true/false for type=boolean", () => {
    expect(isRuntimeValueValid("true", "boolean")).toBe(true);
    expect(isRuntimeValueValid("false", "boolean")).toBe(true);
    expect(isRuntimeValueValid("yes", "boolean")).toBe(false);
  });

  // Date requires the ISO 8601 calendar form.
  it("validates ISO 8601 dates for type=date", () => {
    expect(isRuntimeValueValid("2025-01-15", "date")).toBe(true);
    expect(isRuntimeValueValid("2025-1-15", "date")).toBe(false);
  });

  // DateTime accepts the canonical and zoned forms.
  it("validates ISO 8601 dateTimes for type=dateTime", () => {
    expect(isRuntimeValueValid("2025-01-15T12:00:00Z", "dateTime")).toBe(true);
    expect(isRuntimeValueValid("2025-01-15T12:00:00+10:00", "dateTime")).toBe(
      true,
    );
    expect(isRuntimeValueValid("yesterday", "dateTime")).toBe(false);
  });
});

describe("areRuntimeBindingsValid", () => {
  // Empty bindings pass for declared-but-not-bound parameters.
  it("accepts empty bindings", () => {
    expect(
      areRuntimeBindingsValid([{ name: "x", type: "integer" }], { x: "" }),
    ).toBe(true);
    expect(areRuntimeBindingsValid([{ name: "x", type: "integer" }], {})).toBe(
      true,
    );
  });

  it("rejects when any binding fails type validation", () => {
    expect(
      areRuntimeBindingsValid([{ name: "x", type: "integer" }], { x: "abc" }),
    ).toBe(false);
  });

  it("accepts when all bindings pass type validation", () => {
    expect(
      areRuntimeBindingsValid(
        [
          { name: "x", type: "integer" },
          { name: "y", type: "string" },
        ],
        { x: "42", y: "hello" },
      ),
    ).toBe(true);
  });
});

describe("buildParameterTypes", () => {
  // Builds a name-keyed map suitable for the API client.
  it("maps declared parameters to a type lookup", () => {
    expect(
      buildParameterTypes([
        { name: "patient_id", type: "string" },
        { name: "active", type: "boolean" },
      ]),
    ).toEqual({ patient_id: "string", active: "boolean" });
  });
});
