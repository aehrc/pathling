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

import { decodeSql, encodeSql } from "../../../utils/sqlBase64";
import {
  areBindingsCompleteAndValid,
  areParameterRowsValid,
  areRuntimeBindingsValid,
  buildInlineSqlQueryLibrary,
  buildParameterTypes,
  canExecuteInlineForm,
  canSaveInlineForm,
  extractRequestSql,
  findDuplicateParameterNames,
  isRuntimeValueValid,
  rowsToBindings,
} from "../sqlQueryFormHelpers";

import type {
  SqlQueryLibrary,
  SqlQueryParameterDeclaration,
  SqlQueryParameterType,
  SqlQueryRequest,
} from "../../../types/sqlQuery";

/**
 * Builds an inline parameter row, so each test states only the fields it
 * cares about.
 *
 * @param name - The declared parameter name.
 * @param type - The declared parameter type.
 * @param value - The runtime value entered against the row.
 * @returns A parameter row with a unique row id.
 */
function row(
  name: string,
  type: SqlQueryParameterType,
  value: string,
): SqlQueryParameterDeclaration {
  return { rowId: `${name}-${type}-${value}`, name, type, value };
}

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
          referenceUrl: "https://example.org/ViewDefinition/patients",
        },
      ],
      parameters: [
        { rowId: "p1", name: "patient_id", type: "string", value: "pat-1" },
      ],
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
        resource: "https://example.org/ViewDefinition/patients",
      },
    ]);
    expect(library.parameter).toEqual([
      { name: "patient_id", use: "in", type: "string" },
    ]);
  });

  // FR-008: the value entered against a row is runtime-only state, so the
  // saved declaration carries exactly name, use and type. The emitted key set
  // is asserted in full: `ParameterDefinition` has no default-value element,
  // and no extension may be introduced to smuggle one in.
  it("persists only name, use and type for a row carrying a value", () => {
    const library = buildInlineSqlQueryLibrary({
      title: "Period query",
      sql: "SELECT 1",
      tables: [],
      parameters: [row("period_end", "date", "2025-06-30")],
    });

    const parameters = library.parameter ?? [];
    expect(parameters).toHaveLength(1);
    expect(Object.keys(parameters[0]).sort()).toEqual(["name", "type", "use"]);
    expect(parameters[0]).toStrictEqual({
      name: "period_end",
      use: "in",
      type: "date",
    });
  });

  // Each row emits its chosen source's canonical URL verbatim as the
  // relatedArtifact.resource - regardless of whether the source is a
  // ViewDefinition or a SQLView - so the saved query round-trips a source by
  // URL.
  it("emits each row's canonical url as the relatedArtifact resource", () => {
    const library = buildInlineSqlQueryLibrary({
      sql: "SELECT 1",
      tables: [
        {
          rowId: "r1",
          label: "patients",
          referenceUrl:
            "https://pathling.example/ViewDefinition/patient_demographics",
        },
        {
          rowId: "r2",
          label: "active",
          referenceUrl: "https://pathling.example/Library/ObservationPeriod",
        },
      ],
      parameters: [],
    });

    expect(library.relatedArtifact).toEqual([
      {
        type: "depends-on",
        label: "patients",
        resource:
          "https://pathling.example/ViewDefinition/patient_demographics",
      },
      {
        type: "depends-on",
        label: "active",
        resource: "https://pathling.example/Library/ObservationPeriod",
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
            referenceUrl: "https://example.org/V",
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
          { rowId: "r1", label: "", referenceUrl: "https://example.org/V" },
        ],
        parameters: [],
      }),
    ).toBe(false);
  });

  // A view row with no source picked (no referenceUrl) is incomplete.
  it("returns false when a view row has no source selected", () => {
    expect(
      canExecuteInlineForm({
        sql: "SELECT 1",
        tables: [{ rowId: "r1", label: "patients", referenceUrl: "" }],
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
            referenceUrl: "https://example.org/Library/lib1",
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
            referenceUrl: "https://example.org/V",
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
            referenceUrl: "https://example.org/V",
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

describe("areBindingsCompleteAndValid", () => {
  // A declared parameter with no entry in the bindings map is unbound, which
  // is exactly the case that produces the opaque server failure.
  it("rejects a declared parameter with no binding", () => {
    expect(
      areBindingsCompleteAndValid([{ name: "x", type: "integer" }], {}),
    ).toBe(false);
  });

  // An empty string is unbound too, not a value of the empty string.
  it("rejects a declared parameter bound to an empty value", () => {
    expect(
      areBindingsCompleteAndValid([{ name: "x", type: "integer" }], { x: "" }),
    ).toBe(false);
  });

  // A value that does not parse as its declared type cannot be submitted.
  it("rejects a value that fails type validation", () => {
    expect(
      areBindingsCompleteAndValid([{ name: "x", type: "integer" }], {
        x: "abc",
      }),
    ).toBe(false);
  });

  // Every parameter carrying a parseable value is submittable.
  it("accepts when every parameter has a valid value", () => {
    expect(
      areBindingsCompleteAndValid(
        [
          { name: "x", type: "integer" },
          { name: "period_end", type: "date" },
        ],
        { x: "42", period_end: "2025-06-30" },
      ),
    ).toBe(true);
  });

  // A boolean switch cannot express "unbound": an absent entry is false.
  it("accepts a boolean parameter with no binding, treating it as false", () => {
    expect(
      areBindingsCompleteAndValid([{ name: "flag", type: "boolean" }], {}),
    ).toBe(true);
    expect(
      areBindingsCompleteAndValid([{ name: "flag", type: "boolean" }], {
        flag: "",
      }),
    ).toBe(true);
  });

  // A query declaring nothing imposes no constraint.
  it("accepts a query with no declared parameters", () => {
    expect(areBindingsCompleteAndValid([], {})).toBe(true);
  });
});

describe("findDuplicateParameterNames", () => {
  // Two rows binding the same name are ambiguous, and both are reported.
  it("reports a name declared by two rows", () => {
    expect(
      findDuplicateParameterNames([
        row("x", "string", "a"),
        row("x", "integer", "1"),
      ]),
    ).toEqual(new Set(["x"]));
  });

  // Names are compared after trimming, since the declaration is trimmed too.
  it("compares names after trimming", () => {
    expect(
      findDuplicateParameterNames([
        row(" x ", "string", "a"),
        row("x", "string", "b"),
      ]),
    ).toEqual(new Set(["x"]));
  });

  // Unnamed rows are not declarations, so they never collide.
  it("never reports empty names as duplicates", () => {
    expect(
      findDuplicateParameterNames([
        row("", "string", "a"),
        row("   ", "string", "b"),
      ]),
    ).toEqual(new Set());
  });

  // Distinct names are unambiguous.
  it("reports nothing when every name is distinct", () => {
    expect(
      findDuplicateParameterNames([
        row("x", "string", "a"),
        row("y", "string", "b"),
      ]),
    ).toEqual(new Set());
  });
});

describe("areParameterRowsValid", () => {
  // A named row with no value is unbound.
  it("rejects a named row with an empty value", () => {
    expect(areParameterRowsValid([row("x", "string", "")])).toBe(false);
  });

  // A named row whose value does not parse as its type cannot be submitted.
  it("rejects a named row with an invalid value", () => {
    expect(areParameterRowsValid([row("x", "integer", "abc")])).toBe(false);
  });

  // Ambiguous names block execution regardless of their values.
  it("rejects duplicate names among named rows", () => {
    expect(
      areParameterRowsValid([row("x", "string", "a"), row("x", "string", "b")]),
    ).toBe(false);
  });

  // An unnamed row is not a declaration, so neither its emptiness nor an
  // unparseable value blocks execution.
  it("ignores rows with an empty name", () => {
    expect(areParameterRowsValid([row("", "integer", "")])).toBe(true);
    expect(areParameterRowsValid([row("  ", "integer", "abc")])).toBe(true);
  });

  // A boolean row is always bound, so an untouched switch is still valid.
  it("accepts a boolean row with an empty value", () => {
    expect(areParameterRowsValid([row("flag", "boolean", "")])).toBe(true);
  });

  // Every named row carrying a parseable value is submittable.
  it("accepts named rows with valid values", () => {
    expect(
      areParameterRowsValid([
        row("period_end", "date", "2025-06-30"),
        row("count", "integer", "7"),
      ]),
    ).toBe(true);
  });

  // No rows at all imposes no constraint.
  it("accepts an empty row list", () => {
    expect(areParameterRowsValid([])).toBe(true);
  });
});

describe("rowsToBindings", () => {
  // Named rows become the name-keyed map the request assembly consumes.
  it("maps named rows to their values", () => {
    expect(
      rowsToBindings([
        row("period_end", "date", "2025-06-30"),
        row("count", "integer", "7"),
      ]),
    ).toEqual({ period_end: "2025-06-30", count: "7" });
  });

  // An untouched boolean switch reads as false, and binds as false.
  it("emits false for a boolean row with an empty value", () => {
    expect(rowsToBindings([row("flag", "boolean", "")])).toEqual({
      flag: "false",
    });
  });

  // A named row with no value contributes nothing, so seeding the stored
  // tab's bindings on save leaves that parameter unbound rather than bound
  // to an empty string.
  it("drops named rows with an empty non-boolean value", () => {
    expect(rowsToBindings([row("x", "string", "")])).toEqual({});
  });

  // Unnamed rows are not declarations and cannot be bound.
  it("drops rows with an empty name", () => {
    expect(rowsToBindings([row("  ", "string", "a")])).toEqual({});
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

describe("extractRequestSql", () => {
  /**
   * Builds an inline request wrapping a Library with the supplied content,
   * so the recovery paths can be exercised in isolation.
   *
   * @param content - The `Library.content` array to embed.
   * @returns An inline SQL query request.
   */
  function inlineRequest(content: SqlQueryLibrary["content"]): SqlQueryRequest {
    return {
      mode: "inline",
      library: {
        resourceType: "Library",
        status: "active",
        type: {
          coding: [
            {
              system: "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes",
              code: "sql-query",
            },
          ],
        },
        content,
      },
    };
  }

  // A stored request carries the SQL the form resolved from the picker.
  it("returns the resolved SQL for a stored request", () => {
    expect(
      extractRequestSql({
        mode: "stored",
        libraryId: "lib-1",
        sql: "SELECT 1",
      }),
    ).toBe("SELECT 1");
  });

  // A stored request that was built without a resolved SQL yields an empty
  // string rather than throwing.
  it("returns an empty string for a stored request with no resolved SQL", () => {
    expect(extractRequestSql({ mode: "stored", libraryId: "lib-1" })).toBe("");
  });

  // Inline requests prefer the human-readable sql-text extension.
  it("returns the sql-text extension for an inline request", () => {
    const request = inlineRequest([
      {
        contentType: "application/sql",
        data: encodeSql("SELECT 99"),
        extension: [
          {
            url: "https://sql-on-fhir.org/ig/StructureDefinition/sql-text",
            valueString: "SELECT 2",
          },
        ],
      },
    ]);
    expect(extractRequestSql(request)).toBe("SELECT 2");
  });

  // When no extension is present, inline requests fall back to decoding the
  // Base64 data.
  it("decodes the Base64 data when no sql-text extension is present", () => {
    const request = inlineRequest([
      { contentType: "application/sql", data: encodeSql("SELECT 3") },
    ]);
    expect(extractRequestSql(request)).toBe("SELECT 3");
  });

  // An inline request with no content has no SQL to recover.
  it("returns an empty string for an inline request with no content", () => {
    expect(extractRequestSql(inlineRequest([]))).toBe("");
  });
});
