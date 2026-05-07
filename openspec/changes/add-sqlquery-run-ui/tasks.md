## 1. Types and shared utilities

- [x] 1.1 Add a new `ui/src/types/sqlQuery.ts` module defining
      `SqlQueryLibrary` (Library shape conforming to the SQLQuery profile),
      `SqlQueryRelatedArtifact`, `SqlQueryParameterDeclaration` (FHIR
      primitive types we support), `SqlQueryRuntimeBindings`,
      `SqlQueryRequest` (mode `stored` | `inline`, request payload) and
      `SqlQueryOutputFormat`.
- [x] 1.2 Add a small CSV parser utility in
      `ui/src/utils/csv.ts` with unit tests covering quoted values,
      embedded commas, CRLF and empty cells; mirror the public API of
      `parseNdjsonResponse` from `utils/ndjson.ts`.
- [x] 1.3 Add a Library-flattener utility that turns a `_format=fhir`
      Parameters response into `{columns, rows}` matching the existing
      `ViewDefinitionResult` shape, with unit tests for typed and missing
      parts and zero-row responses.
- [x] 1.4 Add a Base64 SQL helper (encode plus decode) in
      `ui/src/utils/sqlBase64.ts` using browser `btoa`/`atob` with a
      Unicode-safe wrapper, plus unit tests.

## 2. API client

- [x] 2.1 Create `ui/src/api/sqlQuery.ts` exporting `sqlQueryRun` (POST
      `/$sqlquery-run` with `queryReference` or `queryResource`) and
      `listSqlQueryLibraries` (GET `/Library?type=…|sql-query`),
      following the `view.ts` style and reusing
      `buildHeaders`/`buildUrl`/`checkResponse`.
- [x] 2.2 Re-export the new functions from `ui/src/api/index.ts`.
- [x] 2.3 Add unit tests in `ui/src/api/__tests__/sqlQuery.test.ts`
      covering: 200 with body for inline and stored modes, request shape
      assertions (Parameters body, `_format`, `_limit`, `_header`,
      nested `parameters`), 400 with OperationOutcome, 401 throwing
      `UnauthorizedError`.

## 3. Hooks

- [x] 3.1 Add `ui/src/hooks/useSqlQueryLibraries.ts` (TanStack Query
      `useQuery` that calls `listSqlQueryLibraries`); export from
      `ui/src/hooks/index.ts`.
- [x] 3.2 Add `ui/src/hooks/useSaveSqlQueryLibrary.ts` mirroring
      `useSaveViewDefinition.ts`; export from the hooks index.
- [x] 3.3 Add `ui/src/hooks/useSqlQueryRun.ts` providing imperative
      `execute`, `reset` and `status`/`result`/`error` similar to
      `useViewRun`; branch on `_format` to either parse rows
      (csv/ndjson/json/fhir) or return a binary blob (parquet); export
      from the hooks index.
- [x] 3.4 Unit-test pure helpers used by the hook (request-body
      assembly, response branching) with Vitest. Hooks themselves stay
      thin per the project's React rules.

## 4. Form components

- [x] 4.1 Add a top-level `Tabs.Root` to
      `ui/src/components/sqlOnFhir/SqlOnFhirForm.tsx` (or extract the
      existing form into `ViewDefinitionForm` and create a new
      `SqlOnFhirForm` shell that switches between `ViewDefinitionForm`
      and a new `SqlQueryForm`).
- [x] 4.2 Implement `ui/src/components/sqlOnFhir/SqlQueryForm.tsx` with
      the inner `Tabs.Root` for "Select query" vs "Provide SQL"
      and the runtime-values + output-options section shared below the
      tabs. Wire it up to the new hooks and types.
- [x] 4.3 Implement `SqlQueryStoredTab.tsx` (Library picker, decoded
      SQL preview with copy `IconButton`, related-artifact summary,
      declared-parameters summary).
- [x] 4.4 Implement `SqlQueryInlineTab.tsx` (SQL `TextArea`, "Tables"
      list with add/remove rows, "Parameters" list with add/remove
      rows).
- [x] 4.5 Implement `SqlQueryRuntimeBindings.tsx` rendering one input
      per declared parameter using the type-to-input mapping (string,
      integer, decimal, boolean, date, dateTime, code).
- [x] 4.6 Implement `SqlQueryOutputControls.tsx` with the
      `Select.Root` for output format, the numeric `_limit` field and
      the `_header` `Switch` (visible only when format is csv).
- [x] 4.7 Wire the Execute and Save-to-server actions; disable Execute
      when required inputs are missing; hide Save in the stored tab.

## 5. Result card

- [x] 5.1 Implement `ui/src/components/sqlOnFhir/SqlQueryCard.tsx`
      mirroring `ViewCard` (Card chrome, job ID, timestamp, format
      `Badge`, close button when terminated).
- [x] 5.2 Render the pending state (`Spinner` + "Executing SQL
      query…") and route to the right body based on output format:
      `Table` for tabular formats, download-only for parquet, error
      `Callout` with submitted SQL on failure.
- [x] 5.3 Implement the download action by streaming the cached
      response body to a Blob and triggering a download with a
      filename derived from the format.
- [x] 5.4 Add a `SqlQueryJob` type to
      `ui/src/types/sqlQuery.ts` for the in-page job list (ID, mode,
      request, createdAt) - parallel to `ViewJob`.

## 6. Page wiring

- [x] 6.1 Update `ui/src/pages/SqlOnFhir.tsx` to track both
      `ViewJob[]` and `SqlQueryJob[]` and to render `SqlQueryCard`
      alongside `ViewCard` in the result column, sorted by `createdAt`
      descending.
- [x] 6.2 Pipe the Execute and Save callbacks through the new form
      components, including the auto-switch to "Select query" with
      the new Library ID after a successful save.
- [x] 6.3 Confirm the existing auth gate, server-capabilities check
      and `SessionExpiredDialog` continue to apply unchanged.

## 7. Tests

- [x] 7.1 Unit-test the form submission helpers (Library
      construction, request assembly, runtime-binding mapping per FHIR
      type) in `ui/src/components/sqlOnFhir/__tests__/`.
- [x] 7.2 Unit-test result branching (CSV / NDJSON / JSON / FHIR /
      parquet) and OperationOutcome rendering in `SqlQueryCard`'s tests.
- [x] 7.3 Add Playwright E2E coverage in `ui/e2e/` for: (a) executing a
      stored Library, (b) authoring an inline Library and executing
      it, (c) saving an inline Library and seeing it in the picker,
      (d) error rendering when the server returns a 400. Mock the FHIR
      endpoints with `page.route()`.

## 8. Docs and polish

- [x] 8.1 Update `ui/CONTRIBUTING.md` only if a new pattern is
      introduced that future contributors need to know about (else
      skip; the README does not document individual pages).
- [x] 8.2 Run `bun run lint`, `bun run lint:duplication`,
      `bun run test:coverage`, `bun run test:e2e` and `bun run build`
      from `ui/` and fix any regressions.
- [x] 8.3 Manually exercise the page in dev mode (`bun run dev`)
      against a local Pathling server with the
      `feature/sql-run-operation` branch deployed; verify each
      wireframe scenario (`wireframes/index.html`) renders in line
      with the design.
