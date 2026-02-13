# Pathling Admin UI

A web-based administration interface for managing
[Pathling](https://pathling.csiro.au/) FHIR servers. Provides tools for bulk
data operations, resource management, and SQL on FHIR queries.

## Features

- **Bulk export** - Export FHIR resources with configurable filters and
  resource type selection
- **Bulk import** - Import FHIR data from NDJSON sources
- **Resource management** - Search, view, create, update, and delete FHIR
  resources
- **SQL on FHIR** - Create and execute ViewDefinitions to project FHIR data
  into tabular formats

## Technology stack

- [React](https://react.dev/) 19 with TypeScript
- [Vite](https://vite.dev/) for development and building
- [Radix UI Themes](https://www.radix-ui.com/) for accessible UI components
- [TanStack Query](https://tanstack.com/query) for server state management
- [React Router](https://reactrouter.com/) for client-side routing
- [fhirclient](https://github.com/smart-on-fhir/client-js) for SMART on FHIR
  authentication

## Configuration

The UI is configured via environment variables. Create a `.env.local` file for
local development or set these at build time for production:

| Variable             | Description                      | Default                   |
| -------------------- | -------------------------------- | ------------------------- |
| `VITE_FHIR_BASE_URL` | FHIR server endpoint URL         | `/fhir`                   |
| `VITE_CLIENT_ID`     | OAuth client ID for SMART launch | `pathling-admin-ui`       |
| `VITE_SCOPE`         | OAuth scopes to request          | `openid profile user/*.*` |

Example `.env.local` for development:

```
VITE_FHIR_BASE_URL=https://demo.pathling.app/fhir
```

The UI is designed to be served from the `/admin/` path on the same host as the
Pathling server.

## Development

### Prerequisites

- [Node.js](https://nodejs.org/) 20+
- [Bun](https://bun.sh/) package manager

### Getting started

```bash
# Install dependencies
bun install

# Start development server
bun run dev
```

The development server runs at `http://localhost:5173/admin/`.

### Available scripts

| Script             | Description                       |
| ------------------ | --------------------------------- |
| `bun run dev`      | Start development server with HMR |
| `bun run build`    | Build for production              |
| `bun run preview`  | Preview production build locally  |
| `bun run lint`     | Run ESLint                        |
| `bun run format`   | Format code with Prettier         |
| `bun run test`     | Run unit tests in watch mode      |
| `bun run test:run` | Run unit tests once               |
| `bun run test:e2e` | Run end-to-end tests              |

## Building and hosting

Build the production bundle:

```bash
bun run build
```

The output is written to `dist/`. Serve these static files from the `/admin/`
path on your web server.

### Integration with Pathling server

The Pathling server serves the UI from `/admin/` when configured. Place the
built `dist/` contents in the server's static resources directory, or configure
your reverse proxy to serve the UI alongside the FHIR API.

## Testing

### Unit tests

Unit tests use [Vitest](https://vitest.dev/) with
[Testing Library](https://testing-library.com/docs/react-testing-library/intro/).

```bash
# Watch mode
bun run test

# Single run
bun run test:run
```

Test files are located in `__tests__` directories alongside their source files
(e.g., `src/api/__tests__/rest.test.ts`).

**Conventions:**

- Follow the Arrange-Act-Assert pattern
- Mock external APIs using `vi.fn()` and `vi.stubGlobal()`
- Test both success and error cases (401, 404, 500 responses)
- Reset mocks in `afterEach` blocks

Example structure:

```typescript
describe("functionName", () => {
    beforeEach(() => {
        vi.stubGlobal("fetch", mockFetch);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it("returns data on success", async () => {
        mockFetch.mockResolvedValueOnce(successResponse);
        const result = await functionUnderTest();
        expect(result).toEqual(expectedData);
    });

    it("throws UnauthorizedError on 401", async () => {
        mockFetch.mockResolvedValueOnce(unauthorizedResponse);
        await expect(functionUnderTest()).rejects.toThrow(UnauthorizedError);
    });
});
```

### End-to-end tests

E2E tests use [Playwright](https://playwright.dev/).

```bash
# Headless
bun run test:e2e

# With UI
bun run test:e2e:ui

# Headed browser
bun run test:e2e:headed
```

Test files are in the `e2e/` directory. Mock data fixtures are in
`e2e/fixtures/fhirData.ts`.

**Conventions:**

- Mock network requests using `page.route()`
- Use role-based selectors (`getByRole`, `getByText`)
- Test complete user flows including error states

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for coding conventions, static analysis
configuration, and quality checks.

## Licence

Copyright 2026 Commonwealth Scientific and Industrial Research Organisation
(CSIRO) ABN 41 687 119 230.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at

<https://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
