# How to contribute to Pathling Admin UI

This document provides guidance specific to working on the `ui` module. For
contribution guidelines for the Pathling core libraries, see the main
[CONTRIBUTING.md](../CONTRIBUTING.md) in the repository root.

## Development dependencies

You will need the following software:

- [Node.js](https://nodejs.org/) 20+
- [Bun](https://bun.sh/) package manager

## Getting started

Install dependencies and start the development server:

```bash
cd ui
bun install
bun run dev
```

The development server runs at `http://localhost:5173/admin/`.

## Technology stack

- [React](https://react.dev/) 19 with TypeScript
- [Vite](https://vite.dev/) for development and building
- [Radix UI Themes](https://www.radix-ui.com/) for accessible UI components
- [TanStack Query](https://tanstack.com/query) for server state management
- [React Router](https://reactrouter.com/) for client-side routing
- [fhirclient](https://github.com/smart-on-fhir/client-js) for SMART on FHIR
  authentication

## Building

Build the production bundle:

```bash
bun run build
```

The output is written to `dist/`.

## Testing

### Unit tests

Unit tests use [Vitest](https://vitest.dev/) with
[Testing Library](https://testing-library.com/docs/react-testing-library/intro/).
Test files are located in `__tests__` directories alongside their source files
(e.g., `src/api/__tests__/rest.test.ts`).

```bash
# Watch mode
bun run test

# Single run
bun run test:run

# With coverage
bun run test:coverage
```

Coverage thresholds are set to 80% for lines, functions, branches, and
statements.

### End-to-end tests

E2E tests use [Playwright](https://playwright.dev/). Test files are in the
`e2e/` directory.

```bash
# Headless
bun run test:e2e

# With UI
bun run test:e2e:ui

# Headed browser
bun run test:e2e:headed
```

### Test conventions

- Follow the Arrange-Act-Assert pattern.
- Mock external APIs using `vi.fn()` and `vi.stubGlobal()`.
- Test both success and error cases (401, 404, 500 responses).
- Reset mocks in `afterEach` blocks.
- Put significant logic into plain functions and unit test those
  comprehensively. Keep hooks as thin wrappers that compose plain functions with
  React primitives.
- Use role-based selectors (`getByRole`, `getByText`) in E2E tests.
- Mock network requests using `page.route()` in Playwright tests.

## Coding conventions

### TypeScript

- **No classes.** Avoid the `class` keyword entirely. Use plain functions,
  closures, and immutable data structures. The only exception is error
  boundaries, which require class components.
- **Pure functional style.** Prefer composition over inheritance. Use `type` and
  `interface` for data shapes, and express behaviour through standalone
  functions.
- **Use lower camel case for file names.** Name files using `lowerCamelCase.ts`
  (e.g., `bulkExport.ts`, `useCreate.ts`). The exception is React component
  files, which use Pascal case (e.g., `ErrorBoundary.tsx`).
- **Comprehensive JSDoc on all exported functions.** Every exported function
  must have a JSDoc comment that includes a description, `@param` tags,
  `@returns` tag, and `@throws` tags where applicable.

### React

- **Functional components only.** Class components are only permitted for error
  boundaries.
- **Components must be pure.** Same props and state must produce the same
  output.
- **Never nest component definitions** inside other components.
- **Extract significant logic into custom hooks.** Components should primarily
  compose hooks and render UI.
- **Local state first.** Use `useState`/`useReducer` before reaching for
  context. Only use React Context for state that genuinely needs to be shared
  across distant components (auth, jobs, toasts).
- **TanStack Query for all server state.** Use TanStack Query for data
  fetching, caching, and mutations.
- **Avoid effects where possible.** Do not use effects to transform data for
  rendering or to handle user events. Effects are escape hatches, not primary
  tools.

### Naming

| Element               | Convention     | Example                         |
| --------------------- | -------------- | ------------------------------- |
| Components            | `PascalCase`   | `ExportForm`                    |
| Props and variables   | `camelCase`    | `resourceType`                  |
| Event handler props   | `on` prefix    | `onClick`, `onSubmit`           |
| Event handler methods | `handle`       | `handleClick`, `handleSubmit`   |
| Custom hooks          | `use` prefix   | `useAuth`, `useBulkExport`      |
| Boolean props         | omit `={true}` | `<Foo hidden />`                |
| Files                 | `camelCase`    | `bulkExport.ts`, `useCreate.ts` |
| Component files       | `PascalCase`   | `ExportForm.tsx`                |

### Import ordering

Imports are enforced by ESLint and must follow this order, with blank lines
between groups:

1. Built-in and external packages
2. Internal modules
3. Parent, sibling, and index imports
4. Type imports

### Accessibility

- Use semantic HTML elements (`<button>`, `<a>`, landmarks, proper headings,
  `<label>` with `htmlFor`).
- All interactive elements must be keyboard accessible.
- Use ARIA attributes (`aria-label`, `aria-labelledby`, `aria-describedby`)
  where visible labels are not present.
- Trap focus inside modals and dialogs.

### State management

Follow this hierarchy when choosing where to put state:

1. **Local state** (`useState`/`useReducer`) for single components.
2. **Lifted state** when two or three siblings need the same data.
3. **Context API** for truly global, infrequently-changing data (themes, auth,
   locale).
4. **TanStack Query** for all server state (API data, caching, refetching).

### Performance

Measure before optimising. Use React DevTools Profiler before reaching for
`useMemo`, `useCallback`, or `React.memo`.

## Static analysis

### Linting

ESLint 9 with flat config format. The configuration is in `eslint.config.js`
and includes type-aware TypeScript linting, React hooks rules, accessibility
checks, JSDoc enforcement, and import ordering.

```bash
bun run lint
bun run lint:fix
```

### Formatting

Prettier with default settings.

```bash
bun run format
bun run format:check
```

### Code duplication

jscpd detects copy-paste duplication.

```bash
bun run lint:duplication
```

### Type checking

TypeScript strict mode is enabled with `noUnusedLocals`,
`noUnusedParameters`, and `noFallthroughCasesInSwitch`.

## Quality checks

Run the following after making code changes to ensure quality:

- `bun run build`
- `bun run lint`
- `bun run lint:duplication`
- `bun run test:coverage`
- `bun run test:e2e`
