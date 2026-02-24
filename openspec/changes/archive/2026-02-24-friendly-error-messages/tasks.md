## 1. Tests

- [x] 1.1 Add test for Py4J error message extraction (concise message in
      details.text, full trace in diagnostics)
- [x] 1.2 Add test for Py4J error with unparseable format (falls back to full
      string, no diagnostics)
- [x] 1.3 Add test for non-Py4J exception (message passes through, no
      diagnostics)

## 2. Implementation

- [x] 2.1 Add helper function to extract human-readable message from
      Py4JJavaError string
- [x] 2.2 Update `_handle_evaluate` to use separate message and diagnostics for
      Py4J errors

## 3. Spec update

- [x] 3.1 Update the error handling requirement in the fhirpath-lab-server main
      spec
