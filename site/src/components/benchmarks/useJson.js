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
 * A hook that fetches JSON from the benchmark data directory.
 *
 * @author John Grimes
 */

import { useEffect, useState } from "react";

/**
 * The state of a fetch.
 *
 * @typedef {object} FetchState
 * @property {any} data - The parsed response, once loaded.
 * @property {string | null} error - A description of the failure, if any.
 * @property {boolean} loading - Whether the request is in progress.
 */

/**
 * Fetches and parses a JSON document, refetching when the URL changes.
 *
 * @param {string | null} url - The URL, or null to fetch nothing.
 * @returns {FetchState} The state of the request.
 */
export default function useJson(url) {
  const [state, setState] = useState({
    data: null,
    error: null,
    loading: url !== null,
  });

  useEffect(() => {
    if (url === null) {
      setState({ data: null, error: null, loading: false });
      return undefined;
    }
    let cancelled = false;
    setState({ data: null, error: null, loading: true });
    fetch(url)
      .then((response) => {
        if (!response.ok) {
          throw new Error(`${response.status} ${response.statusText}`);
        }
        return response.json();
      })
      .then(
        (data) => !cancelled && setState({ data, error: null, loading: false }),
        (error) =>
          !cancelled &&
          setState({
            data: null,
            error: `Could not load ${url}: ${error.message}`,
            loading: false,
          }),
      );
    return () => {
      cancelled = true;
    };
  }, [url]);

  return state;
}
