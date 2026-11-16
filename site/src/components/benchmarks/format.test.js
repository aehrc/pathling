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
 * Tests for the labels that identify benchmark runs on the page.
 *
 * @author John Grimes
 */

import { describe, expect, test } from "bun:test";
import { runLabel, runName } from "./format.js";

const RELEASE = {
  date: "2026-10-21T04:27:47Z",
  commit: "2e654ed5f4a3b2c1d0e9f8a7b6c5d4e3f2a1b0c9",
  version: "10.0.0",
};

const SNAPSHOT = {
  date: "2026-11-15T06:52:38Z",
  commit: "2c5e82f1eb0000000000000000000000000000aa",
  version: "11.0.0-SNAPSHOT",
};

// Runs published before the version was recorded have no version.
const UNVERSIONED = {
  date: "2026-11-13T21:13:09Z",
  commit: "3bdc1aa0abc6153b901b3f1e7a9479fdcd801aef",
};

describe("runLabel", () => {
  test("labels a release run with its tag name, date and commit", () => {
    expect(runLabel(RELEASE)).toBe("v10.0.0, 2026-10-21 (2e654ed)");
  });

  test("labels a development run with its version as is", () => {
    // A snapshot has no tag, so it is not given the "v" prefix.
    expect(runLabel(SNAPSHOT)).toBe("11.0.0-SNAPSHOT, 2026-11-15 (2c5e82f)");
  });

  test("labels a run without a version by its date and commit", () => {
    expect(runLabel(UNVERSIONED)).toBe("2026-11-13 (3bdc1aa)");
  });

  test("uses the date in UTC", () => {
    // In any time zone east of UTC, the local date of this run is 3 June.
    expect(runLabel({ ...RELEASE, date: "2026-06-02T23:59:59Z" })).toBe(
      "v10.0.0, 2026-06-02 (2e654ed)",
    );
  });
});

describe("runName", () => {
  test("names a release run with its tag name and time", () => {
    expect(runName(RELEASE)).toBe("v10.0.0, 2026-10-21 04:27 UTC");
  });

  test("names a development run with its version as is", () => {
    expect(runName(SNAPSHOT)).toBe("11.0.0-SNAPSHOT, 2026-11-15 06:52 UTC");
  });

  test("names a run without a version by its time", () => {
    expect(runName(UNVERSIONED)).toBe("2026-11-13 21:13 UTC");
  });
});
