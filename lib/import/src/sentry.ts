/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

import * as Sentry from "@sentry/node";
import { Config } from "./config.js";

export function initializeSentry(config: Config): void {
  const dsn = config.getOptionalStringValue("sentryDsn"),
    environment = config.getOptionalStringValue("sentryEnvironment"),
    release = config.getOptionalStringValue("sentryRelease");
  if (dsn) {
    Sentry.init({
      dsn,
      environment: environment ?? undefined,
      release: release ?? undefined,
    });
  }
}
