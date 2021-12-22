/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

import { Config } from "./config";
import * as Sentry from "@sentry/node";

export function initializeSentry(config: Config): void {
  const dsn = config.getStringValue("sentryDsn", true),
    environment = config.getStringValue("sentryEnvironment", true),
    release = config.getStringValue("sentryRelease", true);
  if (dsn) {
    Sentry.init({
      dsn,
      environment: environment ?? undefined,
      release: release ?? undefined,
    });
  }
}
