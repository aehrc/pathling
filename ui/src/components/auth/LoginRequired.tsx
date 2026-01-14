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
 * Component displayed when authentication is required but the user is not logged in.
 *
 * @author John Grimes
 */

import { InfoCircledIcon, LockClosedIcon } from "@radix-ui/react-icons";
import { Box, Button, Callout } from "@radix-ui/themes";

import { SessionExpiredDialog } from "./SessionExpiredDialog";
import { config } from "../../config";
import { useAuth } from "../../contexts/AuthContext";
import { useServerCapabilities } from "../../hooks/useServerCapabilities";
import { initiateAuth } from "../../services/auth";

/**
 * Displays a login prompt when authentication is required.
 *
 * @returns The login required component.
 */
export function LoginRequired() {
  const { fhirBaseUrl } = config;
  const { setLoading, setError } = useAuth();
  const { data: capabilities } = useServerCapabilities(fhirBaseUrl);

  const handleLogin = async () => {
    if (!fhirBaseUrl) return;
    setLoading(true);
    try {
      await initiateAuth(fhirBaseUrl);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Authentication failed");
    }
  };

  return (
    <Box>
      <Callout.Root>
        <Callout.Icon>
          <InfoCircledIcon />
        </Callout.Icon>
        <Callout.Text>You need to login before you can use this page.</Callout.Text>
      </Callout.Root>

      <Box mt="4">
        <Button size="3" onClick={handleLogin}>
          <LockClosedIcon />
          Login to {capabilities?.serverName ?? window.location.hostname}
        </Button>
      </Box>
      <SessionExpiredDialog />
    </Box>
  );
}
