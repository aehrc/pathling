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
 * Dialog shown when the user's session has expired.
 * Prompts the user to log in again.
 *
 * @author John Grimes
 */

import { LockClosedIcon } from "@radix-ui/react-icons";
import { AlertDialog, Box, Button, Flex, Spinner } from "@radix-ui/themes";

import { useAuth } from "../../contexts/AuthContext";
import { useLogin } from "../../hooks/useLogin";
import { ErrorCallout } from "../error/ErrorCallout";

/**
 * Dialog prompting the user to re-authenticate after session expiry.
 *
 * The dialog stays open while an authorisation attempt is under way, and stays
 * open when that attempt fails, so the user sees the outcome where they asked
 * for it. The login control is therefore a plain button rather than an
 * `AlertDialog.Action`, which would close the dialog on activation.
 *
 * @returns The session expired dialog component.
 */
export function SessionExpiredDialog() {
  const { sessionExpired, setSessionExpired } = useAuth();
  const { login, isPending, error } = useLogin();

  const handleDismiss = () => {
    setSessionExpired(false);
  };

  return (
    <AlertDialog.Root open={sessionExpired} onOpenChange={setSessionExpired}>
      <AlertDialog.Content maxWidth="450px">
        <AlertDialog.Title>Session expired</AlertDialog.Title>
        <AlertDialog.Description size="2">
          Your session has expired. Please log in again to continue working.
        </AlertDialog.Description>
        {error && (
          <Box mt="3">
            <ErrorCallout message={error} size="1" />
          </Box>
        )}
        <Flex gap="3" mt="4" justify="end">
          <AlertDialog.Cancel>
            <Button variant="soft" color="gray" disabled={isPending} onClick={handleDismiss}>
              Dismiss
            </Button>
          </AlertDialog.Cancel>
          {/* The spinner replaces the icon rather than the whole label, so the
              button still says what it does while the attempt is under way. */}
          <Button disabled={isPending} onClick={() => void login()}>
            <Spinner loading={isPending}>
              <LockClosedIcon />
            </Spinner>
            Log in
          </Button>
        </Flex>
      </AlertDialog.Content>
    </AlertDialog.Root>
  );
}
