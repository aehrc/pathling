/**
 * OAuth callback page for completing SMART on FHIR authentication.
 *
 * @author John Grimes
 */

import { CrossCircledIcon } from "@radix-ui/react-icons";
import { Box, Callout, Flex, Spinner, Text } from "@radix-ui/themes";
import { useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router";

import { useAuth } from "../contexts/AuthContext";
import { clearReturnUrl, completeAuth, getReturnUrl } from "../services/auth";

/**
 * Handles the OAuth callback and completes authentication.
 *
 * @returns The callback page component.
 */
export function Callback() {
  const navigate = useNavigate();
  const { setClient, setError } = useAuth();
  const [localError, setLocalError] = useState<string | null>(null);

  // Track whether callback has been processed to prevent double-execution.
  const hasProcessed = useRef(false);

  useEffect(() => {
    async function handleCallback() {
      if (hasProcessed.current) return;
      hasProcessed.current = true;

      try {
        const client = await completeAuth();
        setClient(client);
        // Get return URL before clearing - safe to call multiple times.
        const returnUrl = getReturnUrl();
        clearReturnUrl();
        navigate(returnUrl, { replace: true });
      } catch (err) {
        const message = err instanceof Error ? err.message : "Authentication failed";
        setLocalError(message);
        setError(message);
      }
    }

    handleCallback();
  }, [navigate, setClient, setError]);

  if (localError) {
    return (
      <Box p="6">
        <Callout.Root color="red">
          <Callout.Icon>
            <CrossCircledIcon />
          </Callout.Icon>
          <Callout.Text>
            <Text weight="bold">Authentication Failed</Text>
            <br />
            {localError}
          </Callout.Text>
        </Callout.Root>
      </Box>
    );
  }

  return (
    <Flex align="center" justify="center" style={{ minHeight: "100vh" }} direction="column" gap="4">
      <Spinner size="3" />
      <Text color="gray">Completing authentication...</Text>
    </Flex>
  );
}
