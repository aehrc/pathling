/**
 * OAuth callback page for completing SMART on FHIR authentication.
 *
 * @author John Grimes
 */

import { CrossCircledIcon } from "@radix-ui/react-icons";
import { Box, Callout, Flex, Spinner, Text } from "@radix-ui/themes";
import { useEffect, useState } from "react";
import { useNavigate } from "react-router";
import { useAuth } from "../contexts/AuthContext";
import { completeAuth } from "../services/auth";

export function Callback() {
  const navigate = useNavigate();
  const { setClient, setError } = useAuth();
  const [error, setLocalError] = useState<string | null>(null);

  useEffect(() => {
    async function handleCallback() {
      try {
        const client = await completeAuth();
        setClient(client);
        navigate("/", { replace: true });
      } catch (err) {
        const message = err instanceof Error ? err.message : "Authentication failed";
        setLocalError(message);
        setError(message);
      }
    }

    handleCallback();
  }, [navigate, setClient, setError]);

  if (error) {
    return (
      <Box p="6">
        <Callout.Root color="red">
          <Callout.Icon>
            <CrossCircledIcon />
          </Callout.Icon>
          <Callout.Text>
            <Text weight="bold">Authentication Failed</Text>
            <br />
            {error}
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
