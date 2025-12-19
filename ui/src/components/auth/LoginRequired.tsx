/**
 * Component displayed when authentication is required but the user is not logged in.
 *
 * @author John Grimes
 */

import { InfoCircledIcon, LockClosedIcon } from "@radix-ui/react-icons";
import { Box, Button, Callout } from "@radix-ui/themes";
import { config } from "../../config";
import { useAuth } from "../../contexts/AuthContext";
import { useServerCapabilities } from "../../hooks/useServerCapabilities";
import { initiateAuth } from "../../services/auth";
import { SessionExpiredDialog } from "./SessionExpiredDialog";

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
