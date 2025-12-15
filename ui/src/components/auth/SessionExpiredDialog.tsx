/**
 * Dialog shown when the user's session has expired.
 * Prompts the user to log in again.
 *
 * @author John Grimes
 */

import { LockClosedIcon } from "@radix-ui/react-icons";
import { AlertDialog, Button, Flex } from "@radix-ui/themes";
import { config } from "../../config";
import { useAuth } from "../../contexts/AuthContext";
import { initiateAuth } from "../../services/auth";

export function SessionExpiredDialog() {
  const { sessionExpired, setSessionExpired, setLoading, setError } = useAuth();
  const { fhirBaseUrl } = config;

  const handleLogin = async () => {
    if (!fhirBaseUrl) return;
    setSessionExpired(false);
    setLoading(true);
    try {
      await initiateAuth(fhirBaseUrl);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Authentication failed");
    }
  };

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
        <Flex gap="3" mt="4" justify="end">
          <AlertDialog.Cancel>
            <Button variant="soft" color="gray" onClick={handleDismiss}>
              Dismiss
            </Button>
          </AlertDialog.Cancel>
          <AlertDialog.Action>
            <Button onClick={handleLogin}>
              <LockClosedIcon />
              Log in
            </Button>
          </AlertDialog.Action>
        </Flex>
      </AlertDialog.Content>
    </AlertDialog.Root>
  );
}
