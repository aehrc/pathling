/**
 * Settings page for configuring the FHIR server endpoint.
 *
 * @author John Grimes
 */

import { useState } from "react";
import { useNavigate } from "react-router";
import { Box, Button, Card, Flex, Heading, Text, TextField, Callout } from "@radix-ui/themes";
import { InfoCircledIcon, CheckCircledIcon } from "@radix-ui/react-icons";
import { useSettings } from "../contexts/SettingsContext";
import { useAuth } from "../contexts/AuthContext";
import { checkServerCapabilities } from "../services/auth";

export function Settings() {
  const navigate = useNavigate();
  const { fhirBaseUrl, setFhirBaseUrl, clearSettings } = useSettings();
  const { isAuthenticated, logout } = useAuth();
  const [url, setUrl] = useState(fhirBaseUrl || "");
  const [isValidating, setIsValidating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  const handleSave = async () => {
    if (!url.trim()) {
      setError("Please enter a FHIR server URL");
      return;
    }

    // Normalise URL - remove trailing slash.
    let normalizedUrl = url.trim();
    if (normalizedUrl.endsWith("/")) {
      normalizedUrl = normalizedUrl.slice(0, -1);
    }

    setIsValidating(true);
    setError(null);
    setSuccess(false);

    try {
      // Check if the server is reachable by fetching its capabilities.
      await checkServerCapabilities(normalizedUrl);
      setFhirBaseUrl(normalizedUrl);
      setSuccess(true);
      // If changing the endpoint, log out.
      if (isAuthenticated && normalizedUrl !== fhirBaseUrl) {
        logout();
      }
      // Navigate to dashboard after a brief delay.
      setTimeout(() => navigate("/"), 1000);
    } catch (err) {
      setError(
        `Failed to validate FHIR server: ${err instanceof Error ? err.message : "Unknown error"}`,
      );
    } finally {
      setIsValidating(false);
    }
  };

  const handleClear = () => {
    clearSettings();
    logout();
    setUrl("");
    setSuccess(false);
    setError(null);
  };

  return (
    <Box>
      <Heading size="6" mb="4">
        Settings
      </Heading>

      <Card style={{ maxWidth: 600 }}>
        <Flex direction="column" gap="4">
          <Box>
            <Text as="label" size="2" weight="medium" mb="1">
              FHIR Server URL
            </Text>
            <TextField.Root
              size="3"
              placeholder="https://demo.pathling.app/fhir"
              value={url}
              onChange={(e) => {
                setUrl(e.target.value);
                setError(null);
                setSuccess(false);
              }}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  handleSave();
                }
              }}
            />
            <Text size="1" color="gray" mt="1">
              Enter the base URL of a FHIR server that supports the bulk export operation.
            </Text>
          </Box>

          {error && (
            <Callout.Root color="red">
              <Callout.Icon>
                <InfoCircledIcon />
              </Callout.Icon>
              <Callout.Text>{error}</Callout.Text>
            </Callout.Root>
          )}

          {success && (
            <Callout.Root color="green">
              <Callout.Icon>
                <CheckCircledIcon />
              </Callout.Icon>
              <Callout.Text>
                FHIR server configured successfully. Redirecting to dashboard...
              </Callout.Text>
            </Callout.Root>
          )}

          <Flex gap="3">
            <Button onClick={handleSave} disabled={isValidating}>
              {isValidating ? "Validating..." : "Save"}
            </Button>
            {fhirBaseUrl && (
              <Button variant="soft" color="gray" onClick={handleClear}>
                Clear
              </Button>
            )}
          </Flex>
        </Flex>
      </Card>
    </Box>
  );
}
