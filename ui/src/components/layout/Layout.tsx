/**
 * Main layout component with header and content area.
 *
 * @author John Grimes
 */

import { Outlet, Link, useLocation } from "react-router";
import { Box, Container, Flex, Heading, Text } from "@radix-ui/themes";
import { DownloadIcon, GearIcon, HomeIcon, UploadIcon } from "@radix-ui/react-icons";
import { useSettings } from "../../contexts/SettingsContext";
import { useAuth } from "../../contexts/AuthContext";

interface NavLinkProps {
  to: string;
  icon: React.ReactNode;
  label: string;
  isActive: boolean;
}

function NavLink({ to, icon, label, isActive }: NavLinkProps) {
  return (
    <Link
      to={to}
      style={{
        textDecoration: "none",
        color: isActive ? "var(--accent-11)" : "var(--gray-11)",
        display: "flex",
        alignItems: "center",
        gap: "4px",
      }}
    >
      {icon}
      {label}
    </Link>
  );
}

export function Layout() {
  const location = useLocation();
  const { fhirBaseUrl } = useSettings();
  const { isAuthenticated, logout } = useAuth();

  return (
    <Box style={{ minHeight: "100vh", display: "flex", flexDirection: "column" }}>
      <Box
        style={{
          borderBottom: "1px solid var(--gray-5)",
          backgroundColor: "var(--color-background)",
        }}
      >
        <Container size="4">
          <Flex justify="between" align="center" py="3">
            <Flex align="center" gap="4">
              <Heading size="5" weight="bold">
                Pathling
              </Heading>
              <Flex gap="4" ml="6">
                <NavLink
                  to="/"
                  icon={<HomeIcon />}
                  label="Dashboard"
                  isActive={location.pathname === "/"}
                />
                <NavLink
                  to="/export"
                  icon={<DownloadIcon />}
                  label="Export"
                  isActive={location.pathname === "/export"}
                />
                <NavLink
                  to="/import"
                  icon={<UploadIcon />}
                  label="Import"
                  isActive={location.pathname === "/import"}
                />
                <NavLink
                  to="/settings"
                  icon={<GearIcon />}
                  label="Settings"
                  isActive={location.pathname === "/settings"}
                />
              </Flex>
            </Flex>
            <Flex align="center" gap="4">
              {fhirBaseUrl && (
                <Text size="2" color="gray">
                  {new URL(fhirBaseUrl).hostname}
                </Text>
              )}
              {isAuthenticated && (
                <Text size="2" color="blue" style={{ cursor: "pointer" }} onClick={logout}>
                  Logout
                </Text>
              )}
            </Flex>
          </Flex>
        </Container>
      </Box>
      <Box style={{ flex: 1 }}>
        <Container size="4" py="6">
          <Outlet />
        </Container>
      </Box>
    </Box>
  );
}
