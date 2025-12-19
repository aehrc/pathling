/**
 * Main layout component with header and content area.
 *
 * @author John Grimes
 */

import { DownloadIcon, HomeIcon, PaperPlaneIcon, ReaderIcon, UploadIcon } from "@radix-ui/react-icons";
import { Box, Container, Flex, Text } from "@radix-ui/themes";
import { Link, Outlet, useLocation } from "react-router";
import { config } from "../../config";
import { useAuth } from "../../contexts/AuthContext";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";

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

function getHostname(url: string): string {
  try {
    return new URL(url, window.location.origin).hostname;
  } catch {
    return url;
  }
}

export function Layout() {
  const location = useLocation();
  const { isAuthenticated, logout } = useAuth();
  useDocumentTitle();

  return (
    <Box style={{ minHeight: "100vh", display: "flex", flexDirection: "column" }}>
      <Box
        style={{
          borderBottom: "1px solid var(--gray-5)",
          backgroundColor: "var(--color-background)",
        }}
      >
        <Container size="4" px={{ initial: "4", md: "0" }}>
          <Flex justify="between" align="center" py="3">
            <Flex align="center" gap="5">
              <picture>
                <source srcSet="/admin/logo-colour-dark.svg" media="(prefers-color-scheme: dark)" />
                <img
                  src="/admin/logo-colour.svg"
                  alt="Pathling"
                  height={50}
                  style={{ display: "block" }}
                />
              </picture>
              <Flex gap="5" ml="8" style={{ position: "relative", top: "10px" }}>
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
                  to="/bulk-submit"
                  icon={<PaperPlaneIcon />}
                  label="Bulk submit"
                  isActive={location.pathname === "/bulk-submit"}
                />
                <NavLink
                  to="/resources"
                  icon={<ReaderIcon />}
                  label="Resources"
                  isActive={location.pathname === "/resources"}
                />
              </Flex>
            </Flex>
            <Flex align="center" gap="5" style={{ position: "relative", top: "10px" }}>
              <Text size="2" color="gray">
                {getHostname(config.fhirBaseUrl)}
              </Text>
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
        <Container size="4" py="6" px={{ initial: "4", md: "0" }}>
          <Outlet />
        </Container>
      </Box>
    </Box>
  );
}
