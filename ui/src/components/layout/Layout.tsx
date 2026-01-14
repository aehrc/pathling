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
 * Main layout component with header and content area.
 *
 * @author John Grimes
 */

import {
  DownloadIcon,
  HamburgerMenuIcon,
  PaperPlaneIcon,
  ReaderIcon,
  TableIcon,
  UploadIcon,
} from "@radix-ui/react-icons";
import { Box, Container, DropdownMenu, Flex, IconButton, Text } from "@radix-ui/themes";
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

interface NavItem {
  to: string;
  icon: React.ReactNode;
  label: string;
}

const navItems: NavItem[] = [
  { to: "/resources", icon: <ReaderIcon />, label: "Resources" },
  { to: "/export", icon: <DownloadIcon />, label: "Export" },
  { to: "/import", icon: <UploadIcon />, label: "Import" },
  { to: "/bulk-submit", icon: <PaperPlaneIcon />, label: "Bulk submit" },
  { to: "/sql-on-fhir", icon: <TableIcon />, label: "SQL on FHIR" },
];

interface MobileNavProps {
  pathname: string;
  hostname: string;
  isAuthenticated: boolean;
  logout: () => void;
}

function MobileNav({ pathname, hostname, isAuthenticated, logout }: MobileNavProps) {
  return (
    <DropdownMenu.Root>
      <DropdownMenu.Trigger>
        <IconButton variant="ghost" size="3">
          <HamburgerMenuIcon width="20" height="20" />
        </IconButton>
      </DropdownMenu.Trigger>
      <DropdownMenu.Content align="end">
        {navItems.map((item) => (
          <DropdownMenu.Item key={item.to} asChild>
            <Link
              to={item.to}
              style={{
                textDecoration: "none",
                color: pathname === item.to ? "var(--accent-11)" : "inherit",
                display: "flex",
                alignItems: "center",
                gap: "8px",
              }}
            >
              {item.icon}
              {item.label}
            </Link>
          </DropdownMenu.Item>
        ))}
        <DropdownMenu.Separator />
        <DropdownMenu.Item disabled>
          <Text size="2" color="gray">
            {hostname}
          </Text>
        </DropdownMenu.Item>
        {isAuthenticated && (
          <DropdownMenu.Item onSelect={logout}>
            <Text size="2" color="blue">
              Logout
            </Text>
          </DropdownMenu.Item>
        )}
      </DropdownMenu.Content>
    </DropdownMenu.Root>
  );
}

function getHostname(url: string): string {
  try {
    return new URL(url, window.location.origin).hostname;
  } catch {
    return url;
  }
}

/**
 * Main application layout with navigation header and content area.
 *
 * @returns The layout component with header and outlet.
 */
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
        <Container size="4" px={{ initial: "4", lg: "0" }}>
          <Flex justify="between" align="center" py="3">
            <Flex align="center" gap="5">
              <Link to="/">
                <picture>
                  <source
                    srcSet="/admin/logo-colour-dark.svg"
                    media="(prefers-color-scheme: dark)"
                  />
                  <img
                    src="/admin/logo-colour.svg"
                    alt="Pathling"
                    height={50}
                    style={{ display: "block" }}
                  />
                </picture>
              </Link>
              <Flex
                gap="5"
                ml="8"
                display={{ initial: "none", md: "flex" }}
                style={{ position: "relative", top: "10px" }}
              >
                {navItems.map((item) => (
                  <NavLink
                    key={item.to}
                    to={item.to}
                    icon={item.icon}
                    label={item.label}
                    isActive={location.pathname === item.to}
                  />
                ))}
              </Flex>
            </Flex>
            <Flex
              align="center"
              gap="5"
              display={{ initial: "none", md: "flex" }}
              style={{ position: "relative", top: "10px" }}
            >
              <Text size="2" color="gray">
                {getHostname(config.fhirBaseUrl)}
              </Text>
              {isAuthenticated && (
                <Text size="2" color="teal" style={{ cursor: "pointer" }} onClick={logout}>
                  Logout
                </Text>
              )}
            </Flex>
            <Box display={{ initial: "block", md: "none" }}>
              <MobileNav
                pathname={location.pathname}
                hostname={getHostname(config.fhirBaseUrl)}
                isAuthenticated={isAuthenticated}
                logout={logout}
              />
            </Box>
          </Flex>
        </Container>
      </Box>
      <Box style={{ flex: 1, overflow: "hidden" }}>
        <Container size="4" py="6" px={{ initial: "4", lg: "0" }}>
          <Outlet />
        </Container>
      </Box>
    </Box>
  );
}
