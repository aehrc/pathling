/**
 * Context for managing application settings, including FHIR server configuration.
 * Settings are persisted to localStorage.
 *
 * @author John Grimes
 */

import { createContext, useContext, useState, useCallback, type ReactNode } from "react";

const STORAGE_KEY = "pathling-export-settings";

interface SettingsState {
  fhirBaseUrl: string | null;
}

interface SettingsContextValue extends SettingsState {
  setFhirBaseUrl: (url: string) => void;
  clearSettings: () => void;
}

const SettingsContext = createContext<SettingsContextValue | null>(null);

function loadSettings(): SettingsState {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored) {
      return JSON.parse(stored);
    }
  } catch (e) {
    console.error("Failed to load settings from localStorage:", e);
  }
  return { fhirBaseUrl: null };
}

function saveSettings(settings: SettingsState): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(settings));
  } catch (e) {
    console.error("Failed to save settings to localStorage:", e);
  }
}

export function SettingsProvider({ children }: { children: ReactNode }) {
  const [settings, setSettings] = useState<SettingsState>(loadSettings);

  const setFhirBaseUrl = useCallback((url: string) => {
    const newSettings = { fhirBaseUrl: url };
    setSettings(newSettings);
    saveSettings(newSettings);
  }, []);

  const clearSettings = useCallback(() => {
    const newSettings = { fhirBaseUrl: null };
    setSettings(newSettings);
    localStorage.removeItem(STORAGE_KEY);
  }, []);

  return (
    <SettingsContext.Provider value={{ ...settings, setFhirBaseUrl, clearSettings }}>
      {children}
    </SettingsContext.Provider>
  );
}

export function useSettings(): SettingsContextValue {
  const context = useContext(SettingsContext);
  if (!context) {
    throw new Error("useSettings must be used within a SettingsProvider");
  }
  return context;
}
