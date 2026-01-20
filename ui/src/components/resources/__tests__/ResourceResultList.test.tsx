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
 * Tests for the ResourceResultList component.
 *
 * This test suite verifies that the ResourceResultList component correctly
 * handles different states: initial (before search), loading, error, empty
 * results, and successful results with resources.
 *
 * @author John Grimes
 */

import { beforeEach, describe, expect, it, vi } from "vitest";

import { render, screen } from "../../../test/testUtils";
import { ResourceResultList } from "../ResourceResultList";

import type { Resource } from "fhir/r4";

// Mock the ResourceCard component to simplify testing.
vi.mock("../ResourceCard", () => ({
  ResourceCard: ({ resource }: { resource: Resource }) => (
    <div data-testid="resource-card">
      {resource.resourceType}/{resource.id}
    </div>
  ),
}));

describe("ResourceResultList", () => {
  const defaultFhirBaseUrl = "https://fhir.example.org/fhir";
  const defaultOnDelete = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("Initial state", () => {
    it("displays prompt to search when hasSearched is false", () => {
      render(
        <ResourceResultList
          resources={undefined}
          total={undefined}
          isLoading={false}
          error={null}
          hasSearched={false}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("Results")).toBeInTheDocument();
      expect(
        screen.getByText("Select a resource type and search to view results."),
      ).toBeInTheDocument();
    });
  });

  describe("Loading state", () => {
    it("displays loading spinner when isLoading is true", () => {
      render(
        <ResourceResultList
          resources={undefined}
          total={undefined}
          isLoading={true}
          error={null}
          hasSearched={true}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("Results")).toBeInTheDocument();
      expect(screen.getByText("Searching...")).toBeInTheDocument();
    });
  });

  describe("Error state", () => {
    it("displays error message when error is present", () => {
      const testError = new Error("Failed to fetch resources");

      render(
        <ResourceResultList
          resources={undefined}
          total={undefined}
          isLoading={false}
          error={testError}
          hasSearched={true}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("Results")).toBeInTheDocument();
      expect(screen.getByText("Failed to fetch resources")).toBeInTheDocument();
    });
  });

  describe("Empty results", () => {
    it("displays no results message when resources array is empty", () => {
      render(
        <ResourceResultList
          resources={[]}
          total={0}
          isLoading={false}
          error={null}
          hasSearched={true}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("Results")).toBeInTheDocument();
      expect(screen.getByText("No resources found matching your criteria.")).toBeInTheDocument();
    });

    it("displays no results message when resources is undefined after search", () => {
      render(
        <ResourceResultList
          resources={undefined}
          total={undefined}
          isLoading={false}
          error={null}
          hasSearched={true}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("No resources found matching your criteria.")).toBeInTheDocument();
    });
  });

  describe("Results display", () => {
    it("displays resource cards when resources are present", () => {
      const resources: Resource[] = [
        { resourceType: "Patient", id: "patient-1" },
        { resourceType: "Observation", id: "obs-1" },
      ];

      render(
        <ResourceResultList
          resources={resources}
          total={2}
          isLoading={false}
          error={null}
          hasSearched={true}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("Results")).toBeInTheDocument();
      expect(screen.getAllByTestId("resource-card")).toHaveLength(2);
      expect(screen.getByText("Patient/patient-1")).toBeInTheDocument();
      expect(screen.getByText("Observation/obs-1")).toBeInTheDocument();
    });

    it("displays total count badge with first N shown message", () => {
      const resources: Resource[] = [
        { resourceType: "Patient", id: "patient-1" },
        { resourceType: "Patient", id: "patient-2" },
      ];

      render(
        <ResourceResultList
          resources={resources}
          total={100}
          isLoading={false}
          error={null}
          hasSearched={true}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("100 total, first 2 shown")).toBeInTheDocument();
    });

    it("displays only shown count when total is undefined", () => {
      const resources: Resource[] = [
        { resourceType: "Patient", id: "patient-1" },
        { resourceType: "Patient", id: "patient-2" },
        { resourceType: "Patient", id: "patient-3" },
      ];

      render(
        <ResourceResultList
          resources={resources}
          total={undefined}
          isLoading={false}
          error={null}
          hasSearched={true}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("3 shown")).toBeInTheDocument();
    });
  });

  describe("State priority", () => {
    // These tests verify that the component renders states in the correct
    // priority order: !hasSearched > isLoading > error > empty/results.

    it("shows initial state even when loading is true but hasSearched is false", () => {
      render(
        <ResourceResultList
          resources={undefined}
          total={undefined}
          isLoading={true}
          error={null}
          hasSearched={false}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(
        screen.getByText("Select a resource type and search to view results."),
      ).toBeInTheDocument();
      expect(screen.queryByText("Searching...")).not.toBeInTheDocument();
    });

    it("shows loading state before error when both are set", () => {
      render(
        <ResourceResultList
          resources={undefined}
          total={undefined}
          isLoading={true}
          error={new Error("Some error")}
          hasSearched={true}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("Searching...")).toBeInTheDocument();
      expect(screen.queryByText("Some error")).not.toBeInTheDocument();
    });

    it("shows error state before empty results", () => {
      render(
        <ResourceResultList
          resources={[]}
          total={0}
          isLoading={false}
          error={new Error("Error takes priority")}
          hasSearched={true}
          fhirBaseUrl={defaultFhirBaseUrl}
          onDelete={defaultOnDelete}
        />,
      );

      expect(screen.getByText("Error takes priority")).toBeInTheDocument();
      expect(
        screen.queryByText("No resources found matching your criteria."),
      ).not.toBeInTheDocument();
    });
  });
});
