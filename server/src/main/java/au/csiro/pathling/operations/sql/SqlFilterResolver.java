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

package au.csiro.pathling.operations.sql;

import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.compartment.GroupMemberService;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.Reference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Resolves the {@code patient}, {@code group} and {@code _since} filters shared by {@code $sql-run}
 * and {@code $sql-export} into the set of patient logical ids the request restricts to.
 *
 * <p>Unresolvable {@code patient} and {@code group} values are reported rather than silently
 * dropped: quietly ignoring an id that names nothing would return a narrower result set than the
 * client asked for, with no indication that anything was wrong. Issues are collected rather than
 * thrown so several bad values, and a bad subject alongside them, can be reported in one outcome.
 *
 * @author John Grimes
 */
@Component
public class SqlFilterResolver {

  /** The {@code expression} value naming the patient filter in error outcomes. */
  public static final String PATIENT_EXPRESSION = "patient";

  /** The {@code expression} value naming the group filter in error outcomes. */
  public static final String GROUP_EXPRESSION = "group";

  private static final String PATIENT = "Patient";

  private static final String GROUP = "Group";

  @Nonnull private final QueryableDataSource deltaLake;

  @Nonnull private final GroupMemberService groupMemberService;

  /**
   * Constructs a new SqlFilterResolver.
   *
   * @param deltaLake the data source used to confirm that a named patient exists
   * @param groupMemberService resolves a {@code group} to the patient ids of its members
   */
  @Autowired
  public SqlFilterResolver(
      @Nonnull final QueryableDataSource deltaLake,
      @Nonnull final GroupMemberService groupMemberService) {
    this.deltaLake = deltaLake;
    this.groupMemberService = groupMemberService;
  }

  /**
   * Resolves the filter parameters, collecting an issue for each value that names nothing.
   *
   * @param patients the {@code patient} references, if any
   * @param groups the {@code group} references, if any
   * @param since the {@code _since} filter, if any
   * @return the resolved filters and any issues raised
   */
  @Nonnull
  public ResolvedFilters resolve(
      @Nullable final List<Reference> patients,
      @Nullable final List<Reference> groups,
      @Nullable final InstantType since) {

    final List<OperationOutcomeIssueComponent> issues = new ArrayList<>();
    final Set<String> patientIds = new LinkedHashSet<>();

    patientIds.addAll(resolvePatients(patients, issues));
    patientIds.addAll(resolveGroups(groups, issues));

    return new ResolvedFilters(patientIds, since, List.copyOf(issues));
  }

  /** Resolves the {@code patient} references, reporting each id that names no stored Patient. */
  @Nonnull
  private Set<String> resolvePatients(
      @Nullable final List<Reference> patients,
      @Nonnull final List<OperationOutcomeIssueComponent> issues) {
    if (patients == null || patients.isEmpty()) {
      return Set.of();
    }
    final Set<String> requested = new LinkedHashSet<>();
    for (final Reference patient : patients) {
      final String id = logicalId(patient, PATIENT);
      if (id == null) {
        issues.add(
            SqlOperationError.issue(
                IssueType.NOTFOUND,
                PATIENT_EXPRESSION,
                "The 'patient' value '%s' is not a reference to a Patient."
                    .formatted(patient.getReference())));
      } else {
        requested.add(id);
      }
    }
    if (requested.isEmpty()) {
      return Set.of();
    }

    final Set<String> existing = existingIds(PATIENT, requested);
    for (final String id : requested) {
      if (!existing.contains(id)) {
        issues.add(
            SqlOperationError.issue(
                IssueType.NOTFOUND,
                PATIENT_EXPRESSION,
                "No Patient with id '%s' was found.".formatted(id)));
      }
    }
    return existing;
  }

  /**
   * Resolves the {@code group} references to their member patient ids, reporting each group that
   * names nothing. A group with no Patient members contributes no ids but is not an error, since
   * the group itself resolved.
   */
  @Nonnull
  private Set<String> resolveGroups(
      @Nullable final List<Reference> groups,
      @Nonnull final List<OperationOutcomeIssueComponent> issues) {
    if (groups == null || groups.isEmpty()) {
      return Set.of();
    }
    final Set<String> memberIds = new LinkedHashSet<>();
    for (final Reference group : groups) {
      final String id = logicalId(group, GROUP);
      if (id == null) {
        issues.add(
            SqlOperationError.issue(
                IssueType.NOTFOUND,
                GROUP_EXPRESSION,
                "The 'group' value '%s' is not a reference to a Group."
                    .formatted(group.getReference())));
        continue;
      }
      try {
        memberIds.addAll(groupMemberService.extractPatientIdsFromGroup(id));
      } catch (final RuntimeException e) {
        // The group could not be read, which for the filter parameters is a client error naming
        // the parameter rather than a 404 for the request as a whole.
        issues.add(
            SqlOperationError.issue(
                IssueType.NOTFOUND,
                GROUP_EXPRESSION,
                "No Group with id '%s' was found.".formatted(id)));
      }
    }
    return memberIds;
  }

  /**
   * Returns the subset of the requested ids that exist for the given resource type. A type with no
   * stored data matches nothing, rather than surfacing the data source's missing-type error.
   */
  @Nonnull
  private Set<String> existingIds(
      @Nonnull final String resourceType, @Nonnull final Set<String> requestedIds) {
    final Dataset<Row> all;
    try {
      all = deltaLake.read(resourceType);
    } catch (final IllegalArgumentException e) {
      if (e.getMessage() != null && e.getMessage().contains("No data found for resource type")) {
        return Set.of();
      }
      throw e;
    }
    final Dataset<Row> matched =
        all.filter(functions.col("id").isin(requestedIds.toArray())).select("id").distinct();
    final Set<String> found = new LinkedHashSet<>();
    for (final Row row : matched.collectAsList()) {
      found.add(row.getString(0));
    }
    return found;
  }

  /**
   * Extracts the logical id from a reference to the expected resource type, accepting both a typed
   * relative reference and a bare id. Returns null when the reference names a different type or
   * carries no value.
   */
  @Nullable
  private static String logicalId(
      @Nullable final Reference reference, @Nonnull final String expectedType) {
    if (reference == null) {
      return null;
    }
    final String value = reference.getReference();
    if (value == null || value.isBlank()) {
      return null;
    }
    final int slash = value.lastIndexOf('/');
    if (slash < 0) {
      return value;
    }
    if (!value.substring(0, slash).equals(expectedType)) {
      return null;
    }
    final String id = value.substring(slash + 1);
    return id.isBlank() ? null : id;
  }
}
