/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.compartment;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import au.csiro.pathling.library.io.source.QueryableDataSource;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import jakarta.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Service for extracting patient IDs from FHIR Group resources.
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class GroupMemberService {

  private static final String PATIENT_REFERENCE_PREFIX = "Patient/";

  @Nonnull private final QueryableDataSource deltaLake;

  /**
   * Constructs a new GroupMemberService.
   *
   * @param deltaLake the queryable data source
   */
  @Autowired
  public GroupMemberService(@Nonnull final QueryableDataSource deltaLake) {
    this.deltaLake = deltaLake;
  }

  /**
   * Extracts patient IDs from a Group resource's member references.
   *
   * @param groupId the group ID
   * @return set of patient IDs referenced by the group's members
   * @throws ResourceNotFoundException if the group does not exist
   */
  @Nonnull
  public Set<String> extractPatientIdsFromGroup(@Nonnull final String groupId) {
    final Dataset<Row> groupDataset = deltaLake.read("Group");
    final Dataset<Row> filtered = groupDataset.filter(col("id").equalTo(groupId));

    if (filtered.isEmpty()) {
      throw new ResourceNotFoundException("Group/" + groupId);
    }

    final Dataset<Row> memberRefs =
        filtered
            .select(explode(col("member")).as("member"))
            .select(col("member.entity.reference").as("reference"));

    final Set<String> patientIds = new HashSet<>();
    for (final Row row : memberRefs.collectAsList()) {
      final String reference = row.getString(0);
      if (reference != null && reference.startsWith(PATIENT_REFERENCE_PREFIX)) {
        patientIds.add(reference.substring(PATIENT_REFERENCE_PREFIX.length()));
      }
    }

    log.debug("Extracted {} patient IDs from Group/{}", patientIds.size(), groupId);
    return patientIds;
  }
}
