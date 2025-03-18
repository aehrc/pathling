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

package au.csiro.pathling.fhirpath.execution;

import jakarta.annotation.Nonnull;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents a tag used to identify and name columns in datasets during join operations.
 * <p>
 * Join tags are used to create consistent naming conventions for columns that represent
 * joined resources in Spark datasets. The tag format typically includes the resource type
 * and join type, separated by an '@' character.
 * <p>
 * There are three main types of join tags:
 * <ul>
 *   <li>ResourceTag: For direct resource columns</li>
 *   <li>ResolveTag: For forward reference resolution (e.g., Patient.managingOrganization.resolve())</li>
 *   <li>ReverseResolveTag: For reverse reference resolution (e.g., Patient.reverseResolve(Condition.subject))</li>
 * </ul>
 */
public interface JoinTag {

  /**
   * Gets the string representation of this join tag.
   * <p>
   * This string is used as the column name in Spark datasets to identify joined resources.
   * The format varies depending on the type of join:
   * <ul>
   *   <li>ResourceTag: The resource type code (e.g., "Patient")</li>
   *   <li>ResolveTag: "id@{resourceType}" (e.g., "id@Organization")</li>
   *   <li>ReverseResolveTag: "{resourceType}@{path}" (e.g., "Condition@subject")</li>
   * </ul>
   *
   * @return The string representation of this join tag
   */
  @Nonnull
  String getTag();
  
  /**
   * Determines if a column name represents a join tag.
   * <p>
   * Join tags are identified by the presence of the '@' character in the column name.
   * This method is used to identify columns that contain joined resource data.
   *
   * @param name The column name to check
   * @return true if the name represents a join tag, false otherwise
   */
  static boolean isJoinTag(@Nonnull final String name) {
    return name.contains("@");
  }
  
  /**
   * Represents a tag for a direct resource column.
   * <p>
   * This tag is used for columns that contain the primary resource data,
   * not joined resources.
   */
  @Value
  class ResourceTag implements JoinTag {

    /**
     * The type of the resource.
     */
    @Nonnull
    ResourceType resourceType;

    /**
     * {@inheritDoc}
     * <p>
     * For a ResourceTag, the tag is simply the resource type code.
     */
    @Override
    @Nonnull
    public String getTag() {
      return resourceType.toCode();
    }
  }

  /**
   * Represents a tag for a forward resolve join.
   * <p>
   * This tag is used for columns that contain resources referenced by the subject resource.
   * For example, in Patient.managingOrganization.resolve(), this tag would represent the
   * Organization resources referenced by the Patient resources.
   */
  @Value(staticConstructor = "of")
  class ResolveTag implements JoinTag {

    /**
     * The type of the referenced (child) resource.
     */
    @Nonnull
    ResourceType childResourceType;

    /**
     * {@inheritDoc}
     * <p>
     * For a ResolveTag, the tag is "id@{resourceType}".
     * This format indicates that the column contains a map where the keys are resource IDs
     * and the values are the referenced resources.
     */
    @Override
    @Nonnull
    public String getTag() {
      return "id" + "@" + childResourceType.toCode();
    }
  }

  /**
   * Represents a tag for a reverse resolve join.
   * <p>
   * This tag is used for columns that contain resources that reference the subject resource.
   * For example, in Patient.reverseResolve(Condition.subject), this tag would represent the
   * Condition resources that reference the Patient resources.
   */
  @Value(staticConstructor = "of")
  class ReverseResolveTag implements JoinTag {

    /**
     * The type of the referencing (child) resource.
     */
    @Nonnull
    ResourceType childResourceType;

    /**
     * The path in the child resource that references the master resource.
     */
    @Nonnull
    String masterKeyPath;

    /**
     * {@inheritDoc}
     * <p>
     * For a ReverseResolveTag, the tag is "{resourceType}@{path}".
     * This format indicates that the column contains a map where the keys are master resource IDs
     * and the values are arrays of child resources that reference the master resource.
     * <p>
     * The path is normalized by replacing dots with underscores to create a valid column name.
     */
    @Override
    @Nonnull
    public String getTag() {
      return childResourceType.toCode() + "@" + masterKeyPath.replace(".", "_");
    }
  }

}
