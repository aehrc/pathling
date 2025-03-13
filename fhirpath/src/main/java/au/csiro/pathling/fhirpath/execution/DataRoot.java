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
 * Represents a root of data in the FHIR path execution context.
 * This interface defines the common behavior for different types of data roots
 * such as resource roots and join roots.
 */
public interface DataRoot {

  /**
   * Gets the resource type of this data root.
   *
   * @return The resource type
   */
  @Nonnull
  ResourceType getResourceType();

  /**
   * Gets the parent resource type of this data root.
   * For a resource root, this is the same as the resource type.
   * For a join root, this is the resource type of the master.
   *
   * @return The parent resource type
   */
  @Nonnull
  ResourceType getParentResourceType();

  /**
   * Returns a human-readable string representation of this data root.
   *
   * @return A display string
   */
  @Nonnull
  String toDisplayString();

  /**
   * Represents a root of a FHIR resource.
   * This is the simplest form of data root, representing a direct resource.
   */
  @Value(staticConstructor = "of")
  class ResourceRoot implements DataRoot {

    /**
     * The type of the resource.
     */
    @Nonnull
    ResourceType resourceType;

    @Override
    @Nonnull
    public ResourceType getParentResourceType() {
      return resourceType;
    }

    @Nonnull
    @Override
    public String getTag() {
      return resourceType.toCode();
    }

    @Override
    @Nonnull
    public String toDisplayString() {
      return resourceType.toCode();
    }
  }

  /**
   * Represents a root that is joined to another root.
   * This interface defines the common behavior for different types of join roots.
   */
  interface JoinRoot extends DataRoot {

    /**
     * Gets the parent resource type of this join root.
     * This is the resource type of the master.
     *
     * @return The parent resource type
     */
    @Override
    @Nonnull
    default ResourceType getParentResourceType() {
      return getMaster().getResourceType();
    }

    /**
     * Gets the master data root that this join root is joined to.
     *
     * @return The master data root
     */
    @Nonnull
    DataRoot getMaster();

    /**
     * Converts this join root to a join tag.
     *
     * @return The join tag
     */
    @Nonnull
    JoinTag asTag();

    /**
     * Gets the tag for this join root.
     * This is the tag of the join tag.
     *
     * @return The tag
     */
    @Override
    @Nonnull
    default String getTag() {
      return asTag().getTag();
    }
  }

  /**
   * Represents a root that is joined to another root via a reverse resolve.
   * This is used when resolving from a resource to resources that reference it.
   */
  @Value(staticConstructor = "of")
  class ReverseResolveRoot implements JoinRoot {

    /**
     * The master data root that this reverse resolve root is joined to.
     */
    @Nonnull
    DataRoot master;
    
    /**
     * The type of the foreign resource that references the master resource.
     */
    @Nonnull
    ResourceType foreignResourceType;
    
    /**
     * The path in the foreign resource that references the master resource.
     */
    @Nonnull
    String foreignKeyPath;

    @Override
    @Nonnull
    public ResourceType getResourceType() {
      return foreignResourceType;
    }

    @Nonnull
    @Override
    public JoinTag asTag() {
      return JoinTag.ReverseResolveTag.of(foreignResourceType, foreignKeyPath);
    }

    @Override
    @Nonnull
    public String toDisplayString() {
      return foreignResourceType.toCode() + "<-" + foreignKeyPath;
    }

    /**
     * Creates a new reverse resolve root from a resource type.
     *
     * @param masterType The type of the master resource
     * @param foreignResourceType The type of the foreign resource
     * @param foreignResourcePath The path in the foreign resource that references the master resource
     * @return A new reverse resolve root
     */
    public static ReverseResolveRoot ofResource(@Nonnull final ResourceType masterType,
        @Nonnull final ResourceType foreignResourceType,
        @Nonnull final String foreignResourcePath) {
      return new ReverseResolveRoot(ResourceRoot.of(masterType), foreignResourceType,
          foreignResourcePath);
    }

  }

  /**
   * Represents a root that is joined to another root via a resolve.
   * This is used when resolving from a resource to resources that it references.
   */
  @Value(staticConstructor = "of")
  class ResolveRoot implements JoinRoot {

    /**
     * The master data root that this resolve root is joined to.
     */
    @Nonnull
    DataRoot master;
    
    /**
     * The type of the foreign resource that is referenced by the master resource.
     */
    @Nonnull
    ResourceType foreignResourceType;
    
    /**
     * The path in the master resource that references the foreign resource.
     */
    @Nonnull
    String masterResourcePath;

    @Override
    @Nonnull
    public ResourceType getResourceType() {
      return foreignResourceType;
    }

    @Nonnull
    @Override
    public JoinTag asTag() {
      return JoinTag.ResolveTag.of(foreignResourceType);
    }

    @Override
    @Nonnull
    public String toDisplayString() {
      return masterResourcePath + "->" + foreignResourceType.toCode();
    }

    /**
     * Creates a new resolve root from a resource type.
     *
     * @param masterType The type of the master resource
     * @param foreignResourceType The type of the foreign resource
     * @param masterResourcePath The path in the master resource that references the foreign resource
     * @return A new resolve root
     */
    public static ResolveRoot ofResource(@Nonnull final ResourceType masterType,
        @Nonnull final ResourceType foreignResourceType,
        @Nonnull final String masterResourcePath) {
      return new ResolveRoot(ResourceRoot.of(masterType), foreignResourceType,
          masterResourcePath);
    }
  }

  /**
   * Gets the tag for this data root.
   * This is used to identify the data root in the execution context.
   *
   * @return The tag
   */
  @Nonnull
  String getTag();

  /**
   * Gets the parent key tag for this data root.
   * This is used to identify the parent key in the execution context.
   *
   * @return The parent key tag
   */
  @Nonnull
  default String getParentKeyTag() {
    return getTag() + "__pkey";
  }

  /**
   * Gets the child key tag for this data root.
   * This is used to identify the child key in the execution context.
   *
   * @return The child key tag
   */
  @Nonnull
  default String getChildKeyTag() {
    return getTag() + "__ckey";
  }

  /**
   * Gets the value tag for this data root.
   * This is used to identify the value in the execution context.
   *
   * @return The value tag
   */
  @Nonnull
  default String getValueTag() {
    return getTag();
  }
}

