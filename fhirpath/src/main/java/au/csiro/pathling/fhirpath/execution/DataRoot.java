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

public interface DataRoot {

  @Value(staticConstructor = "of")
  class ResourceRoot implements DataRoot {

    @Nonnull
    ResourceType resourceType;


    @Nonnull
    @Override
    public String getTag() {
      return resourceType.toCode();
    }
  }

  @Value(staticConstructor = "of")
  class ReverseResolveRoot implements DataRoot {

    @Nonnull
    DataRoot master;
    @Nonnull
    ResourceType foreignResourceType;
    @Nonnull
    String foreignKeyPath;

    @Nonnull
    public ResourceType getMasterResourceType() {
      return ((ResourceRoot) master).resourceType;
    }

    @Nonnull
    @Override
    public String getTag() {
      return master.getTag() + "@" + foreignResourceType.toCode() + "_"
          + foreignKeyPath.replace(".", "_");
    }

    public static ReverseResolveRoot ofResource(@Nonnull final ResourceType masterType,
        @Nonnull final ResourceType foreignResourceType,
        @Nonnull final String foreignResourcePath) {
      return new ReverseResolveRoot(ResourceRoot.of(masterType), foreignResourceType,
          foreignResourcePath);
    }
  }

  @Value(staticConstructor = "of")
  class ResolveRoot implements DataRoot {

    @Nonnull
    DataRoot master;
    @Nonnull
    ResourceType foreignResourceType;
    @Nonnull
    String masterResourcePath;

    @Nonnull
    public ResourceType getMasterResourceType() {
      return ((ResourceRoot) master).resourceType;
    }

    @Nonnull
    @Override
    public String getTag() {
      return master.getTag() + "_"
          + masterResourcePath.replace(".", "_") + "@" + foreignResourceType.toCode();
    }

    public static ResolveRoot ofResource(@Nonnull final ResourceType masterType,
        @Nonnull final ResourceType foreignResourceType,
        @Nonnull final String masterResourcePath) {
      return new ResolveRoot(ResourceRoot.of(masterType), foreignResourceType,
          masterResourcePath);
    }
  }

  @Nonnull
  String getTag();

  @Nonnull
  default String getParentKeyTag() {
    return getTag() + "__pkey";
  }


  @Nonnull
  default String getChildKeyTag() {
    return getTag() + "__ckey";
  }

  @Nonnull
  default String getValueTag() {
    return getTag() + "__value";
  }
}

