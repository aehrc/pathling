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

package au.csiro.pathling.library.query;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.query.DataSource;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Abstract base class for builders of {@link PathlingClient}.
 *
 * @param <T> the actual class of the builder.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class AbstractClientBuilder<T extends AbstractClientBuilder> {

  @Nonnull
  protected final PathlingContext pathlingContext;

  @Nonnull
  protected QueryConfiguration queryConfiguration = QueryConfiguration.builder().build();

  protected AbstractClientBuilder(@Nonnull final PathlingContext pathlingContext) {
    this.pathlingContext = pathlingContext;
  }

  /**
   * Sets the query configuration to use with the client.
   *
   * @param queryConfiguration the query configuration.
   * @return this builder.
   */
  @Nonnull
  public T withQueryConfiguration(@Nonnull final QueryConfiguration queryConfiguration) {
    this.queryConfiguration = queryConfiguration;
    return (T) this;
  }

  /**
   * Builds a new instance of {@link PathlingClient} with the configuration defined by this
   * builder.
   *
   * @return the configured instance of pathling client.
   */
  @Nonnull
  public PathlingClient build() {
    return new PathlingClient(pathlingContext.getFhirContext(), pathlingContext.getSpark(),
        buildDataSource(), queryConfiguration,
        Optional.of(pathlingContext.getTerminologyServiceFactory()));
  }

  /**
   * This is a shortcut to directly build an {@link  ExtractQuery} bound to the pathling client
   * created by this builder.
   *
   * @param subjectResourceType the type of the subject resource for the extract query.
   * @return the new instance of extract query.
   */
  @Nonnull
  public ExtractQuery buildExtractQuery(@Nonnull final ResourceType subjectResourceType) {
    return this.build().newExtractQuery(subjectResourceType);
  }
  
  /**
   * This is a shortcut to directly build an {@link  AggregateQuery} bound to the pathling client
   * created by this builder.
   *
   * @param subjectResourceType the type of the subject resource for the aggregate query.
   * @return the new instance of aggregate query.
   */   
  @Nonnull
  public AggregateQuery buildAggregateQuery(@Nonnull final ResourceType subjectResourceType) {
    return this.build().newAggregateQuery(subjectResourceType);
  }
  
  @Nonnull
  protected abstract DataSource buildDataSource();
}
