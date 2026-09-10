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

package au.csiro.pathling.io;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.AbstractSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.library.query.FhirViewQuery;
import au.csiro.pathling.views.FhirView;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A source derived from another by a row transformation, a resource-type filter, or both, which
 * resolves every read through the source it was derived from.
 *
 * <p>Deferring resolution is what makes the derivation see the same data its parent does. A source
 * built eagerly over the parent's resource map is frozen at the moment of derivation, so a type
 * whose table appears later - or whose dataset the parent replaces on refresh - is invisible to it.
 * Because a read here calls the parent's own {@code read}, the parent's dynamic discovery,
 * empty-dataset fallback and schema drift guard all apply, and they apply equally to a source
 * derived from a derived source.
 *
 * <p>Extending {@link AbstractSource} gives this source a query dispatcher and sink builder over
 * itself, so view queries and writes resolve their datasets through this class's {@code read}
 * rather than the parent's.
 *
 * @author John Grimes
 */
public class DerivedSource extends AbstractSource {

  /** A row operator that leaves the dataset untouched, for a derivation that only filters types. */
  static final BiFunction<String, Dataset<Row>, Dataset<Row>> IDENTITY_OPERATOR =
      (resourceType, dataset) -> dataset;

  /** A type predicate that retains every type, for a derivation that only transforms rows. */
  static final Predicate<String> RETAIN_ALL_TYPES = resourceType -> true;

  @Nonnull private final QueryableDataSource parent;

  @Nonnull private final BiFunction<String, Dataset<Row>, Dataset<Row>> operator;

  @Nonnull private final Predicate<String> typePredicate;

  @Nonnull private final Set<String> driftedTypes;

  /**
   * Constructs a new DerivedSource.
   *
   * @param context the Pathling context
   * @param parent the source this one is derived from, through which all reads are resolved
   * @param operator the transformation applied to each dataset read from the parent
   * @param typePredicate the types this source retains from the parent
   * @param driftedTypes the resource types whose tables are drifted and unmigrated; held by
   *     reference so that guard decisions reflect the parent's current state
   */
  DerivedSource(
      @Nonnull final PathlingContext context,
      @Nonnull final QueryableDataSource parent,
      @Nonnull final BiFunction<String, Dataset<Row>, Dataset<Row>> operator,
      @Nonnull final Predicate<String> typePredicate,
      @Nonnull final Set<String> driftedTypes) {
    super(context);
    this.parent = parent;
    this.operator = operator;
    this.typePredicate = typePredicate;
    this.driftedTypes = driftedTypes;
  }

  @Override
  @Nonnull
  public Dataset<Row> read(@Nullable final String resourceCode) {
    if (resourceCode == null) {
      throw new IllegalArgumentException("Resource code must not be null");
    }
    if (!typePredicate.test(resourceCode)) {
      // A type removed by the predicate fails exactly as it does when a source is filtered
      // eagerly, so that compartment semantics are unchanged by the deferred resolution.
      throw new IllegalArgumentException("No data found for resource type: " + resourceCode);
    }
    return operator.apply(resourceCode, parent.read(resourceCode));
  }

  @Override
  @Nonnull
  public Set<String> getResourceTypes() {
    return parent.getResourceTypes().stream().filter(typePredicate).collect(Collectors.toSet());
  }

  @Override
  @Nonnull
  public FhirViewQuery view(@Nullable final String subjectResource) {
    checkNotDrifted(subjectResource);
    return super.view(subjectResource);
  }

  @Override
  @Nonnull
  public FhirViewQuery view(@Nullable final FhirView view) {
    if (view != null) {
      checkNotDrifted(view.getResource());
    }
    return super.view(view);
  }

  @Override
  @Nonnull
  public QueryableDataSource map(
      @Nonnull final BiFunction<String, Dataset<Row>, Dataset<Row>> operator) {
    return new DerivedSource(context, this, operator, RETAIN_ALL_TYPES, driftedTypes);
  }

  @Override
  @Nonnull
  public QueryableDataSource filterByResourceType(
      @Nonnull final Predicate<String> resourceTypePredicate) {
    return new DerivedSource(context, this, IDENTITY_OPERATOR, resourceTypePredicate, driftedTypes);
  }

  @Override
  @Nonnull
  public DataSource cache() {
    return new DerivedSource(
        context, this, (resourceType, dataset) -> dataset.cache(), RETAIN_ALL_TYPES, driftedTypes);
  }

  /**
   * Fails with a {@link SchemaDriftError} if the given resource type is marked as drifted. A read
   * inherits this check from the parent, but a view query is only executed later, so its subject is
   * checked when the query is constructed - as {@link DriftGuardedSource} does.
   *
   * @param resourceCode the resource type code to check, or null to skip the check
   */
  private void checkNotDrifted(@Nullable final String resourceCode) {
    if (resourceCode != null && driftedTypes.contains(resourceCode)) {
      throw new SchemaDriftError(resourceCode);
    }
  }
}
