/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import static org.apache.spark.sql.functions.*;

import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * @author John Grimes
 */
public interface Referrer {

  /**
   * The name of the field within the value column that holds the ID of a foreign resource.
   */
  String REFERENCE_FIELD_NAME = "reference";

  /**
   * The character that separates path components within the reference element of a Reference.
   */
  String PATH_SEPARATOR = "/";

  /**
   * @param referrer the Referrer that is the subject of the operation
   * @return the {@code reference} element from within the Reference struct in this path's value
   * column
   */
  @Nonnull
  static Column referenceColumnFor(@Nonnull final Referrer referrer) {
    return referrer.getValueColumn().getField(REFERENCE_FIELD_NAME);
  }

  /**
   * Constructs an equality column for matching a resource reference to a {@link ResourcePath}.
   *
   * @param referrer the Referrer that is the subject of the operation
   * @param resourcePath the target ResourcePath
   * @return a {@link Column} representing the matching condition
   */
  @Nonnull
  static Column resourceEqualityFor(@Nonnull final Referrer referrer,
      @Nonnull final ResourcePath resourcePath) {
    final Column targetId = resourcePath.getIdColumn();
    final Column targetCode = lit(resourcePath.getResourceType().toCode());

    return Referrer.resourceEqualityFor(referrer, targetId, targetCode);
  }

  /**
   * Constructs an equality column for matching a resource reference to a dataset with a target
   * resource ID and code.
   *
   * @param referrer the Referrer that is the subject of the operation
   * @param targetId the resource identity column to match
   * @param targetCode a column containing the resource code of the target
   * @return a {@link Column} representing the matching condition
   */
  @Nonnull
  static Column resourceEqualityFor(@Nonnull final Referrer referrer,
      @Nonnull final Column targetId,
      @Nonnull final Column targetCode) {
    final Column components = split(referrer.getReferenceColumn(), PATH_SEPARATOR, 2);
    final Column numComponents = size(components);

    final Column resourceCode = element_at(components, 1);
    final Column resourceId = element_at(components, 2);

    // If the resource type in the reference does not match the target resource, return null.
    final Column idEquality = resourceId.equalTo(targetId);
    final Column conditionalEquality = when(resourceCode.equalTo(
        targetCode), idEquality).otherwise(null);

    // If the reference does not look like a relative reference with only two path components,
    // return null.
    return when(numComponents.equalTo(2), conditionalEquality).otherwise(null);
  }

  /**
   * @return a {@link Column} within the dataset containing the values of the nodes
   */
  @Nonnull
  Column getValueColumn();

  /**
   * @return the {@code reference} element from within the Reference struct in this path's value
   * column
   */
  @Nonnull
  Column getReferenceColumn();

  /**
   * Constructs an equality column for matching a resource reference to a {@link ResourcePath}.
   *
   * @param resourcePath the target ResourcePath
   * @return a {@link Column} representing the matching condition
   */
  @Nonnull
  Column getResourceEquality(@Nonnull ResourcePath resourcePath);

  /**
   * Constructs an equality column for matching a resource reference to a dataset with a target
   * resource ID and code.
   *
   * @param targetId the resource identity column to match
   * @param targetCode a column containing the resource code of the target
   * @return a {@link Column} representing the matching condition
   */
  @Nonnull
  Column getResourceEquality(@Nonnull Column targetId, @Nonnull Column targetCode);

}
