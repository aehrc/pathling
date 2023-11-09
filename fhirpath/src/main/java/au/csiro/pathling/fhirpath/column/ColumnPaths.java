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

package au.csiro.pathling.fhirpath.column;

import lombok.Value;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import javax.annotation.Nonnull;
import java.util.function.BiFunction;
import java.util.function.Function;

abstract class ColumnPaths {

  private ColumnPaths() {
  }

  @Value
  public static class GetField implements ColumnPath {

    @Nonnull
    String fieldName;
  }


  @Value
  public static class Resource implements ColumnPath {

    @Nonnull
    ResourceType resourceType;
  }


  @Value
  public static class ReverseJoin implements ColumnPath {

    @Nonnull
    ResourceType resourceType;

    @Nonnull
    ResourceType foreignResourceType;
  }


  @Value
  public static class Join implements ColumnPath {

    @Nonnull
    ResourceType foreignResourceType;
  }


  @Value
  public static class Literal implements ColumnPath {

    @Nonnull
    Object value;
  }


  @Value
  public static class Call implements ColumnPath {

    @Nonnull
    Function<Column, Column> function;
  }

  @Value
  public static class Operator2 implements ColumnPath {

    @Nonnull
    ColumnPath left;

    @Nonnull
    ColumnPath right;

    @Nonnull
    BiFunction<Column, Column, Column> operator;
  }


  @Value
  public static class VectorizedCall implements ColumnPath {

    @Nonnull
    Function<Column, Column> arrayExpression;

    @Nonnull
    Function<Column, Column> singulaExpression;
  }

  @Value
  public static class CallUDF implements ColumnPath {

    @Nonnull
    final String udfName;
    @Nonnull
    final ColumnCtx[] args;
  }


  @Value
  public static class Explode implements ColumnPath {

    @Nonnull
    final boolean withNulls;
  }

}
