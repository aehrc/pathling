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

package au.csiro.pathling.fhirpath.column;

import static au.csiro.pathling.sql.SqlFunctions.let;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.annotation.kinds.DateRangeKind;
import au.csiro.pathling.schema.date.FhirDateTime;
import au.csiro.pathling.utilities.CanonicalStructure;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * The columns of System dates and dateTimes, the form in which the engine holds every date,
 * dateTime and instant (FR-004, FR-022, FR-023).
 *
 * <p>A date is a flat structure of its text, {@code value}, and those of its range annotations, its
 * lower bound, {@code __start}, and its upper bound, {@code __end}, that are available without
 * computing (decisions 141 and 152). A traversed date carries the stored annotations the schema
 * has, read by name, and nothing more, so a date over unannotated data is {@code struct(value)}. A
 * literal carries both, as constants built as the query is built. Any other date, such as the
 * result of {@code toDate()}, carries none (rule A, design §6, question 6). A FHIR date and a
 * System date therefore have the same fields until a primitive's id and extensions are carried
 * (ticket 17), so a FHIR date converts to a System date as it is.
 *
 * <p>Nothing is computed as a date is traversed. An operation that needs a bound asks for it
 * through {@link #start} or {@link #end}, which reads the field where the date carries it and
 * computes it from the text otherwise, decided by Spark as it resolves the plan (design §4.3). The
 * two bounds computed from one text share one memoised parse of it (decision 145).
 *
 * <p>A text outside FHIR's date grammar is not conformant, and under {@code trust} the engine's
 * behaviour on it is undefined (ticket 31).
 */
public final class SystemDateTime {

  /** The name of the text of a System date. */
  @Nonnull public static final String VALUE = "value";

  /** The name of the lower bound of a System date. */
  @Nonnull public static final String START = LayoutFields.START_ANNOTATION;

  /** The name of the upper bound of a System date. */
  @Nonnull public static final String END = LayoutFields.END_ANNOTATION;

  /** The names of the range annotations of a date. */
  @Nonnull private static final List<String> ANNOTATIONS = List.of(START, END);

  /** The canonical order of the fields of a date, by which dates of different shapes merge. */
  @Nonnull
  public static final CanonicalStructure STRUCTURE = FlatStructure.of(List.of(VALUE, START, END));

  /** The type of a System date that carries no annotations. */
  @Nonnull
  public static final StructType DATA_TYPE = new StructType().add(VALUE, DataTypes.StringType);

  private SystemDateTime() {}

  /**
   * Returns the System date of a text, carrying no annotations.
   *
   * @param text the text of the date
   * @return the System date, null where the text is
   */
  @Nonnull
  public static Column fromText(@Nonnull final Column text) {
    // The text is bound once, because the guard and the value both read it, and a traced text must
    // be evaluated once (#2594).
    return let(text, t -> when(t.isNotNull(), struct(t.alias(VALUE))));
  }

  /**
   * Returns the System date of a text that may not be a date, as a conversion to a date gives it:
   * null where the text does not follow FHIR's date grammar, or names a date or time that does not
   * exist. It carries no annotations, as no function result does (rule A).
   *
   * @param text the text
   * @return the System date, null where the text is not a date
   */
  @Nonnull
  public static Column tryFromText(@Nonnull final Column text) {
    return let(
        text, t -> when(DateRangeKind.INSTANCE.startOrNull(t).isNotNull(), struct(t.alias(VALUE))));
  }

  /**
   * Returns the date of a stored text and its metadata group: its text and the range annotations
   * the group's schema carries. Nothing is computed.
   *
   * @param text the stored text of the date
   * @param group the metadata group beside it
   * @return the date, null where the text is
   */
  @Nonnull
  public static Column fromStored(@Nonnull final Column text, @Nonnull final Column group) {
    return when(
        text.isNotNull(),
        ColumnFunctions.resolveOrNone(group, ANNOTATIONS, List.of(VALUE), List.of(text), false));
  }

  /**
   * Returns the dates of a repeating stored date and its metadata groups, paired by position, each
   * with its text and the range annotations the groups' schema carries, with the dates that have no
   * text left out. Nothing is computed.
   *
   * <p>The groups are paired with the texts by position, so a missing array of groups leaves the
   * dates without their stored bounds rather than shifting the bounds of other dates.
   *
   * @param texts the stored texts of the date
   * @param groups the metadata groups beside them
   * @return the dates present, null where the texts are
   */
  @Nonnull
  public static Column fromStoredArray(@Nonnull final Column texts, @Nonnull final Column groups) {
    return functions.filter(
        ColumnFunctions.resolveOrNone(groups, ANNOTATIONS, List.of(VALUE), List.of(texts), true),
        date -> date.getField(VALUE).isNotNull());
  }

  /**
   * Returns the absent System date, typed as one.
   *
   * @return a null System date
   */
  @Nonnull
  public static Column empty() {
    return lit(null).cast(DATA_TYPE);
  }

  /**
   * Returns the System date of a literal, with the range computed as the query is built.
   *
   * @param text the text of the date
   * @return the System date
   */
  @Nonnull
  public static Column fromLiteral(@Nonnull final String text) {
    final FhirDateTime parsed = FhirDateTime.parse(text);
    // Instants, which Spark takes as they are, as it takes the ones the range annotation's kind
    // computes.
    return struct(
        lit(text).alias(VALUE),
        lit(parsed.getLowerBoundary()).alias(START),
        lit(parsed.getUpperBoundary()).alias(END));
  }

  /**
   * Returns the text of a date.
   *
   * @param date the date
   * @return its text
   */
  @Nonnull
  public static Column text(@Nonnull final Column date) {
    return date.getField(VALUE);
  }

  /**
   * Returns the lower bound of a date, {@code __start}: the field where the date carries it, and
   * the bound computed from its text otherwise.
   *
   * @param date the date
   * @return its lower bound
   */
  @Nonnull
  public static Column start(@Nonnull final Column date) {
    return ColumnFunctions.annotationOrElse(date, START, DateRangeKind.INSTANCE.start(text(date)));
  }

  /**
   * Returns the upper bound of a date, {@code __end}: the field where the date carries it, and the
   * bound computed from its text otherwise.
   *
   * @param date the date
   * @return its upper bound
   */
  @Nonnull
  public static Column end(@Nonnull final Column date) {
    return ColumnFunctions.annotationOrElse(date, END, DateRangeKind.INSTANCE.end(text(date)));
  }
}
