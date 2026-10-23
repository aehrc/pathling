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

package au.csiro.pathling.schema;

import lombok.Builder;
import lombok.Data;

/**
 * Represents configuration specific to the stored schema: which annotations the annotation step
 * stores, and what it does with the annotations its input already carries.
 *
 * <p>It sits beside {@code EncodingConfiguration} rather than on it. That class lives in the module
 * carrying the previous encoding implementation, which must be left in place and unmodified
 * (FR-051), so the options this work adds cannot go there.
 *
 * <p>The switches govern what the annotation step stores, never what the engine can compute: the
 * engine computes an annotation the schema does not carry, so turning one off trades the width of
 * the stored data against the speed of a query, never its answer (FR-022).
 */
@Data
@Builder
public class SchemaConfiguration {

  /**
   * Whether the numeric annotation of a decimal is stored, named by {@link
   * LayoutFields#NUMERIC_ANNOTATION}.
   */
  @Builder.Default private boolean enableNumericAnnotation = true;

  /**
   * Whether the range annotations of a date, dateTime or instant are stored, named by {@link
   * LayoutFields#START_ANNOTATION} and {@link LayoutFields#END_ANNOTATION}. They are one annotation
   * in two fields, the bounds of the same stated precision, so one switch governs the pair.
   */
  @Builder.Default private boolean enableRangeAnnotation = true;

  /**
   * Whether the canonical annotations of a quantity are stored, named by {@link
   * LayoutFields#CANONICAL_ANNOTATION} and {@link LayoutFields#CANONICAL_EXACT_ANNOTATION}. They
   * are one annotation in two representations, the specification's fixed-point form and a
   * magnitude-preserving one, so one switch governs the pair.
   */
  @Builder.Default private boolean enableCanonicalAnnotation = true;

  /**
   * Whether the annotations of the kinds that are on are all computed again, rather than computed
   * only where the input lacks them.
   *
   * <p>Computing only the missing ones, called fill, is the default. An annotation is worth having
   * only once stored, and recomputing what Pathling's own files already carry would throw that
   * away. Recomputing is the repair for annotations from a producer that is not trusted: it makes
   * every annotation of a kind that is on consistent with its value, whatever the input carried,
   * including one of the wrong value or the wrong type. Fill makes them consistent only where the
   * input's own annotations were consistent and not null.
   *
   * <p>Where the input carries a primitive's metadata group but not the primitive, the primitive is
   * taken to be null in every row. An annotation the group carries is then kept by fill and
   * replaced by the one computed for a null value by recomputing, and none is added.
   */
  @Builder.Default private boolean recomputeAnnotations = false;

  /**
   * Whether an annotation in the input of a kind that is off is kept, trusting its producer, rather
   * than left out.
   *
   * <p>Leaving it out is the default. One that is kept stays in the structure it was found in, and
   * its value is passed through unchanged.
   */
  @Builder.Default private boolean keepUnrequestedAnnotations = false;

  /**
   * Whether a field in the input whose name marks it as an annotation, but which is no annotation
   * Pathling computes there, is left out rather than kept.
   *
   * <p>Keeping it is the default, because once dropped it cannot be restored. One that is kept
   * stays in the structure it was found in, after the annotations the step derives there, and its
   * value is passed through unchanged.
   */
  @Builder.Default private boolean dropUnknownAnnotations = false;
}
