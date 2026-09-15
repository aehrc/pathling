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
 * Represents configuration specific to the derivation of the stored schema.
 *
 * <p>It sits beside {@code EncodingConfiguration} rather than on it. That class lives in the module
 * carrying the previous encoding implementation, which must be left in place and unmodified
 * (FR-051), so the options this work adds cannot go there. A caller therefore holds two
 * configuration objects for what reads as one concern, which is the price of leaving the previous
 * implementation genuinely untouched.
 *
 * <p>The nesting depth, the extension switch and the open types stay on that class and keep their
 * current meaning (FR-044). They bound the dense mode only, and the derivation takes them as plain
 * values, so restating them here would duplicate a live option rather than move it.
 */
@Data
@Builder
public class SchemaConfiguration {

  /**
   * Whether the stored schema comprises every element the definitions describe, rather than being
   * fitted to the elements the data populates.
   *
   * <p>Fitted is the default: the schema being fitted to the data is the design of this layout, and
   * the dense schema is the option (FR-009). The mode is a flag rather than a type of its own,
   * because the derivation itself distinguishes the two by which of {@link SchemaBuilder#dense} and
   * {@link SchemaBuilder#pruned} is called, and because a two-valued type would be an enumeration,
   * which this project does not use.
   */
  @Builder.Default private boolean denseSchema = false;

  /**
   * Whether content the definition set does not describe raises an error, rather than being ignored
   * (FR-018). It governs the presence of {@code contained} resources and input whose cardinality
   * contradicts the definitions alike, so that no part of that carve-out is silent.
   *
   * <p>Ignoring is the default. The contract for this layout enumerates the behaviour changes a
   * caller must be told about, and an ingest that begins failing on content the previous
   * implementation accepted is not among them; the previous implementation parsed leniently, and
   * the strictness switch is the opt-in to something stricter rather than a new baseline. Ignored
   * content is still detected rather than silently truncated, which is what FR-018 forbids.
   */
  @Builder.Default private boolean failOnNonConformantContent = false;

  /**
   * Whether the numeric annotation accompanying a decimal is emitted, named by {@link
   * LayoutFields#NUMERIC_SUFFIX}.
   */
  @Builder.Default private boolean enableNumericAnnotation = true;

  /**
   * Whether the range annotations accompanying a date, dateTime or instant are emitted, named by
   * {@link LayoutFields#START_SUFFIX} and {@link LayoutFields#END_SUFFIX}. They are one annotation
   * in two fields, a lower and an upper bound on the same stated precision, so one toggle governs
   * the pair.
   */
  @Builder.Default private boolean enableRangeAnnotation = true;

  /**
   * Whether the canonical annotation accompanying a quantity is emitted, named by {@link
   * LayoutFields#CANONICAL_SUFFIX}.
   */
  @Builder.Default private boolean enableCanonicalAnnotation = true;
}
