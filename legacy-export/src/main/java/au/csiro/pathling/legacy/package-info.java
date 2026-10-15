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
/**
 * The legacy export: converts tables in the legacy layout to FHIR NDJSON, from which they are
 * re-imported into the Parquet on FHIR layout (FR-064).
 *
 * <p>The legacy layout is the one the Pathling FHIR encoders wrote before the Parquet on FHIR
 * layout. Releases that read only the Parquet on FHIR layout refuse it, and this export is the way
 * out for data that has no other source to re-import from. Where the original FHIR data still
 * exists, re-importing it is better, because it loses nothing that the export below loses.
 *
 * <p>This module depends on {@code encoders}, which decodes the legacy layout, and on nothing newer
 * in the core chain. It is not part of {@code library-runtime} or of the language bindings.
 *
 * <h2>Use</h2>
 *
 * <p>As a component, through {@link au.csiro.pathling.legacy.LegacyExport}:
 *
 * <pre>{@code
 * LegacyExport export = LegacyExport.of(spark);
 * export.export("Patient", "/warehouse/Patient.parquet", LegacyTableFormat.DELTA, "/export");
 * }</pre>
 *
 * <p>As a command, through {@link au.csiro.pathling.legacy.LegacyExportCommand}, run as a Spark
 * application. Spark and Delta Lake are provided by the environment, and the module's other
 * dependencies, {@code encoders} and HAPI FHIR, are put on the classpath beside it:
 *
 * <pre>
 * spark-submit --class au.csiro.pathling.legacy.LegacyExportCommand \
 *   --packages au.csiro.pathling:encoders:&lt;version&gt;,io.delta:delta-spark_2.13:&lt;version&gt; \
 *   legacy-export-&lt;version&gt;.jar &lt;source&gt; &lt;parquet|delta&gt; &lt;output&gt; \
 *   [--resource-type &lt;type&gt;]
 * </pre>
 *
 * <p>The source is one table, or a directory of tables such as a warehouse. A table holds the
 * resource type its name starts with, as in {@code Patient.parquet}, unless {@code --resource-type}
 * names it. The output is a directory of files named {@code <type>.<part>.ndjson}, which the NDJSON
 * source reads directly. The re-import is then an ordinary read of NDJSON, written with any sink.
 *
 * <p>Only a table in the legacy layout is exported. A table without the {@code id_versioned} column
 * the legacy encoder writes for every resource is refused, as is one that also carries {@code
 * resourceType}, which mixes the two layouts. So is a table holding a column the legacy encoder
 * would not read, such as a table of another resource type: the decoder reads columns by name and
 * would drop an unknown one without a word. The table does not record how its encoder was
 * configured, so the export decodes with extensions enabled where the table has them, every open
 * type, and the lowest nesting level that reads every column.
 *
 * <h2>Losses</h2>
 *
 * <p>The export writes what the legacy layout stored, and the legacy encoder did not store
 * everything. What it dropped or changed when the table was written cannot be recovered:
 *
 * <ul>
 *   <li><b>Decimals</b> were stored at a fixed scale of six, rounding half up. Digits beyond the
 *       sixth decimal place are lost, and the value exported is the rounded one. The original scale
 *       is kept up to six, so {@code 1.50} is still exported as {@code 1.50}. A decimal with more
 *       than twenty-six digits before the point was not stored at all, and is absent from the
 *       export.
 *   <li><b>Instants</b> were stored as timestamps. They are exported in UTC, to the millisecond:
 *       {@code 2020-01-01T10:00:00+10:00} is exported as {@code 2020-01-01T00:00:00.000+00:00}. The
 *       instant is the same; its offset and precision are lost.
 *   <li><b>Nesting</b> was bounded. An element nested within more elements of its own type than the
 *       encoder's nesting level, three by default, was not stored, so a {@code Questionnaire.item}
 *       is exported to a depth of four items. Extensions are not bounded in this way.
 *   <li><b>Extensions</b> were stored only where they were enabled, which they were by default. Of
 *       an extension's value, only a type among the encoder's open types was stored, which by
 *       default are {@code boolean}, {@code code}, {@code date}, {@code dateTime}, {@code decimal},
 *       {@code integer}, {@code string}, {@code Coding}, {@code CodeableConcept}, {@code Address},
 *       {@code Identifier} and {@code Reference}. An extension that carried only a value of another
 *       type is absent from the export.
 *   <li><b>Modifier extensions</b> were not stored.
 *   <li><b>The id and extensions of a primitive element</b>, carried in FHIR JSON by a key such as
 *       {@code _birthDate}, were not stored. The primitive's value is exported.
 *   <li><b>Contained resources</b> were not stored. A reference to one, such as {@code #org}, is
 *       exported, but resolves to nothing.
 *   <li>The nested {@code contains} of {@code ValueSet.expansion.contains} was not stored.
 *   <li>Resources of the types the encoder did not support, {@code Bundle}, {@code Parameters},
 *       {@code StructureDefinition} and {@code StructureMap}, were never stored in the legacy
 *       layout.
 * </ul>
 *
 * <p>The re-imported data is subject, in addition, to the documented limits of the Parquet on FHIR
 * layout itself, as any data read from FHIR JSON is.
 */
package au.csiro.pathling.legacy;
