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

package au.csiro.pathling.library.data;

import au.csiro.pathling.library.PathlingContext;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Functions that support the building of file system-based data sources.
 */
public interface SupportFunctions {

  /**
   * Extracts the resource type from the base name of the provided filepath or URI. Allows for an
   * optional qualifier string, which is separated from the resource name by a period. For example,
   * "/foo/bar/Procedure.ICU.ndjson" will return ["Procedure"].
   *
   * @param filePath the path/URI of the file
   * @return a single-element list containing the resource type
   */
  @Nonnull
  static List<String> basenameWithQualifierToResource(@Nonnull final String filePath) {
    final String baseName = FilenameUtils.getBaseName(filePath);
    final String qualifierRemoved = baseName.replaceFirst("\\.([^\\.]+)$", "");
    return Collections.singletonList(
        ResourceType.fromCode(qualifierRemoved).toCode());
  }

  /**
   * The transformer that returns the original dataset.
   *
   * @param dataset the dataset to transform.
   * @param resourceType the resource type of the dataset.
   * @return the original dataset.
   */
  @Nonnull
  static Dataset<Row> identityTransformer(@Nonnull final Dataset<Row> dataset,
      @Nonnull final String resourceType) {
    return dataset;
  }

  /**
   * Creates the transformer that encodes the text dataset of the specified resource type and mime
   * type to the structured representation.
   *
   * @param pathlingContext the pathling context.
   * @param mimeType the mime type of the text.
   * @return the transformer.
   */
  @Nonnull
  static BiFunction<Dataset<Row>, String, Dataset<Row>> textEncodingTransformer(@Nonnull final
  PathlingContext pathlingContext, @Nonnull final String mimeType) {
    return (df, resourceType) ->
        pathlingContext.encode(df, resourceType, mimeType);
  }
}
