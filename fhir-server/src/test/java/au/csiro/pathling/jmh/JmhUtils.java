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

package au.csiro.pathling.jmh;

import jakarta.annotation.Nonnull;
import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.openjdk.jmh.results.format.ResultFormatType;

public class JmhUtils {

  private JmhUtils() {
  }

  @Nonnull
  public static String buildResultsFileName(@Nonnull final String outputDir,
      @Nonnull final String resultFilePrefix, @Nonnull final ResultFormatType resultType) {
    LocalDateTime date = LocalDateTime.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-hhmmss");

    String suffix;
    switch (resultType) {
      case CSV:
        suffix = ".csv";
        break;
      case SCSV:
        // Semi-colon separated values
        suffix = ".scsv";
        break;
      case LATEX:
        suffix = ".tex";
        break;
      case JSON:
      default:
        suffix = ".json";
        break;

    }
    return new File(new File(outputDir),
        String.format("%s%s%s", resultFilePrefix, date.format(formatter), suffix)).getPath();
  }


}
