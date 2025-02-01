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

package au.csiro.pathling.test;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.update.ImportExecutor;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.FileFilter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.UrlType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Profile;

/**
 * Converts the test fhir data in `src/test/resources/test-data/fhir` to their parquet version in
 * `src/test/resources/test-data/parquet`.
 *
 * @author Piotr Szul
 */
@SpringBootApplication
@ComponentScan(basePackages = "au.csiro.pathling",
    excludeFilters = @ComponentScan.Filter(type = FilterType.ANNOTATION,
        classes = TestConfiguration.class))
@Profile("cli")
@Slf4j
public class TestDataImporter implements CommandLineRunner {

  @Nonnull
  protected final SparkSession spark;

  @Nonnull
  private final ImportExecutor importExecutor;

  @Autowired
  public TestDataImporter(@Nonnull final SparkSession spark,
      @Nonnull final ImportExecutor importExecutor) {
    this.spark = spark;
    this.importExecutor = importExecutor;
  }

  public static void main(final String[] args) {
    SpringApplication.run(TestDataImporter.class, args);
  }

  @Override
  public void run(final String... args) {
    final String sourcePath = args[0];

    final File srcNdJsonDir = new File(sourcePath);
    final FileFilter fileFilter = WildcardFileFilter.builder().setWildcards("*.ndjson").get();
    final File[] srcNdJsonFiles = srcNdJsonDir.listFiles(fileFilter);

    final List<ParametersParameterComponent> sources = Stream.of(requireNonNull(srcNdJsonFiles))
        .map(file -> {
          final String resourceName = FilenameUtils.getBaseName(file.getName());
          final ResourceType subjectResource = ResourceType
              .valueOf(resourceName.toUpperCase());
          final ParametersParameterComponent source = new ParametersParameterComponent();
          source.setName("source");
          final ParametersParameterComponent resourceType = new ParametersParameterComponent();
          resourceType.setName("resourceType");
          resourceType.setValue(new CodeType(subjectResource.toCode()));
          source.addPart(resourceType);
          final ParametersParameterComponent url = new ParametersParameterComponent();
          url.setName("url");
          url.setValue(new UrlType("file://" + file.toPath()));
          source.addPart(url);
          return source;
        })
        .collect(Collectors.toList());
    final Parameters parameters = new Parameters();
    parameters.setParameter(sources);

    importExecutor.execute(parameters);
  }

}
