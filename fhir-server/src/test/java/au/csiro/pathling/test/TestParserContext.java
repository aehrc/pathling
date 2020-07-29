/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ThisPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.helpers.FhirHelpers;
import au.csiro.pathling.test.helpers.SparkHelpers;
import ca.uhn.fhir.context.FhirContext;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;

/**
 * @author John Grimes
 */
public class TestParserContext extends ParserContext {

  private TestParserContext(@Nonnull final FhirPath inputContext,
      @Nonnull final Optional<ThisPath> thisContext, @Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession, @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyClient> terminologyClient,
      @Nonnull final Optional<TerminologyClientFactory> terminologyClientFactory) {
    super(inputContext, thisContext, fhirContext, sparkSession, resourceReader, terminologyClient,
        terminologyClientFactory);
  }

  @Nonnull
  public static TestParserContextBuilder builder() {
    return new TestParserContextBuilder();
  }

  public static class TestParserContextBuilder {

    @Nonnull
    private FhirPath inputContext;

    @Nullable
    private ThisPath thisContext;

    @Nonnull
    private FhirContext fhirContext;

    @Nonnull
    private SparkSession sparkSession;

    @Nonnull
    private ResourceReader resourceReader;

    @Nullable
    private TerminologyClient terminologyClient;

    @Nullable
    private TerminologyClientFactory terminologyClientFactory;

    private TestParserContextBuilder() {
      inputContext = mock(FhirPath.class);
      when(inputContext.getIdColumn()).thenReturn(mock(Column.class));
      fhirContext = FhirHelpers.getFhirContext();
      sparkSession = SparkHelpers.getSparkSession();
      resourceReader = mock(ResourceReader.class, new DefaultAnswer());
    }

    @Nonnull
    public TestParserContextBuilder inputContext(@Nonnull final FhirPath inputContext) {
      this.inputContext = inputContext;
      return this;
    }

    @Nonnull
    public TestParserContextBuilder idColumn(@Nonnull final Column idColumn) {
      when(inputContext.getIdColumn()).thenReturn(idColumn);
      return this;
    }

    @Nonnull
    public TestParserContextBuilder thisContext(@Nonnull final ThisPath thisContext) {
      this.thisContext = thisContext;
      return this;
    }

    @Nonnull
    public TestParserContextBuilder fhirContext(@Nonnull final FhirContext fhirContext) {
      this.fhirContext = fhirContext;
      return this;
    }

    @Nonnull
    public TestParserContextBuilder sparkSession(@Nonnull final SparkSession sparkSession) {
      this.sparkSession = sparkSession;
      return this;
    }

    @Nonnull
    public TestParserContextBuilder resourceReader(@Nonnull final ResourceReader resourceReader) {
      this.resourceReader = resourceReader;
      return this;
    }

    @Nonnull
    public TestParserContextBuilder terminologyClient(
        @Nonnull final TerminologyClient terminologyClient) {
      this.terminologyClient = terminologyClient;
      return this;
    }

    @Nonnull
    public TestParserContextBuilder terminologyClientFactory(
        @Nonnull final TerminologyClientFactory terminologyClientFactory) {
      this.terminologyClientFactory = terminologyClientFactory;
      return this;
    }

    @Nonnull
    public TestParserContext build() {
      return new TestParserContext(inputContext, Optional.ofNullable(thisContext), fhirContext,
          sparkSession, resourceReader, Optional.ofNullable(terminologyClient),
          Optional.ofNullable(terminologyClientFactory));
    }

  }
}
