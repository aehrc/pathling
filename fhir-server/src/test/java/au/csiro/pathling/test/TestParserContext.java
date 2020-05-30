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
  public static ParserContext build() {
    final FhirPath inputContext = mock(FhirPath.class);
    when(inputContext.getIdColumn()).thenReturn(mock(Column.class));
    final ThisPath thisContext = mock(ThisPath.class);
    final FhirContext fhirContext = FhirHelpers.getFhirContext();
    final SparkSession sparkSession = SparkHelpers.getSparkSession();
    final ResourceReader resourceReader = mock(ResourceReader.class, new DefaultAnswer());
    final TerminologyClient terminologyClient = mock(TerminologyClient.class, new DefaultAnswer());
    final TerminologyClientFactory terminologyClientFactory = mock(TerminologyClientFactory.class,
        new DefaultAnswer());

    return new TestParserContext(inputContext, Optional.of(thisContext), fhirContext, sparkSession,
        resourceReader, Optional.of(terminologyClient), Optional.of(terminologyClientFactory));
  }

  @Nonnull
  public static ParserContext build(@Nonnull final Column idColumn) {
    final FhirPath inputContext = mock(FhirPath.class);
    when(inputContext.getIdColumn()).thenReturn(idColumn);
    final ThisPath thisContext = mock(ThisPath.class);
    final FhirContext fhirContext = FhirHelpers.getFhirContext();
    final SparkSession sparkSession = SparkHelpers.getSparkSession();
    final ResourceReader resourceReader = mock(ResourceReader.class, new DefaultAnswer());
    final TerminologyClient terminologyClient = mock(TerminologyClient.class, new DefaultAnswer());
    final TerminologyClientFactory terminologyClientFactory = mock(TerminologyClientFactory.class,
        new DefaultAnswer());

    return new TestParserContext(inputContext, Optional.of(thisContext), fhirContext, sparkSession,
        resourceReader, Optional.of(terminologyClient), Optional.of(terminologyClientFactory));
  }

  @Nonnull
  public static ParserContext build(@Nonnull final Column idColumn,
      @Nonnull final ResourceReader resourceReader) {
    final FhirPath inputContext = mock(FhirPath.class);
    when(inputContext.getIdColumn()).thenReturn(idColumn);
    final ThisPath thisContext = mock(ThisPath.class);
    final FhirContext fhirContext = FhirHelpers.getFhirContext();
    final SparkSession sparkSession = SparkHelpers.getSparkSession();
    final TerminologyClient terminologyClient = mock(TerminologyClient.class, new DefaultAnswer());
    final TerminologyClientFactory terminologyClientFactory = mock(TerminologyClientFactory.class,
        new DefaultAnswer());

    return new TestParserContext(inputContext, Optional.of(thisContext), fhirContext, sparkSession,
        resourceReader, Optional.of(terminologyClient), Optional.of(terminologyClientFactory));
  }

  @Nonnull
  public static ParserContext build(@Nonnull final FhirPath inputContext,
      @Nonnull final ResourceReader resourceReader) {
    final ThisPath thisContext = mock(ThisPath.class, new DefaultAnswer());
    final FhirContext fhirContext = FhirHelpers.getFhirContext();
    final SparkSession sparkSession = SparkHelpers.getSparkSession();
    final TerminologyClient terminologyClient = mock(TerminologyClient.class, new DefaultAnswer());
    final TerminologyClientFactory terminologyClientFactory = mock(TerminologyClientFactory.class,
        new DefaultAnswer());

    return new TestParserContext(inputContext, Optional.of(thisContext), fhirContext, sparkSession,
        resourceReader, Optional.of(terminologyClient), Optional.of(terminologyClientFactory));
  }

}
