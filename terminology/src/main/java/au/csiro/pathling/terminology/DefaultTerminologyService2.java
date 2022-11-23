package au.csiro.pathling.terminology;

import au.csiro.pathling.fhir.TerminologyClient2;
import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;

import static java.util.Objects.nonNull;

public class DefaultTerminologyService2 implements TerminologyService2, Closeable {

  @Nonnull
  private final TerminologyClient2 terminologyClient;

  @Nullable
  private final Closeable toClose;

  public DefaultTerminologyService2(@Nonnull final TerminologyClient2 terminologyClient,
      @Nullable Closeable toClose) {
    this.terminologyClient = terminologyClient;
    this.toClose = toClose;
  }

  @Nullable
  private static <T> T optional(@Nonnull final Function<String, T> converter,
      @Nullable String value) {
    return value != null
           ? converter.apply(value)
           : null;
  }

  @Nonnull
  private static <T> T required(@Nonnull final Function<String, T> converter,
      @Nullable String value) {
    return converter.apply(Objects.requireNonNull(value));
  }

  public static boolean isResultTrue(final @Nonnull Parameters parameters) {
    return parameters.getParameterBool("result");
  }

  @Override
  public boolean validate(@Nonnull final String url, @Nonnull final Coding coding) {
    return isResultTrue(terminologyClient.validateCode(
        required(UriType::new, url), required(UriType::new, coding.getSystem()),
        optional(StringType::new, coding.getVersion()),
        required(CodeType::new, coding.getCode())
    ));
  }

  @Nonnull
  @Override
  public Parameters translate(@Nonnull final Coding coding,
      @Nonnull final String conceptMapUrl,
      final boolean reverse) {

    // TODO: how should we deal with things like null code inside here ???
    // Should this be an error or should we just return null 
    return terminologyClient.translate(
        required(UriType::new, conceptMapUrl), required(UriType::new, coding.getSystem()),
        optional(StringType::new, coding.getVersion()),
        required(CodeType::new, coding.getCode()),
        new BooleanType(reverse)
    );
  }

  @Nullable
  @Override
  public Parameters subsumes(@Nonnull final Coding codingA, @Nonnull final Coding codingB) {

    // TODO: 
    if (codingA.getSystem() == null || !codingA.getSystem().equals(codingB.getSystem())) {
      return null;
    }
    final String resolvedSystem = codingA.getSystem();

    // if both version are present then ten need to be equal
    // TODO: this should most likely result either in null or false
    if (!(codingA.getVersion() == null || codingB.getVersion() == null || codingA.getVersion()
        .equals(codingB.getVersion()))) {
      return null;
    }
    final String resolvedVersion = codingA.getVersion() != null
                                   ? codingA.getVersion()
                                   : codingB.getVersion();

    // TODO: optimize not call the client if not needed

    // there are some assertions here to make
    return terminologyClient.subsumes(
        required(CodeType::new, codingA.getCode()),
        required(CodeType::new, codingB.getCode()),
        required(UriType::new, resolvedSystem),
        optional(StringType::new, resolvedVersion)
    );
  }

  @Override
  public void close() throws IOException {
    if (nonNull(toClose)) {
      toClose.close();
    }
  }
}
