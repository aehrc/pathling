package au.csiro.pathling.terminology;

import au.csiro.pathling.fhir.TerminologyClient2;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;

public class SimpleTerminologyService implements TerminologyService, Closeable {

  @Nonnull
  private final TerminologyClient2 terminologyClient;

  @Nonnull
  private final Closeable toClose;

  public SimpleTerminologyService(@Nonnull final TerminologyClient2 terminologyClient,
      @Nonnull Closeable toClose) {
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

  @Nonnull
  @Override
  public Parameters validate(@Nonnull final String url, @Nonnull final Coding coding) {
    // TODO: This should also do the system validation unless somehow validate is impervious
    // to the errors caused by unknown systems.
    return terminologyClient.validateCode(
        required(UriType::new, url), required(UriType::new, coding.getSystem()),
        optional(StringType::new, coding.getVersion()),
        required(CodeType::new, coding.getCode())
    );
  }

  @Nonnull
  @Override
  public Parameters translateCoding(@Nonnull final Coding coding,
      @Nonnull final String conceptMapUrl,
      final boolean reverse) {
    return terminologyClient.translate(
        required(UriType::new, conceptMapUrl), required(UriType::new, coding.getSystem()),
        optional(StringType::new, coding.getVersion()),
        required(CodeType::new, coding.getCode()),
        new BooleanType(reverse)
    );
  }

  @Nonnull
  @Override
  public ConceptTranslator translate(@Nonnull final Collection<SimpleCoding> codings,
      @Nonnull final String conceptMapUrl, final boolean reverse,
      @Nonnull final Collection<ConceptMapEquivalence> equivalences) {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  public Relation getSubsumesRelation(@Nonnull final Collection<SimpleCoding> systemAndCodes) {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  public Set<SimpleCoding> intersect(@Nonnull final String valueSetUri,
      @Nonnull final Collection<SimpleCoding> systemAndCodes) {
    throw new UnsupportedOperationException();
  }


  @Override
  public void close() throws IOException {
    toClose.close();
  }
}
