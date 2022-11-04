package au.csiro.pathling.terminology;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang.NotImplementedException;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.ValueSet;

public class CacheableTerminologyService implements TerminologyService, Closeable {

  @Nonnull
  private final IGenericClient fhirClient;

  @Nonnull
  private final Closeable toClose;

  public CacheableTerminologyService(@Nonnull final IGenericClient fhirClient,
      @Nonnull Closeable toClose) {
    this.fhirClient = fhirClient;
    this.toClose = toClose;
  }

  @Nullable
  @Override
  public Parameters validate(@Nonnull final String url, @Nonnull final Coding coding) {
    // TODO: Check why this needs to be done with the GenericClient
    // (Why the typed client uses POST rather then get for this operatio

    // TODO: This should also do the system validation unless somehow validate is impervious
    // to the errors caused by unknown systems.

    final Parameters params = new Parameters();
    params.addParameter().setName("url").setValue(new UriType(url));
    params.addParameter().setName("system").setValue(new UriType(coding.getSystem()));
    params.addParameter().setName("code").setValue(new CodeType(coding.getCode()));
    if (coding.hasVersion()) {
      params.addParameter().setName("systemVersion").setValue(new UriType(coding.getVersion()));
    }

    return fhirClient.operation()
        .onType(ValueSet.class)
        .named("$validate-code")
        .withParameters(params)
        .useHttpGet()
        .execute();
  }

  @Nonnull
  @Override
  public Parameters translateCoding(@Nonnull final Coding coding,
      @Nonnull final String conceptMapUrl,
      final boolean reverse) {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  public ConceptTranslator translate(@Nonnull final Collection<SimpleCoding> codings,
      @Nonnull final String conceptMapUrl, final boolean reverse,
      @Nonnull final Collection<ConceptMapEquivalence> equivalences) {
    throw new NotImplementedException();
  }

  @Nonnull
  @Override
  public Relation getSubsumesRelation(@Nonnull final Collection<SimpleCoding> systemAndCodes) {
    throw new NotImplementedException();
  }

  @Nonnull
  @Override
  public Set<SimpleCoding> intersect(@Nonnull final String valueSetUri,
      @Nonnull final Collection<SimpleCoding> systemAndCodes) {
    throw new NotImplementedException();
  }


  @Override
  public void close() throws IOException {
    toClose.close();
  }
}
