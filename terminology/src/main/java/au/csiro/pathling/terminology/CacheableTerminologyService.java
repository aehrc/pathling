package au.csiro.pathling.terminology;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import ca.uhn.fhir.rest.client.api.IGenericClient;
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

public class CacheableTerminologyService implements TerminologyService {

  @Nonnull
  private final IGenericClient fhirClient;

  public CacheableTerminologyService(@Nonnull final IGenericClient fhirClient) {
    this.fhirClient = fhirClient;
  }

  @Nullable
  @Override
  public Parameters validate(@Nonnull final String url, @Nonnull final Coding coding) {
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
}
