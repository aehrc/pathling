package au.csiro.pathling.fhirpath.function.subsumes;

import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.encoding.BooleanResult;
import au.csiro.pathling.fhirpath.encoding.IdAndCodingSets;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import com.google.common.collect.Streams;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapPartitionsFunction;

@Slf4j
public class SubsumptionMapper
    implements MapPartitionsFunction<IdAndCodingSets, BooleanResult> {

  private static final long serialVersionUID = 1L;

  private final TerminologyClientFactory terminologyClientFactory;
  private final boolean inverted;

  public SubsumptionMapper(@Nonnull TerminologyClientFactory terminologyClientFactory,
      boolean inverted) {
    this.terminologyClientFactory = terminologyClientFactory;
    this.inverted = inverted;
  }

  @Override
  public Iterator<BooleanResult> call(Iterator<IdAndCodingSets> input) {
    List<IdAndCodingSets> entries = Streams.stream(input).collect(Collectors.toList());
    // collect distinct token
    Set<SimpleCoding> entrySet = entries.stream()
        .filter(r -> r.getInputCodings() != null && r.getArgCodings() != null)
        .flatMap(r -> Streams.concat(r.getInputCodings().stream(), r.getArgCodings().stream()))
        .collect(Collectors.toSet());

    ClosureService closureService = new ClosureService(terminologyClientFactory.build(log));
    final Closure subsumeClosure = closureService.getSubsumesRelation(entrySet);

    return entries.stream().map(r -> {
      if (r.getInputCodings() == null) {
        return BooleanResult.nullOf(r.getId());
      } else {
        boolean result = (!inverted
                          ? subsumeClosure.anyRelates(r.getInputCodings(), r.getArgCodings())
                          :
                          subsumeClosure.anyRelates(r.getArgCodings(), r.getInputCodings()));
        return BooleanResult.of(r.getId(), result);
      }
    }).iterator();
  }
}