package au.csiro.pathling.fhirpath.function.subsumes;

import au.csiro.pathling.fhir.SimpleCoding;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.function.subsumes.encoding.IdAndBoolean;
import au.csiro.pathling.fhirpath.function.subsumes.encoding.IdAndCodingSets;
import com.google.common.collect.Streams;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubsumptionMapper
    implements MapPartitionsFunction<IdAndCodingSets, IdAndBoolean> {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LoggerFactory
      .getLogger(SubsumptionMapper.class);
  private final TerminologyClientFactory terminologyClientFactory;
  private final boolean inverted;

  public SubsumptionMapper(@Nonnull TerminologyClientFactory terminologyClientFactory) {
    this(terminologyClientFactory, true);
  }

  public SubsumptionMapper(@Nonnull TerminologyClientFactory terminologyClientFactory,
      boolean inverted) {
    this.terminologyClientFactory = terminologyClientFactory;
    this.inverted = inverted;
  }

  @Override
  public Iterator<IdAndBoolean> call(Iterator<IdAndCodingSets> input) {
    List<IdAndCodingSets> entries = Streams.stream(input).collect(Collectors.toList());
    // collect distinct token

    entries.forEach(System.out::println);

    Set<SimpleCoding> entrySet = entries.stream()
        .filter(r -> r.getInputCodings() != null && r.getArgCodings() != null)
        .flatMap(r -> Streams.concat(r.getInputCodings().stream(), r.getArgCodings().stream()))
        .collect(Collectors.toSet());

    ClosureService closureService = new ClosureService(terminologyClientFactory.build(logger));
    final Closure subsumeClosure = closureService.getSubsumesRelation(entrySet);

    return entries.stream().map(r -> {
      if (r.getInputCodings() == null) {
        return IdAndBoolean.nullFor(r.getId());
      } else {
        boolean result = (!inverted
                          ? subsumeClosure.anyRelates(r.getInputCodings(), r.getArgCodings())
                          :
                          subsumeClosure.anyRelates(r.getArgCodings(), r.getInputCodings()));
        return IdAndBoolean.resultFor(r.getId(), result);
      }
    }).iterator();
  }
}