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
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubsumptionMapper
    implements MapPartitionsFunction<IdAndCodingSets, IdAndBoolean> {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LoggerFactory
      .getLogger(SubsumptionMapper.class);
  private final TerminologyClientFactory terminologyClientFactory;

  public SubsumptionMapper(TerminologyClientFactory terminologyClientFactory) {
    super();
    this.terminologyClientFactory = terminologyClientFactory;
  }

  @Override
  public Iterator<IdAndBoolean> call(Iterator<IdAndCodingSets> input) {
    List<IdAndCodingSets> entries = Streams.stream(input).collect(Collectors.toList());
    // collect distinct token

    entries.forEach(System.out::println);

    Set<SimpleCoding> entrySet = entries.stream()
        .filter(r -> r.getInputCodings() != null && r.getArgCodings() != null)
        .flatMap(r -> Streams.concat(r.getInputCodings().stream(), r.getArgCodings().stream()))
        .filter(SimpleCoding::isNotNull).collect(Collectors.toSet());
    ClosureService closureService = new ClosureService(terminologyClientFactory.build(logger));
    final Closure subsumeClosure = closureService.getSubsumesRelation(entrySet);

    return entries.stream().map(r -> IdAndBoolean.of(r.getId(),
        (r.getInputCodings() == null || r.getArgCodings() == null || r.getInputCodings().isEmpty()
            || r.getArgCodings().isEmpty())
        ? null
        :
        subsumeClosure.anyRelates(r.getInputCodings(), r.getArgCodings()))
    ).iterator();
  }
}