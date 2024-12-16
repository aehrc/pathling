package au.csiro.pathling.fhirpath.execution;

import static au.csiro.pathling.fhirpath.execution.ResolvingFhirPathEvaluator.asReverseResolve;
import static au.csiro.pathling.fhirpath.execution.ResolvingFhirPathEvaluator.isReverseResolve;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResourceRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@Value
public class DataRootResolver {

  @Nonnull
  public Set<DataRoot> findDataRoots(@Nonnull final ResourceType subjectResource,
      @Nonnull final FhirPath path) {
    final ResourceRoot subjectRoot = ResourceRoot.of(subjectResource);
    final Set<DataRoot> dataRoots = new HashSet<>();
    dataRoots.add(subjectRoot);
    collectDataRoots(subjectRoot, path, dataRoots);
    return Collections.unmodifiableSet(dataRoots);
  }

  public void collectDataRoots(@Nonnull final DataRoot currentRoot,
      @Nonnull final FhirPath fhirPath, @Nonnull final Set<DataRoot> dataRoots) {

    final FhirPath headPath = fhirPath.first();

    if (isReverseResolve(headPath)) {
      final ReverseResolveRoot joinRoot = ExecutorUtils.fromPath(currentRoot,
          asReverseResolve(headPath).orElseThrow());
      dataRoots.add(joinRoot);
      collectDataRoots(joinRoot, fhirPath.suffix(), dataRoots);
    } else if (!headPath.isNull()) {
      // and also collect the for the children
      headPath.children().forEach(child -> collectDataRoots(currentRoot, child, dataRoots));
      collectDataRoots(currentRoot, fhirPath.suffix(), dataRoots);
    }
  }
}
