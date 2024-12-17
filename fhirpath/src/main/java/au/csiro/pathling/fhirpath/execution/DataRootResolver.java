package au.csiro.pathling.fhirpath.execution;

import static au.csiro.pathling.fhirpath.execution.ResolvingFhirPathEvaluator.asReverseResolve;
import static au.csiro.pathling.fhirpath.execution.ResolvingFhirPathEvaluator.isResolve;
import static au.csiro.pathling.fhirpath.execution.ResolvingFhirPathEvaluator.isReverseResolve;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResolveRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResourceRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import au.csiro.pathling.fhirpath.path.Paths;
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
    // TODO pehaps it should be This not nullPath
    collectDataRoots(subjectRoot, path, FhirPath.nullPath(), dataRoots);
    return Collections.unmodifiableSet(dataRoots);
  }

  public void collectDataRoots(@Nonnull final DataRoot currentRoot,
      @Nonnull final FhirPath fhirPath,
      @Nonnull final FhirPath traversalPath,
      @Nonnull final Set<DataRoot> dataRoots) {

    final FhirPath headPath = fhirPath.first();
    if (isReverseResolve(headPath)) {
      final ReverseResolveRoot reverseResolveRoot = ExecutorUtils.fromPath(currentRoot,
          asReverseResolve(headPath).orElseThrow());
      dataRoots.add(reverseResolveRoot);
      // We start with a new tracking contxct
      collectDataRoots(reverseResolveRoot, fhirPath.suffix(), FhirPath.nullPath(), dataRoots);
    } else if (isResolve(headPath)) {
      // this here is problematic if we need to deal with polymorphic references
      // but in general I need to create a root
      // TODO: where should we do the validation of reference types? 
      final ResolveRoot resolveRoot = ResolveRoot.of(currentRoot,
          ResourceType.RESOURCE,
          traversalPath.toExpression());
      dataRoots.add(resolveRoot);
      collectDataRoots(resolveRoot, fhirPath.suffix(), FhirPath.nullPath(), dataRoots);
    } else if (!headPath.isNull()) {
      // and also collect the for the children
      headPath.children()
          .forEach(child -> collectDataRoots(currentRoot, child, traversalPath, dataRoots));
      // and then the rest of the path

      // TODO: we should be also need to be abel to check 
      // if the current traversal is to a resource or to a reference
      final FhirPath newTraversalPath = isTraversal(headPath)
                                        ? traversalPath.andThen(headPath)
                                        : traversalPath;
      collectDataRoots(currentRoot, fhirPath.suffix(), newTraversalPath, dataRoots);
    }
  }

  private static boolean isTraversal(@Nonnull final FhirPath path) {
    return path instanceof Paths.Traversal;
  }
}
