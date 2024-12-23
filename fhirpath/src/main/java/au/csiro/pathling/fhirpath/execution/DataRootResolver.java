package au.csiro.pathling.fhirpath.execution;

import static au.csiro.pathling.fhirpath.execution.PathsUtils.asReverseResolve;
import static au.csiro.pathling.fhirpath.execution.PathsUtils.isResolve;
import static au.csiro.pathling.fhirpath.execution.PathsUtils.isReverseResolve;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResolveRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResourceRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import au.csiro.pathling.fhirpath.path.ParserPaths.TypeSpecifierPath;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.fhirpath.path.Paths.EvalFunction;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@Value
public class DataRootResolver {

  ResourceType subjectResource;
  FhirContext fhirContext;


  @Nonnull
  public Set<DataRoot> findDataRoots(@Nonnull final FhirPath path) {
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

      // eval the currenrt reference
      // TODO: make sure that the current root is typed
      final NullEvaluator evaluator = NullEvaluator.of(currentRoot.getResourceType(), fhirContext);
      final ReferenceCollection referenceCollection = (ReferenceCollection) evaluator.evaluate(
          traversalPath);
      final Set<ResourceType> referenceTypes = referenceCollection.getReferenceTypes();

      final ResourceType referenceType = referenceTypes.size() == 1
                                         ? referenceTypes.iterator().next()
                                         : ResourceType.RESOURCE;

      final ResolveRoot resolveRoot = ResolveRoot.of(currentRoot,
          referenceType,
          traversalPath.toExpression());
      // Do not add the untyped root just jest
      // Pehaps it can be typed later\
      if (referenceType != ResourceType.RESOURCE) {
        dataRoots.add(resolveRoot);
      }
      collectDataRoots(resolveRoot, fhirPath.suffix(), FhirPath.nullPath(), dataRoots);
    } else if (PathsUtils.isTypeOf(headPath)) {
      final EvalFunction evalFunction = PathsUtils.asTypeOf(headPath).orElseThrow();
      final TypeSpecifier typeSpecifier = ((TypeSpecifierPath) evalFunction.getArguments()
          .get(0)).getValue();

      // TODO: check if this is a fhir type
      if (currentRoot instanceof ResolveRoot rr && rr.getResourceType() == ResourceType.RESOURCE) {
        final ResolveRoot typedRoot = ResolveRoot.of(rr.getMaster(), typeSpecifier.toResourceType(),
            rr.getMasterResourcePath());
        dataRoots.add(typedRoot);
        collectDataRoots(typedRoot, fhirPath.suffix(), traversalPath, dataRoots);
      } else {
        // TODO: check if we are in a mixed collection
        collectDataRoots(currentRoot, fhirPath.suffix(), traversalPath.andThen(headPath),
            dataRoots);
      }
    } else if (headPath instanceof Paths.ExternalConstantPath ecp) {
      // we do not need to do anything here
      System.out.println("External constant path" + ecp);
      if ("resource".equals(ecp.getName()) || "rootResource".equals(ecp.getName())) {
        // this root should already be addded here
        collectDataRoots(ResourceRoot.of(subjectResource), fhirPath.suffix(), FhirPath.nullPath(),
            dataRoots);
      }
      // NOTE: other paths should be literals so we should not need to resolve them
    } else if (PathsUtils.isMulitPath(headPath)) {
      // combine needs to be processed differentnly 
      // each of the children needs to be processed with the entire suffic
      headPath.children()
          .forEach(child -> collectDataRoots(currentRoot, child.andThen(fhirPath.suffix()),
              traversalPath, dataRoots));

    } else if (!headPath.isNull()) {
      // and also collect the for the children
      headPath.children()
          .forEach(child -> collectDataRoots(currentRoot, child, traversalPath, dataRoots));
      // and then the rest of the path

      // TODO: we should be also need to be abel to check 
      // if the current traversal is to a resource or to a reference
      final FhirPath newTraversalPath = traversalPath.andThen(PathsUtils.toTraversal(headPath));
      collectDataRoots(currentRoot, fhirPath.suffix(), newTraversalPath, dataRoots);
    } else {
      // if we have an untyped resolve root add it here
      if (currentRoot instanceof ResolveRoot rr && rr.getResourceType() == ResourceType.RESOURCE) {
        throw new IllegalStateException("Unresolved resolve root");
      }
    }
  }

}
