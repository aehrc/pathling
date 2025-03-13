package au.csiro.pathling.fhirpath.execution;

import static au.csiro.pathling.fhirpath.FhirPathConstants.PredefinedVariables.RESOURCE;
import static au.csiro.pathling.fhirpath.FhirPathConstants.PredefinedVariables.ROOT_RESOURCE;
import static au.csiro.pathling.fhirpath.execution.FhirPathsUtils.asResolve;
import static au.csiro.pathling.fhirpath.execution.FhirPathsUtils.asReverseResolve;
import static au.csiro.pathling.fhirpath.execution.FhirPathsUtils.isResource;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.definition.ResourceTypeSet;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResolveRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResourceRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import au.csiro.pathling.fhirpath.path.ParserPaths.TypeSpecifierPath;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.fhirpath.path.Paths.EvalFunction;
import au.csiro.pathling.fhirpath.path.Paths.Resource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@Value
@Slf4j
public class DataRootResolver {

  @Nonnull
  ResourceType subjectResource;

  @Nonnull
  FhirContext fhirContext;


  @Nonnull
  public Set<DataRoot> findDataRoots(@Nonnull final FhirPath path) {
    final ResourceRoot subjectRoot = ResourceRoot.of(subjectResource);
    final Set<DataRoot> dataRoots = new HashSet<>();
    dataRoots.add(subjectRoot);
    collectDataRoots(subjectRoot, path, FhirPath.nullPath(), dataRoots);
    return Collections.unmodifiableSet(dataRoots);
  }


  @Nonnull
  public Set<DataRoot> findDataRoots(@Nonnull final Collection<FhirPath> paths) {
    // always include this as  context path
    return Stream.concat(Stream.of(new Paths.This()), paths.stream())
        .map(this::findDataRoots)
        .flatMap(Collection::stream)
        .collect(Collectors.toUnmodifiableSet());
  }

  private void collectDataRoots(@Nonnull final DataRoot contextRoot,
      @Nonnull final FhirPath fhirPath,
      @Nonnull final FhirPath traversalPath,
      @Nonnull final Set<DataRoot> dataRoots) {

    final FhirPath headPath = fhirPath.first();
    if (isResource(headPath)) {
      // add this resource root and use it as the contex for the rest of the path
      final ResourceRoot resourceRoot = ResourceRoot.of(((Resource) headPath).getResourceType());
      dataRoots.add(resourceRoot);
      collectDataRoots(resourceRoot, fhirPath.suffix(), traversalPath, dataRoots);
    } else if (asReverseResolve(headPath) instanceof EvalFunction reverseResolve) {
      final ReverseResolveRoot reverseResolveRoot = ExecutorUtils.fromPath(contextRoot,
          reverseResolve);
      dataRoots.add(reverseResolveRoot);
      // switch the context root to the new reverse resolve root
      collectDataRoots(reverseResolveRoot, fhirPath.suffix(), FhirPath.nullPath(), dataRoots);
    } else if (asResolve(headPath) instanceof EvalFunction) {
      // this here is problematic if we need to deal with polymorphic references
      // but in general I need to create a root
      // TODO: where should we do the validation of reference types? 

      // eval the currenrt reference
      // TODO: make sure that the current root is typed
      final FhirpathEvaluator evaluator = NullEvaluator.of(contextRoot.getResourceType(),
          fhirContext);
      final ReferenceCollection referenceCollection = (ReferenceCollection) evaluator.evaluate(
          traversalPath);
      final ResourceTypeSet referenceTypes = referenceCollection.getReferenceTypes();

      final ResourceType referenceType = referenceTypes.asSingleResourceType()
          .orElse(ResourceType.RESOURCE);

      final ResolveRoot resolveRoot = ResolveRoot.of(contextRoot,
          referenceType,
          traversalPath.toExpression());
      // Do not add the untyped root just jest
      // Pehaps it can be typed later\
      if (referenceType != ResourceType.RESOURCE) {
        dataRoots.add(resolveRoot);
      }
      collectDataRoots(resolveRoot, fhirPath.suffix(), FhirPath.nullPath(), dataRoots);
    } else if (FhirPathsUtils.isTypeOf(headPath)) {
      final EvalFunction evalFunction = FhirPathsUtils.asTypeOf(headPath).orElseThrow();
      final TypeSpecifier typeSpecifier = ((TypeSpecifierPath) evalFunction.getArguments()
          .get(0)).getValue();

      // TODO: check if this is a fhir type
      if (contextRoot instanceof ResolveRoot rr && rr.getResourceType() == ResourceType.RESOURCE) {
        final ResolveRoot typedRoot = ResolveRoot.of(rr.getMaster(), typeSpecifier.toResourceType(),
            rr.getMasterResourcePath());
        dataRoots.add(typedRoot);
        collectDataRoots(typedRoot, fhirPath.suffix(), traversalPath, dataRoots);
      } else {
        // TODO: check if we are in a mixed collection
        collectDataRoots(contextRoot, fhirPath.suffix(), traversalPath.andThen(headPath),
            dataRoots);
      }
    } else if (headPath instanceof Paths.ExternalConstantPath ecp) {
      if (RESOURCE.equals(ecp.getName()) || ROOT_RESOURCE.equals(ecp.getName())) {
        // we do not need to create a new root for this as is the subject root
        // but we need to switch context
        collectDataRoots(ResourceRoot.of(subjectResource), fhirPath.suffix(), FhirPath.nullPath(),
            dataRoots);
      }
      // NOTE: other paths should be literals so we should not need to resolve them
    } else if (FhirPathsUtils.isPropagatesArguments(headPath)) {
      // some paths such as combine or iif needs to be processed differently as one of more of 
      // their arguments may need to be further resolved
      // for example `iif(..., resolve().id).ofType(Condition)` the true branch of the iif 
      // needs to be resolved to identyfy the type of the reference
      FhirPathsUtils.gePropagatesArguments(headPath)
          .forEach(head -> collectDataRoots(contextRoot, head.andThen(fhirPath.suffix()),
              traversalPath, dataRoots));
    } else if (!headPath.isNull()) {
      // and also collect the for the children
      headPath.children()
          .forEach(child -> collectDataRoots(contextRoot, child, traversalPath, dataRoots));
      // and then the rest of the path

      // TODO: we should be also need to be abel to check 
      // if the current traversal is to a resource or to a reference
      final FhirPath newTraversalPath = traversalPath.andThen(FhirPathsUtils.toTraversal(headPath));
      collectDataRoots(contextRoot, fhirPath.suffix(), newTraversalPath, dataRoots);
    } else {
      // if we have an untyped resolve root add it here
      if (contextRoot instanceof ResolveRoot rr && rr.getResourceType() == ResourceType.RESOURCE) {
        throw new IllegalStateException("Unresolved resolve root");
      }
    }
  }

}
