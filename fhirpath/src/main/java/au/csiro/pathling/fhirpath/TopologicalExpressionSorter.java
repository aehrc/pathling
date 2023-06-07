package au.csiro.pathling.fhirpath;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

public class TopologicalExpressionSorter implements Comparator<FhirPathAndContext> {

  @Override
  public int compare(final FhirPathAndContext fpc1, final FhirPathAndContext fpc2) {
    final Set<Object> nodes1 = fpc1.getContext().getNodeIdColumns().keySet();
    final Set<Object> nodes2 = fpc2.getContext().getNodeIdColumns().keySet();

    final Set<Object> intersection = new HashSet<>(nodes1);
    intersection.retainAll(nodes2);

    return nodes1.size() - intersection.size() - (nodes2.size() - intersection.size());
  }

}
