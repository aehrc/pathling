package au.csiro.pathling.views;

import java.util.List;

public abstract class NestedSelectClause extends SelectClause {

  abstract String getExpression();

  abstract List<SelectClause> getSelect();

}
