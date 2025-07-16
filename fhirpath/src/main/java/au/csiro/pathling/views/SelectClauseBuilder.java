package au.csiro.pathling.views;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SelectClauseBuilder {

  @Nonnull
  private final List<Column> column = new ArrayList<>();

  @Nonnull
  private final List<SelectClause> select = new ArrayList<>();

  @Nullable
  private String forEach;

  @Nullable
  private String forEachOrNull;

  @Nonnull
  private final List<SelectClause> unionAll = new ArrayList<>();

  public SelectClauseBuilder column(@Nonnull final Column... column) {
    Collections.addAll(this.column, column);
    return this;
  }

  public SelectClauseBuilder select(@Nonnull final SelectClause... select) {
    Collections.addAll(this.select, select);
    return this;
  }

  public SelectClauseBuilder forEach(@Nonnull final String forEach) {
    this.forEach = forEach;
    return this;
  }

  public SelectClauseBuilder forEachOrNull(@Nonnull final String forEachOrNull) {
    this.forEachOrNull = forEachOrNull;
    return this;
  }

  public SelectClauseBuilder unionAll(@Nonnull final SelectClause... selects) {
    Collections.addAll(this.unionAll, selects);
    return this;
  }

  @Nonnull
  public SelectClause build() {
    return new SelectClause(column, select, forEach, forEachOrNull, unionAll);
  }

}
