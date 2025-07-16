package au.csiro.pathling.views;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.hl7.fhir.instance.model.api.IBase;

public class FhirViewBuilder {

  @Nullable
  private String resource;

  @Nonnull
  private final List<ConstantDeclaration> constant = new ArrayList<>();

  @Nonnull
  private final List<SelectClause> select = new ArrayList<>();

  @Nonnull
  private final List<WhereClause> where = new ArrayList<>();

  public FhirViewBuilder resource(@NotNull final String resource) {
    this.resource = resource;
    return this;
  }

  public FhirViewBuilder constant(@Nonnull final ConstantDeclaration... constant) {
    Collections.addAll(this.constant, constant);
    return this;
  }

  public FhirViewBuilder constant(@Nonnull final String name, @Nonnull final IBase value) {
    this.constant.add(new ConstantDeclaration(name, value));
    return this;
  }

  public FhirViewBuilder select(@Nonnull final SelectClause... select) {
    Collections.addAll(this.select, select);
    return this;
  }

  public FhirViewBuilder where(@Nonnull final WhereClause... where) {
    Collections.addAll(this.where, where);
    return this;
  }

  public FhirViewBuilder where(@Nonnull final String... where) {
    for (final String condition : where) {
      this.where.add(new WhereClause(condition, null));
    }
    return this;
  }

  public FhirViewBuilder where(@Nonnull final String condition,
      @Nullable final String description) {
    this.where.add(new WhereClause(condition, description));
    return this;
  }

  public FhirView build() {
    checkArgument(resource != null, "Resource must be set");
    return new FhirView(resource, constant, select, where);
  }

}
