/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */
package au.csiro.pathling.aggregate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.UnsignedIntType;


/**
 * @author John Grimes
 */
public class AggreateResponseBuilder {


  private static Type valueToType(Object objValue) {
    if (objValue == null) {
      return null;
    } else if (objValue instanceof Type) {
      return (Type) objValue;
    } else if (objValue instanceof Integer) {
      return new UnsignedIntType((Integer) objValue);
    } else if (objValue instanceof Double) {
      return new DecimalType((Double) objValue);
    } else if (objValue instanceof String) {
      return new StringType((String) objValue);
    } else if (objValue instanceof Boolean) {
      return new BooleanType((Boolean) objValue);
    } else {

      throw new IllegalArgumentException("Cannot determine Type for: " + objValue);
    }
  }

  private final List<AggregateResponse.Grouping> groupings = new ArrayList<>();

  public class GroupBuilder {

    private List<Optional<Type>> labels;
    private List<Optional<Type>> values = Collections.emptyList();
    private Optional<String> drillDown = Optional.empty();

    public GroupBuilder(Object... labels) {
      this.labels = Stream.of(labels).map(AggreateResponseBuilder::valueToType)
          .map(Optional::ofNullable).collect(Collectors.toList());
    }

    public GroupBuilder withValues(final Object... values) {
      this.values = Stream.of(values)
          .map(AggreateResponseBuilder::valueToType)
          .map(Optional::ofNullable).collect(Collectors.toList());
      return this;
    }

    public GroupBuilder withDrillDown(final String drillDown) {
      this.drillDown = Optional.of(drillDown);
      return this;
    }

    public AggreateResponseBuilder done() {

      final AggregateResponse.Grouping newGrouping = new AggregateResponse.Grouping(this.labels,
          this.values, this.drillDown);
      groupings.add(newGrouping);
      return AggreateResponseBuilder.this;
    }

    public AggregateResponse build() {
      return this.done().build();
    }

    public GroupBuilder newGroup(final Object... labels) {
      return this.done().newGroup(labels);
    }


    public GroupBuilder newEmptyGroup() {
      return this.done().newGroup((Object) null);
    }

  }

  public GroupBuilder newGroup(final Object... labels) {
    return new GroupBuilder(labels);
  }

  public AggregateResponse build() {
    return new AggregateResponse(groupings);
  }

  public static AggreateResponseBuilder get() {
    return new AggreateResponseBuilder();
  }

  public static GroupBuilder agg() {
    return get().newGroup();
  }
}
