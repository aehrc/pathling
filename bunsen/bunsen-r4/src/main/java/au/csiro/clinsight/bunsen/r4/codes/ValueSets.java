/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © Australian e-Health Research Centre,
 * CSIRO. All rights reserved.
 */

package au.csiro.clinsight.bunsen.r4.codes;

import static org.apache.spark.sql.functions.lit;

import ca.uhn.fhir.context.FhirVersionEnum;
import au.csiro.clinsight.bunsen.FhirEncoders;
import au.csiro.clinsight.bunsen.codes.UrlAndVersion;
import au.csiro.clinsight.bunsen.codes.Value;
import au.csiro.clinsight.bunsen.codes.base.AbstractValueSets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.ValueSet;
import org.hl7.fhir.r4.model.ValueSet.ConceptReferenceComponent;
import org.hl7.fhir.r4.model.ValueSet.ConceptSetComponent;
import org.hl7.fhir.r4.model.ValueSet.ValueSetComposeComponent;

/**
 * An immutable collection of FHIR ValueSets. This class is used to import value set content,
 * explore it, and persist it to a database.
 */
public class ValueSets extends AbstractValueSets<ValueSet, ValueSets> {


  private static final Encoder<ValueSet> VALUE_SET_ENCODER = FhirEncoders.forR4()
      .getOrCreate()
      .of(ValueSet.class);

  /**
   * Returns the encoder for value sets.
   *
   * @return the encoder for value sets.
   */
  public static Encoder<ValueSet> getValueSetEncoder() {
    return VALUE_SET_ENCODER;
  }

  private ValueSets(SparkSession spark,
      Dataset<UrlAndVersion> members,
      Dataset<ValueSet> valueSets,
      Dataset<Value> values) {

    super(spark, FhirVersionEnum.R4, members, valueSets, values, VALUE_SET_ENCODER);
  }

  /**
   * Returns the collection of value sets from the default database and tables.
   *
   * @param spark the spark session
   * @return a ValueSets instance.
   */
  public static ValueSets getDefault(SparkSession spark) {

    return getFromDatabase(spark, VALUE_SETS_DATABASE);
  }

  /**
   * Returns the collection of value sets from the tables in the given database.
   *
   * @param spark the spark session
   * @param databaseName name of the database containing the value sets and values tables
   * @return a ValueSets instance.
   */
  public static ValueSets getFromDatabase(SparkSession spark, String databaseName) {

    Dataset<Value> values = spark.table(databaseName + "." + VALUES_TABLE).as(getValueEncoder());

    Dataset<ValueSet> valueSets = spark.table(databaseName + "." + VALUE_SETS_TABLE)
        .as(VALUE_SET_ENCODER);

    Dataset<UrlAndVersion> members = valueSets.select("url", "version").as(URL_AND_VERSION_ENCODER);

    return new ValueSets(spark,
        members,
        valueSets,
        values);
  }

  /**
   * Returns an empty ValueSets instance.
   *
   * @param spark the spark session
   * @return an empty ValueSets instance.
   */
  public static ValueSets getEmpty(SparkSession spark) {

    Dataset<ValueSet> emptyValueSets = spark.emptyDataset(VALUE_SET_ENCODER)
        .withColumn("timestamp", lit(null).cast("timestamp"))
        .as(VALUE_SET_ENCODER);

    return new ValueSets(spark,
        spark.emptyDataset(URL_AND_VERSION_ENCODER),
        emptyValueSets,
        spark.emptyDataset(getValueEncoder()));
  }

  /**
   * Returns a new ValueSets instance that includes the given value sets.
   *
   * @param valueSets the value sets to add to the returned collection.
   * @return a new ValueSets instance with the added value sets.
   */
  @Override
  public ValueSets withValueSets(Dataset<ValueSet> valueSets) {

    Dataset<UrlAndVersion> newMembers = getUrlAndVersions(valueSets);

    // Ensure that there are no duplicates among the value sets
    if (hasDuplicateUrlAndVersions(newMembers) || valueSets.count() != newMembers.count()) {

      throw new IllegalArgumentException(
          "Cannot add value sets having duplicate valueSetUri and valueSetVersion");
    }

    // The value set concepts will be stored in the values table for persistence, so we remove
    // them from the individual value sets. This can be done most easily by setting concepts to an
    // empty list.
    Dataset<ValueSet> withoutConcepts = valueSets
        .map((MapFunction<ValueSet, ValueSet>) valueSet -> {
          ValueSet valueSetWithoutConcepts = valueSet.copy();

          List<ConceptSetComponent> updatedInclusions = new ArrayList<>();

          for (ConceptSetComponent inclusion : valueSet.getCompose().getInclude()) {

            ConceptSetComponent inclusionWithoutConcepts = inclusion.copy();

            inclusionWithoutConcepts.setConcept(new ArrayList<>());
            updatedInclusions.add(inclusionWithoutConcepts);
          }

          valueSetWithoutConcepts.getCompose().setInclude(updatedInclusions);

          return valueSetWithoutConcepts;
        }, VALUE_SET_ENCODER);

    Dataset<Value> newValues = valueSets.flatMap(ValueSets::expandValuesIterator,
        getValueEncoder());

    return withValueSets(withoutConcepts, newValues);
  }

  private ValueSets withValueSets(Dataset<ValueSet> newValueSets, Dataset<Value> newValues) {

    Dataset<UrlAndVersion> newMembers = getUrlAndVersions(newValueSets);

    // Instantiating a new composite ConceptMaps requires a new timestamp
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    Dataset<ValueSet> newValueSetsWithTimestamp = newValueSets
        .withColumn("timestamp", lit(timestamp.toString()).cast("timestamp"))
        .as(VALUE_SET_ENCODER);

    return new ValueSets(spark,
        this.members.union(newMembers),
        this.valueSets.union(newValueSetsWithTimestamp),
        this.values.union(newValues));
  }

  /**
   * Given a value set, returns a list of value records it contains.
   *
   * @param valueSet a value set
   * @return a list of Value records.
   */
  public static List<Value> expandValues(ValueSet valueSet) {

    List<Value> values = new ArrayList<>();

    expandValuesIterator(valueSet).forEachRemaining(values::add);

    return values;
  }

  private static Iterator<Value> expandValuesIterator(ValueSet valueSet) {

    List<Value> values = new ArrayList<>();

    ValueSetComposeComponent compose = valueSet.getCompose();

    for (ConceptSetComponent inclusion : compose.getInclude()) {

      for (ConceptReferenceComponent concept : inclusion.getConcept()) {

        Value value = new Value();

        value.setValueSetUri(valueSet.getUrl());
        value.setValueSetVersion(valueSet.getVersion());

        value.setSystem(inclusion.getSystem());
        value.setVersion(inclusion.getVersion());

        value.setValue(concept.getCode());

        values.add(value);
      }
    }

    return values.iterator();
  }

  @Override
  protected void addToValueSet(ValueSet valueSet, Dataset<Value> values) {

    ValueSetComposeComponent composeComponent = valueSet.getCompose();
    ConceptSetComponent currentInclusion = null;
    ConceptReferenceComponent concept = null;

    List<Value> sortedValues = values.sort("system", "version", "value").collectAsList();

    // Workaround for the decoder producing an immutable array by replacing it with a mutable one
    composeComponent.setInclude(new ArrayList<>(composeComponent.getInclude()));
    for (Value value : sortedValues) {

      if (currentInclusion == null
          || !value.getSystem().equals(currentInclusion.getSystem())
          || !value.getVersion().equals(currentInclusion.getVersion())) {

        // Find a matching inclusion
        for (ConceptSetComponent candidate : composeComponent.getInclude()) {

          if (value.getSystem().equals(candidate.getSystem())
              && value.getVersion().equals(candidate.getVersion())) {

            currentInclusion = candidate;

            // Workaround for the decoder producing an immutable array by replacing it with a
            // mutable one
            currentInclusion.setConcept(new ArrayList<>(currentInclusion.getConcept()));
          }
        }

        // No matching inclusion found, so add one
        if (currentInclusion == null) {

          currentInclusion = composeComponent.addInclude();

          currentInclusion.setSystem(value.getSystem());
          currentInclusion.setVersion(value.getVersion());

          concept = null;
        }
      }

      // Create concept if not exists
      if (concept == null || !value.getValue().equals(concept.getCode())) {

        concept = currentInclusion.addConcept();
        concept.setCode(value.getValue());
      }
    }
  }
}
