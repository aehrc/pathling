/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.parsing;

import static au.csiro.clinsight.utilities.Strings.md5Short;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeChildResourceDefinition;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Used to represent the results from the parsing of a FHIRPath expression.
 *
 * @author John Grimes
 */
public class ParsedExpression  implements Joinable {

  // This mapping needs to reflect the mappings that Bunsen and Spark use.
  //
  // The Spark mappings are documented here:
  // https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Row.html#get-int-
  //
  // The Bunsen mappings can be found within the code here:
  // https://github.com/cerner/bunsen/blob/master/bunsen-r4/src/main/scala/com/cerner/bunsen/r4/R4DataTypeMappings.scala
  //
  private static final Map<FHIRDefinedType, Class> FHIR_TYPE_TO_JAVA_CLASS = new EnumMap<FHIRDefinedType, Class>(
      FHIRDefinedType.class) {{
    put(FHIRDefinedType.DECIMAL, BigDecimal.class);
    put(FHIRDefinedType.MARKDOWN, String.class);
    put(FHIRDefinedType.ID, String.class);
    put(FHIRDefinedType.DATETIME, String.class);
    put(FHIRDefinedType.TIME, String.class);
    put(FHIRDefinedType.DATE, String.class);
    put(FHIRDefinedType.CODE, String.class);
    put(FHIRDefinedType.STRING, String.class);
    put(FHIRDefinedType.URI, String.class);
    put(FHIRDefinedType.URL, String.class);
    put(FHIRDefinedType.CANONICAL, String.class);
    put(FHIRDefinedType.INTEGER, Integer.class);
    put(FHIRDefinedType.UNSIGNEDINT, Long.class);
    put(FHIRDefinedType.POSITIVEINT, Integer.class);
    put(FHIRDefinedType.BOOLEAN, Boolean.class);
    put(FHIRDefinedType.INSTANT, Timestamp.class);
    // TODO: Data types not catered for: base64Binary, oid, uuid, enumeration of codes.
  }};

  static final SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat(
      "yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
  static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("'T'HH:mm:ss.SSSXXX");

  /**
   * The FHIRPath representation of this expression.
   */
  private String fhirPath;

  /**
   * Information about the FHIRPath type returned by this expression, if primitive or literal.
   */
  private FhirPathType fhirPathType;

  /**
   * Information about the FHIR data type returned by this expression.
   */
  private FHIRDefinedType fhirType;

  /**
   * Definitional information about the type of this element from the FHIR specification.
   */
  private BaseRuntimeChildDefinition definition;

  /**
   * Definitional information about the type of this element from the FHIR specification. Contains
   * different information from the child definition, e.g. child information.
   */
  private BaseRuntimeElementDefinition elementDefinition;

  /**
   * If this expression evaluates to a resource, this flag will be set.
   */
  private boolean isResource;

  /**
   * If this expression evaluates to a resource, this field holds the type of the resource.
   */
  private Enumerations.ResourceType resourceType;

  /**
   * Flag indicating whether this expression evaluates to a collection of primitive values.
   */
  private boolean primitive;

  /**
   * The literal value of this expression, if any.
   */
  private Type literalValue;

  /**
   * Flag indicating whether this expression evaluates to a collection with a single item.
   */
  private boolean singular;

  /**
   * If this expression is an unresolved polymorphic result, e.g. the result of a call to resolve()
   * on a reference with multiple types, this flag will be set.
   */
  private boolean polymorphic;

  /**
   * A reference to the ParseResult at the beginning of this expression. This is used for reverse
   * reference resolution, in order to get back to the origin resource within the argument. It only
   * needs to be populated at the start of expressions and within path traversal.
   */
  private ParsedExpression origin;

  /**
   * A Spark dataset that represents the query required to access the data described by this
   * expression.
   */
  private Dataset<Row> dataset;

  /**
   * The column within the dataset that contains the ID of the subject resource.
   */
  private Column idColumn;

  /**
   * The column within the dataset that contains the result of this expression.
   */
  private Column valueColumn;

  /**
   * For unresolved polymorphic references, this column holds the resource type code.
   */
  private Column resourceTypeColumn;

  
  /**
   * For aggregation expression, this dataset holds the dataset that grouped aggregation should be performed over 
   */
 
  private Dataset<Row> aggregationDataset;
  /**
   * For aggregate expressions, this column hold the unresolved aggregation that will be applied
   * during execution.
   */
  private Column aggregationColumn;

  private Column aggregationIdColumn;
  
	public ParsedExpression() {
  }

  public ParsedExpression(ParsedExpression parsedExpression) {
    this.fhirPath = parsedExpression.fhirPath;
    this.fhirPathType = parsedExpression.fhirPathType;
    this.fhirType = parsedExpression.fhirType;
    this.definition = parsedExpression.definition;
    this.elementDefinition = parsedExpression.elementDefinition;
    this.isResource = parsedExpression.isResource;
    this.resourceType = parsedExpression.resourceType;
    this.primitive = parsedExpression.primitive;
    this.literalValue = parsedExpression.literalValue;
    this.singular = parsedExpression.singular;
    this.polymorphic = parsedExpression.polymorphic;
    this.origin = parsedExpression.origin;
    this.dataset = parsedExpression.dataset;
    this.idColumn = parsedExpression.idColumn;
    this.valueColumn = parsedExpression.valueColumn;
    this.resourceTypeColumn = parsedExpression.resourceTypeColumn;
    this.aggregationDataset = parsedExpression.aggregationDataset;
    this.aggregationColumn = parsedExpression.aggregationColumn;
    this.aggregationIdColumn = parsedExpression.aggregationIdColumn; 
  }

  public String getFhirPath() {
    return fhirPath;
  }

  public void setFhirPath(String fhirPath) {
    this.fhirPath = fhirPath;
  }

  public FhirPathType getFhirPathType() {
    return fhirPathType;
  }

  public void setFhirPathType(FhirPathType fhirPathType) {
    this.fhirPathType = fhirPathType;
  }

  public FHIRDefinedType getFhirType() {
    return fhirType;
  }

  public void setFhirType(FHIRDefinedType fhirType) {
    this.fhirType = fhirType;
  }

  public BaseRuntimeChildDefinition getDefinition() {
    return definition;
  }

  /**
   * Sets both the (child) definition and element definition.
   */
  public void setDefinition(BaseRuntimeChildDefinition definition, String elementName) {
    this.definition = definition;
    this.elementDefinition = elementFromChildDefinition(definition, elementName);
  }

  public BaseRuntimeElementDefinition getElementDefinition() {
    return elementDefinition;
  }

  public boolean isResource() {
    return isResource;
  }

  public void setResource(boolean resource) {
    isResource = resource;
  }

  public Enumerations.ResourceType getResourceType() {
    return resourceType;
  }

  public void setResourceType(Enumerations.ResourceType resourceType) {
    this.resourceType = resourceType;
  }

  public boolean isPrimitive() {
    return primitive;
  }

  public void setPrimitive(boolean primitive) {
    this.primitive = primitive;
  }

  public Type getLiteralValue() {
    return literalValue;
  }

  public void setLiteralValue(Type literalValue) {
    this.literalValue = literalValue;
  }

  public boolean isSingular() {
    return singular;
  }

  public void setSingular(boolean singular) {
    this.singular = singular;
  }

  public boolean isPolymorphic() {
    return polymorphic;
  }

  public void setPolymorphic(boolean polymorphic) {
    this.polymorphic = polymorphic;
  }

  public ParsedExpression getOrigin() {
    return origin;
  }

  public void setOrigin(ParsedExpression origin) {
    this.origin = origin;
  }

  @Override
  public Dataset<Row> getDataset() {
    return dataset;
  }

  public void setDataset(Dataset<Row> dataset) {
    this.dataset = dataset;
  }

  @Override
  public Column getIdColumn() {
    return idColumn;
  }

  public void setIdColumn(Column idColumn) {
    this.idColumn = idColumn;
  }

  public Column getValueColumn() {
    return valueColumn;
  }

  public void setValueColumn(Column valueColumn) {
    this.valueColumn = valueColumn;
  }

  public Column getResourceTypeColumn() {
    return resourceTypeColumn;
  }

  public void setResourceTypeColumn(Column resourceTypeColumn) {
    this.resourceTypeColumn = resourceTypeColumn;
  }
  
  public Dataset<Row> getAggregationDataset() {
		return aggregationDataset;
	}

	public void setAggregationDataset(Dataset<Row> aggregationDataset) {
		this.aggregationDataset = aggregationDataset;
	}
  public Column getAggregationColumn() {
    return aggregationColumn;
  }

  public void setAggregationColumn(Column aggregationColumn) {
    this.aggregationColumn = aggregationColumn;
  }
  
	public Column getAggregationIdColumn() {
		return aggregationIdColumn;
	}

	public void setAggregationIdColumn(Column aggregationIdColumn) {
		this.aggregationIdColumn = aggregationIdColumn;
	}
  
	
	public Joinable getAggreationJoinable() {
		return new Joinable() {
			@Override
			public Dataset<Row> getDataset() {
				return getAggregationDataset();
			}

			@Override
			public Column getIdColumn() {
				return getAggregationIdColumn();
			}
		};
	}
	
  public Object getJavaLiteralValue() {
    assert PrimitiveType.class.isAssignableFrom(literalValue.getClass()) :
        "Encountered non-primitive literal value";
    return ((PrimitiveType) literalValue).getValue();
  }

  /**
   * Retrieves the HAPI class that should be used to represent the type of this expression.
   */
  public Class getImplementingClass(FhirContext fhirContext) {
    if (definition == null && fhirType == null) {
      throw new IllegalStateException("Expression has no definition or FHIR type");
    }
    if (definition == null) {
      if (fhirContext == null) {
        throw new IllegalStateException(
            "Expression has no definition, and null FHIR context passed");
      }
      return fhirContext.getElementDefinition(fhirType.toCode()).getImplementingClass();
    } else {
      return definition.getChildByName(definition.getElementName()).getImplementingClass();
    }
  }

  /**
   * Retrieves the plain Java class that should be used to represent the type of this expression.
   */
  public Class getJavaClass() {
    return FHIR_TYPE_TO_JAVA_CLASS.get(fhirType);
  }

  /**
   * If the type of this expression is a Reference, get the resource types that it can refer to.
   */
  public Set<Enumerations.ResourceType> getReferenceResourceTypes() {
    if (!(definition instanceof RuntimeChildResourceDefinition)) {
      throw new IllegalStateException("Definition is not a Reference");
    }
    RuntimeChildResourceDefinition resourceDefinition = (RuntimeChildResourceDefinition) definition;
    return resourceDefinition.getResourceTypes().stream()
        .map(clazz -> {
          String resourceCode;
          try {
            resourceCode = clazz.getConstructor().newInstance().fhirType();
            return Enumerations.ResourceType.fromCode(resourceCode);
          } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("Problem accessing resource types on element", e);
          }
        })
        .collect(Collectors.toSet());
  }

  /**
   * Creates new columns for ID and value, and renames them to be hashes of the FHIRPath
   * expression.
   */
  public void setHashedValue(Column idColumn, Column valueColumn) {
    if (dataset == null || fhirPath == null) {
      throw new IllegalArgumentException("dataset and fhirPath must be set");
    }
    String hash = md5Short(fhirPath);
    dataset = dataset.withColumn(hash + "_id", idColumn);
    dataset = dataset.withColumn(hash, valueColumn);
    this.idColumn = dataset.col(hash + "_id");
    this.valueColumn = dataset.col(hash);
  }
    
  /**
   * Get the FHIR type from a BaseRuntimeChildDefinition.
   */
  public static FHIRDefinedType fhirTypeFromDefinition(BaseRuntimeChildDefinition definition,
      String elementName) {
    IBase exampleObject = definition.getChildByName(elementName).newInstance();
    return FHIRDefinedType.fromCode(exampleObject.fhirType());
  }

  /**
   * Get the BaseRuntimeElementDefinition for a BaseRuntimeChildDefinition.
   */
  public static BaseRuntimeElementDefinition elementFromChildDefinition(
      BaseRuntimeChildDefinition childDefinition, String elementName) {
    return childDefinition.getChildByName(elementName);
  }

	/**
   * Describes a ParseResult in terms of the FHIRPath type that it evaluates to.
   */
  public enum FhirPathType {
    // See http://hl7.org/fhirpath/2018Sep/index.html#expressions.
    BOOLEAN(BooleanType.class, "Boolean"),
    STRING(StringType.class, "String"),
    INTEGER(IntegerType.class, "Integer"),
    DECIMAL(DecimalType.class, "Decimal"),
    DATE(DateType.class, "Date"),
    DATE_TIME(DateTimeType.class, "DateTime"),
    TIME(TimeType.class, "Time"),
    QUANTITY(Quantity.class, "Quantity"),
    // The Coding data type does not exist in the FHIRPath spec, this is currently unique to our
    // implementation.
    CODING(Coding.class, "Coding");

    // See https://hl7.org/fhir/fhirpath.html#types.
    private static final Map<FHIRDefinedType, FhirPathType> fhirTypeCodeToFhirPathType = new EnumMap<FHIRDefinedType, FhirPathType>(
        FHIRDefinedType.class) {{
      put(FHIRDefinedType.BOOLEAN, BOOLEAN);
      put(FHIRDefinedType.STRING, STRING);
      put(FHIRDefinedType.URI, STRING);
      put(FHIRDefinedType.CODE, STRING);
      put(FHIRDefinedType.OID, STRING);
      put(FHIRDefinedType.ID, STRING);
      put(FHIRDefinedType.UUID, STRING);
      put(FHIRDefinedType.MARKDOWN, STRING);
      put(FHIRDefinedType.BASE64BINARY, STRING);
      put(FHIRDefinedType.INTEGER, INTEGER);
      put(FHIRDefinedType.UNSIGNEDINT, INTEGER);
      put(FHIRDefinedType.POSITIVEINT, INTEGER);
      put(FHIRDefinedType.DECIMAL, DECIMAL);
      put(FHIRDefinedType.DATE, DATE_TIME);
      put(FHIRDefinedType.DATETIME, DATE_TIME);
      put(FHIRDefinedType.INSTANT, DATE_TIME);
      put(FHIRDefinedType.TIME, TIME);
      put(FHIRDefinedType.CODING, CODING);
    }};

    private static final Set<FhirPathType> primitiveTypes = new HashSet<>(
        Arrays.asList(BOOLEAN, STRING, INTEGER, DECIMAL, DATE, DATE_TIME, TIME));

    // Java class that can be used for representing the value of this expression.
    @Nonnull
    private final Class fhirType;

    // One of the data types defined in the FHIRPath specification.
    @Nonnull
    private final String fhirPathType;

    FhirPathType(@Nonnull Class fhirType, @Nonnull String fhirPathType) {
      this.fhirType = fhirType;
      this.fhirPathType = fhirPathType;
    }

    // Maps a FHIR type code to a FHIRPath data type.
    public static FhirPathType forFhirTypeCode(FHIRDefinedType fhirTypeCode) {
      return fhirTypeCodeToFhirPathType.get(fhirTypeCode);
    }

    public boolean isPrimitive() {
      return primitiveTypes.contains(this);
    }

    @Nonnull
    public Class getFhirType() {
      return fhirType;
    }

    @Nonnull
    public String getFhirPathType() {
      return fhirPathType;
    }
  }
}
