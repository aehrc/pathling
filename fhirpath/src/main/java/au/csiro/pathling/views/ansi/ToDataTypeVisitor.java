package au.csiro.pathling.views.ansi;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

/**
 * Visitor that converts parsed ANSI SQL type syntax into Spark SQL DataTypes.
 */
public class ToDataTypeVisitor extends AnsiSqlTypeBaseVisitor<DataType> {

  // Patterns to extract parameters from type strings
  private static final Pattern CHAR_PATTERN = 
      Pattern.compile("(?:CHARACTER|CHAR)(?:\\s+VARYING)?\\s*\\(\\s*(\\d+)\\s*\\)");
  private static final Pattern VARCHAR_PATTERN = 
      Pattern.compile("VARCHAR\\s*\\(\\s*(\\d+)\\s*\\)");
  private static final Pattern NUMERIC_PATTERN = 
      Pattern.compile("(?:NUMERIC|DECIMAL|DEC)\\s*\\(\\s*(\\d+)(?:\\s*,\\s*(\\d+))?\\s*\\)");
  private static final Pattern FLOAT_PATTERN = 
      Pattern.compile("FLOAT\\s*\\(\\s*(\\d+)\\s*\\)");
  private static final Pattern BINARY_PATTERN = 
      Pattern.compile("(?:BINARY|VARBINARY)(?:\\s+VARYING)?\\s*\\(\\s*(\\d+)\\s*\\)");
  private static final Pattern TIMESTAMP_PATTERN = 
      Pattern.compile("TIMESTAMP(?:\\s*\\(\\s*(\\d+)\\s*\\))?\\s*(WITH|WITHOUT)?\\s*TIME\\s*ZONE?");

  @Override
  public DataType visitSqlType(@Nonnull final AnsiSqlTypeParser.SqlTypeContext ctx) {
    final String typeText = ctx.getText().toUpperCase();
    
    // Character types
    if (typeText.startsWith("CHARACTER") || typeText.startsWith("CHAR") || 
        typeText.startsWith("VARCHAR")) {
      return parseCharacterType(typeText);
    }
    
    // Numeric types
    else if (typeText.startsWith("NUMERIC") || typeText.startsWith("DECIMAL") || 
        typeText.startsWith("DEC") || typeText.equals("SMALLINT") || 
        typeText.equals("INTEGER") || typeText.equals("INT") || 
        typeText.equals("BIGINT") || typeText.startsWith("FLOAT") || 
        typeText.equals("REAL") || typeText.equals("DOUBLE PRECISION")) {
      return parseNumericType(typeText);
    }
    
    // Boolean type
    else if (typeText.equals("BOOLEAN")) {
      return DataTypes.BooleanType;
    }
    
    // Binary types
    else if (typeText.startsWith("BINARY") || typeText.startsWith("VARBINARY")) {
      return parseBinaryType(typeText);
    }
    
    // Temporal types
    else if (typeText.equals("DATE") || typeText.startsWith("TIMESTAMP") || 
        typeText.equals("INTERVAL")) {
      return parseTemporalType(typeText);
    }
    
    // Complex types
    else if (typeText.equals("ROW")) {
      // Default to a generic struct type
      return DataTypes.createStructType(new StructField[0]);
    }
    else if (typeText.equals("ARRAY")) {
      // Default to a string array
      return DataTypes.createArrayType(DataTypes.StringType);
    }
    
    // Default to string for unrecognized types
    return DataTypes.StringType;
  }

  /**
   * Parse character type specifications.
   *
   * @param typeText the character type text
   * @return the corresponding Spark SQL DataType
   */
  private DataType parseCharacterType(final String typeText) {
    // For VARCHAR/CHAR with length
    if (typeText.contains("(")) {
      Matcher matcher = null;
      if (typeText.startsWith("VARCHAR")) {
        matcher = VARCHAR_PATTERN.matcher(typeText);
      } else {
        matcher = CHAR_PATTERN.matcher(typeText);
      }
      
      if (matcher.find()) {
        int length = Integer.parseInt(matcher.group(1));
        return DataTypes.StringType;
      }
    }
    
    // Default string type for all character types
    return DataTypes.StringType;
  }

  /**
   * Parse numeric type specifications.
   *
   * @param typeText the numeric type text
   * @return the corresponding Spark SQL DataType
   */
  private DataType parseNumericType(final String typeText) {
    if (typeText.equals("SMALLINT")) {
      return DataTypes.ShortType;
    } else if (typeText.equals("INTEGER") || typeText.equals("INT")) {
      return DataTypes.IntegerType;
    } else if (typeText.equals("BIGINT")) {
      return DataTypes.LongType;
    } else if (typeText.equals("REAL")) {
      return DataTypes.FloatType;
    } else if (typeText.equals("DOUBLE PRECISION")) {
      return DataTypes.DoubleType;
    } else if (typeText.startsWith("FLOAT")) {
      Matcher matcher = FLOAT_PATTERN.matcher(typeText);
      if (matcher.find()) {
        int precision = Integer.parseInt(matcher.group(1));
        return precision <= 24 ? DataTypes.FloatType : DataTypes.DoubleType;
      }
      return DataTypes.DoubleType;
    } else if (typeText.startsWith("NUMERIC") || typeText.startsWith("DECIMAL") || 
        typeText.startsWith("DEC")) {
      Matcher matcher = NUMERIC_PATTERN.matcher(typeText);
      if (matcher.find()) {
        int precision = Integer.parseInt(matcher.group(1));
        int scale = matcher.group(2) != null ? Integer.parseInt(matcher.group(2)) : 0;
        return DataTypes.createDecimalType(precision, scale);
      }
      return DataTypes.createDecimalType();
    }
    
    // Default to double for unrecognized numeric types
    return DataTypes.DoubleType;
  }

  /**
   * Parse binary type specifications.
   *
   * @param typeText the binary type text
   * @return the corresponding Spark SQL DataType
   */
  private DataType parseBinaryType(final String typeText) {
    Matcher matcher = BINARY_PATTERN.matcher(typeText);
    if (matcher.find()) {
      // Length parameter is ignored in Spark's BinaryType
      return DataTypes.BinaryType;
    }
    return DataTypes.BinaryType;
  }

  /**
   * Parse temporal type specifications.
   *
   * @param typeText the temporal type text
   * @return the corresponding Spark SQL DataType
   */
  private DataType parseTemporalType(final String typeText) {
    if (typeText.equals("DATE")) {
      return DataTypes.DateType;
    } else if (typeText.startsWith("TIMESTAMP")) {
      Matcher matcher = TIMESTAMP_PATTERN.matcher(typeText);
      if (matcher.find()) {
        String withTimeZone = matcher.group(2);
        if (withTimeZone != null && withTimeZone.equals("WITH")) {
          return DataTypes.TimestampType;
        }
      }
      return DataTypes.TimestampType;
    } else if (typeText.equals("INTERVAL")) {
      // Spark doesn't have a direct interval type, use string
      return DataTypes.StringType;
    }
    
    // Default to timestamp for unrecognized temporal types
    return DataTypes.TimestampType;
  }
}
