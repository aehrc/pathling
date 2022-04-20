package au.csiro.pathling.encoders;

import static org.junit.Assert.assertTrue;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

public interface SchemaAsserts {

  @SuppressWarnings("SameParameterValue")
  static void assertFieldNotPresent(final String fieldName,
      final DataType maybeStructType) {
    assertTrue("Must be struct type.", maybeStructType instanceof StructType);
    assertTrue("Field: '" + fieldName + "' not present in struct type.",
        ((StructType) maybeStructType).getFieldIndex(
            fieldName).isEmpty());
  }

  @SuppressWarnings("SameParameterValue")
  static void assertFieldPresent(final String fieldName,
      final DataType maybeStructType) {
    assertTrue("Must be struct type.", maybeStructType instanceof StructType);
    assertTrue("Field: '" + fieldName + "' not present in struct type.",
        ((StructType) maybeStructType).getFieldIndex(
            fieldName).isDefined());
  }
}
