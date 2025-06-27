package au.csiro.pathling.views;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link Column} class.
 */
class ColumnTest {

  @Test
  void testGetTagValues() {
    // Create a column with multiple tags
    Column column = Column.builder()
        .name("test_column")
        .path("test.path")
        .tag(Arrays.asList(
            ColumnTag.of("ansi/type", "VARCHAR(255)"),
            ColumnTag.of("description", "A test column"),
            ColumnTag.of("ansi/type", "TEXT"),  // Duplicate tag name with different value
            ColumnTag.of("nullable", "true")
        ))
        .build();

    // Test getting values for a tag that exists multiple times
    List<String> ansiTypeValues = column.getTagValues("ansi/type");
    assertEquals(2, ansiTypeValues.size());
    assertTrue(ansiTypeValues.contains("VARCHAR(255)"));
    assertTrue(ansiTypeValues.contains("TEXT"));

    // Test getting values for a tag that exists once
    List<String> descriptionValues = column.getTagValues("description");
    assertEquals(1, descriptionValues.size());
    assertEquals("A test column", descriptionValues.get(0));

    // Test getting values for a tag that doesn't exist
    List<String> nonExistentValues = column.getTagValues("non-existent");
    assertTrue(nonExistentValues.isEmpty());

    // Test with a column that has no tags
    Column emptyTagsColumn = Column.builder()
        .name("empty_tags")
        .path("empty.path")
        .tag(Collections.emptyList())
        .build();
    
    List<String> emptyResult = emptyTagsColumn.getTagValues("any-tag");
    assertTrue(emptyResult.isEmpty());
  }
  
  @Test
  void testGetTagValue() {
    // Create a column with multiple tags
    Column column = Column.builder()
        .name("test_column")
        .path("test.path")
        .tag(Arrays.asList(
            ColumnTag.of("ansi/type", "VARCHAR(255)"),
            ColumnTag.of("description", "A test column"),
            ColumnTag.of("ansi/type", "TEXT"),  // Duplicate tag name with different value
            ColumnTag.of("nullable", "true")
        ))
        .build();

    // Test getting a value for a tag that exists once
    Optional<String> descriptionValue = column.getTagValue("description");
    assertTrue(descriptionValue.isPresent());
    assertEquals("A test column", descriptionValue.get());

    // Test getting a value for a tag that doesn't exist
    Optional<String> nonExistentValue = column.getTagValue("non-existent");
    assertTrue(nonExistentValue.isEmpty());

    // Test with a column that has no tags
    Column emptyTagsColumn = Column.builder()
        .name("empty_tags")
        .path("empty.path")
        .tag(Collections.emptyList())
        .build();
    
    Optional<String> emptyResult = emptyTagsColumn.getTagValue("any-tag");
    assertTrue(emptyResult.isEmpty());
    
    // Test exception when multiple values exist
    Exception exception = org.junit.jupiter.api.Assertions.assertThrows(
        IllegalStateException.class,
        () -> column.getTagValue("ansi/type")
    );
    assertTrue(exception.getMessage().contains("Multiple values found for tag 'ansi/type'"));
  }
}
