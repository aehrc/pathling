# Pathling Project - Claude Coding Guidelines

## General Code Quality Standards

### Java Code Standards

1. **Use `final` modifier whenever possible**
   - Mark all local variables as `final` if they are not reassigned
   - Mark method parameters as `final`
   - Mark class fields as `final` when they are initialized once
   - Example:
     ```java
     public static void processData(@Nonnull final String input) {
       final List<String> items = parseInput(input);
       final String result = transform(items);
       return result;
     }
     ```

2. **Use `jakarta.annotation.Nonnull` annotations**
   - Annotate all non-null method parameters with `@Nonnull`
   - Annotate all non-null return types with `@Nonnull`
   - This helps with static analysis and makes nullability explicit
   - Example:
     ```java
     @Nonnull
     public static Column structProduct(@Nonnull final Column... columns) {
       // implementation
     }
     ```

3. **Prefer Java Streams for collection operations**
   - Use streams for functional-style operations on collections
   - Example:
     ```java
     final List<Expression> expressions = Arrays.stream(columns)
         .map(ExpressionUtils::expression)
         .collect(Collectors.toList());
     ```

### Scala Code Standards

1. **Prefer immutability**
   - Use `val` instead of `var` whenever possible
   - Use immutable collections by default

2. **Use explicit types for public APIs**
   - Always specify return types for public methods
   - Specify types for public fields

### General Best Practices

1. **Clear, descriptive variable names**
   - Use meaningful names that describe the purpose
   - Avoid abbreviations unless they are well-known

2. **Comment complex logic**
   - Add comments for non-obvious business logic
   - Explain "why" rather than "what" when the code is self-explanatory

3. **Consistent formatting**
   - Follow the existing code style in the file
   - Use proper indentation (2 spaces for Scala, 2 spaces for Java in this project)

## Spark 4.0 Migration Notes

### Converting between Column and Expression

In Spark 4.0, the `Column` companion object and many utility methods are package-private. To work around this:

1. **From Java**: Use `org.apache.spark.sql.classic.ExpressionUtils`
   - Java can access package-private methods that Scala cannot
   - `ExpressionUtils.column(expression)` - converts Expression to Column
   - `ExpressionUtils.expression(column)` - converts Column to Expression

2. **From Scala**: Import and use ExpressionUtils from Java interop
   ```scala
   import org.apache.spark.sql.classic.ExpressionUtils

   val column = ExpressionUtils.column(expression)
   val expression = ExpressionUtils.expression(column)
   ```

3. **Avoid using**:
   - `new Column(expr)` - Column constructor with Expression is not available
   - `Column.expr` - This property was removed in Spark 4.0
   - `ColumnConversions` implicit conversions - Column companion object is private

### Key API Changes in Spark 4.0

1. **NullIntolerant**: Changed from trait to method
   - Old: `extends NullIntolerant`
   - New: `override def nullIntolerant: Boolean = true`

2. **ExpressionEncoder**: Now requires AgnosticEncoder
   - Create custom `AgnosticExpressionPathEncoder` for custom encoders
   - Implement `toCatalyst` and `fromCatalyst` methods

3. **Buffer to Seq conversions**: Add `.toSeq` when needed
   - Scala collections may return Buffer instead of Seq in some operations
