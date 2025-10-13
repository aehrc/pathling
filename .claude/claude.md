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

## Git Commit Guidelines

### Commit Message Format

Write commit messages that capture the **objective** of the change, not the specific implementation details that can be obtained from the diff.

**Structure**:
```
<type>: <succinct description of the objective>

<optional body explaining the why and context>
```

**Types**:
- `fix:` - Bug fixes or resolving warnings/errors
- `feat:` - New features or enhancements
- `refactor:` - Code restructuring without changing behavior
- `docs:` - Documentation updates
- `test:` - Test-related changes
- `chore:` - Build, tooling, or dependency updates

**Guidelines**:
- Focus on **why** the change was needed and **what problem** it solves
- Avoid mentioning specific files, line numbers, or implementation details
- Keep the first line concise (under 72 characters when possible)
- Use the body to provide context if the objective isn't obvious

**Examples**:

Good:
```
fix: Suppress Mockito dynamic agent loading warnings in Java 21

Added JVM flag to suppress warnings about Mockito's inline mock maker
self-attaching. Updated documentation to record Maven test configuration.
```

Poor:
```
fix: Added -XX:+EnableDynamicAgentLoading to pom.xml line 637

Changed the argLine in maven-surefire-plugin configuration.
Updated CLAUDE.md with new section at lines 102-120.
```

## Maven Configuration

### Surefire Plugin Configuration

The Maven Surefire plugin is configured in the top-level `pom.xml` file under `pluginManagement`. All test-related JVM arguments should be added there to ensure consistency across all modules.

**Location**: `/pom.xml` -> `build` -> `pluginManagement` -> `maven-surefire-plugin` -> `configuration` -> `argLine`

**Key JVM arguments**:
- `-XX:+EnableDynamicAgentLoading` - Suppresses warnings about Mockito's dynamic agent loading
- `--add-exports` and `--add-opens` - Required for Java 17+ to access internal JDK APIs
- `-Xmx4g` - Maximum heap size for tests
- `-ea` - Enable assertions
- `@{argLine}` - Must be first to include JaCoCo agent arguments

**When modifying test configuration**:
- Always update the pluginManagement section in the root `pom.xml`
- Do not override surefire configuration in individual module POMs unless absolutely necessary
- Test any changes by running: `mvn test -pl <module>`

## External Code Discovery Pattern

### Finding Implementations in External Libraries

When you need to find the implementation of a class, trait, interface, or other code element in external dependencies (not in the local codebase), use this two-step discovery pattern:

**Step 1: Search GitHub for the Definition**

Use the `mcp__github__search_code` tool to locate the code:
- Search for the definition pattern: `class ClassName`, `trait TraitName`, `interface InterfaceName`, etc.
- Use GitHub's code search syntax to filter results:
  - By language: `language:Scala`, `language:Java`, `language:Python`
  - By repository: `repo:apache/spark`, `repo:scala/scala`
  - By organization: `org:apache`, `org:scala`
- Use exact code patterns (what would appear in the file), not keywords

**Step 2: Retrieve Full Implementation**

Once you have the GitHub URL from search results:
- Use the `WebFetch` tool with the discovered URL
- Request the complete implementation: prompt like "Show the complete implementation of this file" or "Extract the definition of ClassName"
- Present the full code to the user with context about its location

**Example Workflow**

```
User asks: "What file is AgnosticExpressionPathEncoder implemented in?"

You should:
1. Use mcp__github__search_code with:
   - query: "trait AgnosticExpressionPathEncoder repo:apache/spark language:Scala"

2. Get URL from results:
   https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/encoders/EncoderUtils.scala

3. Use WebFetch with that URL:
   - url: (the GitHub URL)
   - prompt: "Show the complete implementation of AgnosticExpressionPathEncoder"

4. Present the full trait definition to the user with explanation of its purpose and location
```

**When to Use This Pattern**

- User asks "Where is X implemented?" or "What file contains X?"
- User requests "Show me the implementation of X" for external code
- You need to reference a specific API from a dependency but don't know its exact location
- You want to demonstrate how an external library implements a feature

**When NOT to Use This Pattern**

- The code is in the local codebase (use Grep and Read tools instead)
- You already have the exact GitHub URL (skip to WebFetch)
- The code is in documentation rather than source files (use WebFetch directly on docs)

**Common Repositories**

- Apache Spark: `apache/spark`
- Scala Standard Library: `scala/scala`
- OpenJDK: `openjdk/jdk`
- Popular frameworks: `spring-projects/spring-framework`, `akka/akka`, etc.

**Benefits of This Pattern**

- Eliminates guesswork about file locations
- Provides accurate, up-to-date source code
- Demonstrates proper tool usage to users
- Works across different versions and branches
- Avoids wasting time searching local files for external dependencies
