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

## External Code/Knowledge Discovery Pattern



### FHIRPath Specification

The complete FHIRPath specification is available at `.local/specs/FHIRPath.md`. This document should be consulted when:
- Implementing FHIRPath operators and functions
- Resolving questions about FHIRPath semantics and behavior
- Verifying correct interpretation of the FHIRPath type system
- Understanding FHIRPath grammar and expression evaluation rules

### Searching in Large Documentation Files

When you need to find specific information in large documentation or specification files (like `tmp/specs/FHIRPath.md`) that are too large to load entirely into the context window:

**Use grep or similar search tools to locate relevant sections:**
- Use the `Grep` tool to search for specific terms, concepts, or patterns
- Search for section headers, function names, operator definitions, or keywords
- Once you've identified the relevant section location, use `Read` tool with line ranges to read only that portion
- Combine multiple targeted searches rather than trying to load the entire file

**Example:**

To find information about the 'where' operator in FHIRPath:
1. Use Grep: pattern="where" path="tmp/specs/FHIRPath.md"
2. Identify line numbers from grep results
3. Use Read to load just that section with content


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
