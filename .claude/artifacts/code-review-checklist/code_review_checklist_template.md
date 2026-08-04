# Code Review Checklist - {{project_name}}

Use this checklist to ensure consistent, thorough code reviews. Focus on items most relevant to the changes being reviewed.

## Functionality & Correctness
- [ ] Code implements the intended functionality
- [ ] Edge cases and error scenarios are handled
- [ ] Logic is sound and free of obvious bugs
{{#additional_functionality_checks}}
- [ ] {{additional_functionality_checks}}
{{/additional_functionality_checks}}

## Code Quality & Readability
- [ ] Code follows {{style_guide_name}} style guidelines{{#style_guide_url}} ([reference]({{style_guide_url}})){{/style_guide_url}}
- [ ] Variable and function names are clear and meaningful
- [ ] Code is well-organized and easy to understand
- [ ] No unnecessary complexity or duplication
{{#additional_quality_checks}}
- [ ] {{additional_quality_checks}}
{{/additional_quality_checks}}

## Security
- [ ] No hardcoded secrets, passwords, or API keys
- [ ] Input validation is present where needed
- [ ] Authentication and authorization checks are correct
{{#additional_security_checks}}
- [ ] {{additional_security_checks}}
{{/additional_security_checks}}

## Testing
- [ ] Tests are included and passing
- [ ] Test coverage is adequate for the changes
- [ ] Tests cover edge cases and error scenarios
{{#additional_testing_checks}}
- [ ] {{additional_testing_checks}}
{{/additional_testing_checks}}

## Documentation
- [ ] Code comments explain complex logic where needed
- [ ] Public APIs have documentation
- [ ] README or relevant docs are updated if needed
{{#additional_documentation_checks}}
- [ ] {{additional_documentation_checks}}
{{/additional_documentation_checks}}

{{#custom_categories}}
## {{custom_category_name}}
{{#custom_category_items}}
- [ ] {{custom_category_items}}
{{/custom_category_items}}
{{/custom_categories}}

---

**Note**: This checklist is a guide. Not all items apply to every change. Use judgment based on the scope and nature of the code being reviewed.

---

## Template Usage Instructions

To customize this template for your project:

1. **Replace placeholders:**
   - `{{project_name}}`: Your project or repository name
   - `{{style_guide_name}}`: Name of your style guide (e.g., "Google Java Style")
   - `{{style_guide_url}}`: Optional link to your style guide documentation

2. **Add project-specific checks:**
   - `{{additional_functionality_checks}}`: Any additional functionality requirements
   - `{{additional_quality_checks}}`: Project-specific quality standards
   - `{{additional_security_checks}}`: Organization security requirements
   - `{{additional_testing_checks}}`: Specific testing standards
   - `{{additional_documentation_checks}}`: Documentation requirements

3. **Add custom categories (optional):**
   - `{{custom_category_name}}`: Name of additional category (e.g., "Performance", "Accessibility")
   - `{{custom_category_items}}`: Items for that category

4. **Remove unused placeholders:**
   - Delete any `{{placeholder}}` sections you don't need
   - Remove the `{{#section}}...{{/section}}` blocks if not using optional sections

### Example Customization

```markdown
# Code Review Checklist - Pathling

## Code Quality & Readability
- [ ] Code follows Pathling coding standards ([reference](https://pathling.csiro.au/docs/contributing))
- [ ] Variable and function names are clear and meaningful
- [ ] Code is well-organized and easy to understand
- [ ] No unnecessary complexity or duplication
- [ ] FHIRPath expressions are properly validated

## Security
- [ ] No hardcoded secrets, passwords, or API keys
- [ ] Input validation is present where needed
- [ ] Authentication and authorization checks are correct
- [ ] FHIR resource access controls are verified

## Performance
- [ ] Spark operations are optimized for large datasets
- [ ] No unnecessary data shuffling or repartitioning
- [ ] Query performance impact has been considered
```
