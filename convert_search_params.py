#!/usr/bin/env python3
# /// script
# dependencies = ["pyyaml"]
# ///
"""
Convert search-params.txt to a YAML test file with parse-only tests.
"""

import yaml
from pathlib import Path
from typing import List, Dict, Any


def convert_search_params_to_yaml(input_file: Path, output_file: Path) -> None:
    """
    Convert search-params.txt to a YAML test file.
    
    Args:
        input_file: Path to the search-params.txt file
        output_file: Path where the YAML test file should be written
    """
    print(f"Reading expressions from: {input_file}")
    
    # Read all expressions from the input file
    expressions = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            expression = line.strip()
            if expression:  # Skip empty lines
                expressions.append(expression)
    
    print(f"Found {len(expressions)} expressions")
    
    # Create test cases - just expression field for parse-only tests
    test_cases = []
    for expression in expressions:
        test_cases.append({'expression': expression})
    
    # Create the YAML structure
    yaml_data = {
        'tests': test_cases
    }
    
    # Write to output file
    print(f"Writing YAML test file to: {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        yaml.dump(yaml_data, f, 
                 default_flow_style=False,
                 allow_unicode=True,
                 sort_keys=False,
                 width=120,
                 indent=2)
    
    print(f"Successfully created YAML test file with {len(test_cases)} test cases")


def main():
    """Main function to run the conversion."""
    # Define file paths
    script_dir = Path(__file__).parent
    input_file = script_dir / 'fhirpath' / 'src' / 'test' / 'resources' / 'search-params.txt'
    output_file = script_dir / 'fhirpath' / 'src' / 'test' / 'resources' / 'search-params-parse-tests.yaml'
    
    # Verify input file exists
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        print("Please ensure the search-params.txt file exists in the expected location.")
        return 1
    
    # Create output directory if it doesn't exist
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        convert_search_params_to_yaml(input_file, output_file)
        print("\nConversion completed successfully!")
        return 0
    except Exception as e:
        print(f"Error during conversion: {e}")
        return 1


if __name__ == '__main__':
    exit(main())
