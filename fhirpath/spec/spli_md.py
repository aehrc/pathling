#!/usr/bin/env python3
"""
Split markdown files by headings into multiple files.

This script takes a markdown file and splits it into multiple files based on headings.
The level parameter controls which heading levels trigger a new file.
"""

import os
import re
import click
from pathlib import Path


def slugify(text):
    """Convert text to snake_case for filenames."""
    # Remove special chars, replace spaces with underscores
    text = re.sub(r'[^\w\s]', '', text.lower())
    return re.sub(r'\s+', '_', text.strip())


def extract_sections(content, max_level):
    """
    Extract sections from markdown content based on heading level.
    
    Args:
        content: The markdown content as a string
        max_level: The maximum heading level to consider for splitting
        
    Returns:
        A list of tuples (heading_path, content) where heading_path is a list
        of headings representing the hierarchy
    """
    # Pattern to match headings: # to ######
    heading_pattern = re.compile(r'^(#{1,6})\s+(.+)$', re.MULTILINE)
    
    # Find all headings with their levels and titles
    headings = [(len(match.group(1)), match.group(2).strip(), match.start()) 
                for match in heading_pattern.finditer(content)]
    
    if not headings:
        return [(['root'], content)]
    
    sections = []
    for i, (level, title, start) in enumerate(headings):
        # Only process headings at or above the max_level
        if level <= max_level:
            # Determine the end of this section (start of next section at same or higher level)
            end = None
            for j, (next_level, _, next_start) in enumerate(headings[i+1:], i+1):
                if next_level <= level:
                    end = next_start
                    break
            
            # Extract the section content
            section_content = content[start:end].strip() if end else content[start:].strip()
            
            # Build the heading path
            heading_path = []
            for prev_level, prev_title, _ in headings[:i+1]:
                if prev_level <= level:
                    while len(heading_path) >= prev_level:
                        heading_path.pop() if heading_path else None
                    heading_path.append(slugify(prev_title))
            
            sections.append((heading_path, section_content))
    
    return sections


@click.command()
@click.argument('input_file', type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument('output_dir', type=click.Path(file_okay=False, dir_okay=True))
@click.option('--max-level', '-l', default=1, help='Maximum heading level to split at (1-6)')
def split_markdown(input_file, output_dir, max_level):
    """
    Split a markdown file into multiple files based on headings.
    
    INPUT_FILE: Path to the markdown file to split
    
    OUTPUT_DIR: Directory where the split files will be saved
    """
    # Validate max_level
    if max_level < 1 or max_level > 6:
        click.echo("Error: max-level must be between 1 and 6", err=True)
        return
    
    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Read the input file
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Extract sections
    sections = extract_sections(content, max_level)
    
    # Write each section to a separate file
    for heading_path, section_content in sections:
        if not heading_path:
            continue
            
        # Create filename from heading path
        filename = "~".join(heading_path) + ".md"
        file_path = output_path / filename
        
        # Write content to file
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(section_content)
        
        click.echo(f"Created: {file_path}")
    
    click.echo(f"Split {len(sections)} sections from {input_file}")


if __name__ == '__main__':
    split_markdown()
