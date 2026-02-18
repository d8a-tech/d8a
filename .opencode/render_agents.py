#!/usr/bin/env python3
"""
Agent Template Renderer

Purpose: Renders agent files from templates to avoid duplication across agent variants.

Usage:
    python .opencode/render_agents.py          # Render all agents
    python .opencode/render_agents.py --check  # Verify outputs without writing

When to run:
    After editing .opencode/agent-templates/developer-base.md or agents.json

Constraints:
    Uses only Python stdlib (json, pathlib, argparse) and f-strings for formatting.
"""

import argparse
import json
from pathlib import Path


def render_agent(base_template: str, config: dict) -> str:
    """
    Render agent file from template and config.

    Args:
        base_template: The base markdown template content
        config: Dictionary with model, description, temperature, mode, custom_paragraph

    Returns:
        Fully rendered agent file content with frontmatter and body
    """
    # Build YAML frontmatter
    frontmatter = f"""---
temperature: {config["temperature"]}
description: >-
  {config["description"]}
mode: {config["mode"]}
model: {config["model"]}
---"""

    # Substitute custom paragraph (or empty string if not provided)
    custom = config.get("custom_paragraph", "")
    body = base_template.replace("{CUSTOM_PARAGRAPH}", custom)

    # Add auto-generation warning comment at the top
    warning = "<!-- AUTO-GENERATED from .opencode/agent-templates/developer-base.md -->\n<!-- DO NOT EDIT DIRECTLY - edit template and run render_agents.py -->\n"

    return f"{warning}{frontmatter}\n{body}"


def main():
    parser = argparse.ArgumentParser(description="Render agent files from templates")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify outputs match without writing files",
    )
    args = parser.parse_args()

    # Define paths
    script_dir = Path(__file__).parent
    templates_dir = script_dir / "agent-templates"
    agents_dir = script_dir / "agent"

    config_path = templates_dir / "agents.json"
    base_template_path = templates_dir / "developer-base.md"

    # Read configuration
    if not config_path.exists():
        print(f"Error: Configuration file not found: {config_path}")
        return 1

    with open(config_path, "r") as f:
        agents_config = json.load(f)

    # Read base template
    if not base_template_path.exists():
        print(f"Error: Base template not found: {base_template_path}")
        return 1

    with open(base_template_path, "r") as f:
        base_template = f.read()

    # Ensure output directory exists
    agents_dir.mkdir(exist_ok=True)

    # Render each agent
    rendered_count = 0
    for agent_name, config in agents_config.items():
        # Skip special keys like _comment
        if agent_name.startswith("_"):
            continue

        output_path = agents_dir / f"{agent_name}.md"
        rendered_content = render_agent(base_template, config)

        if args.check:
            # Check mode: verify content matches
            if output_path.exists():
                with open(output_path, "r") as f:
                    existing_content = f.read()
                if existing_content == rendered_content:
                    print(f"✓ {agent_name}.md matches template")
                else:
                    print(f"✗ {agent_name}.md differs from template")
            else:
                print(f"✗ {agent_name}.md does not exist")
        else:
            # Write mode: write the file
            with open(output_path, "w") as f:
                f.write(rendered_content)
            print(f"Rendered: {output_path}")
            rendered_count += 1

    if not args.check:
        print(f"\nSuccessfully rendered {rendered_count} agent file(s)")

    return 0


if __name__ == "__main__":
    exit(main())
