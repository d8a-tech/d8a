#!/usr/bin/env python3

import json
import os
import sys

def generate_matrix():
    # Read the apps and changed files from environment variables
    apps_json = os.environ.get('APPS', '[]')
    changed_files_json = os.environ.get('CHANGED_FILES', '[]')
    is_tag = bool(json.loads(os.environ.get('IS_TAG', 'false')))
    
    try:
        # Parse the JSON inputs
        apps = json.loads(apps_json)
        changed_files = json.loads(changed_files_json)
        
        # Initialize empty list for changed apps
        changed_apps = []
        
        # If IS_TAG is present, include all apps
        if is_tag:
            changed_apps = apps
        else:
            # Check if src/libs has changes
            libs_changed = any(changed_file.startswith('src/libs') for changed_file in changed_files)
            
            # Check each app for changes
            for app in apps:
                # If libs changed, include all apps
                # Or if any changed files are in this app directory
                if libs_changed or any(changed_file.startswith(app) for changed_file in changed_files):
                    changed_apps.append(app)
        
        # Generate matrix output
        matrix = {"app": changed_apps}
        
        # Write the matrix to GITHUB_OUTPUT
        github_output = os.environ.get('GITHUB_OUTPUT')
        if github_output:
            with open(github_output, 'a') as f:
                f.write(f"matrix={json.dumps(matrix)}\n")
        
        print(f"Changed Apps Matrix: {json.dumps(matrix)}")
        return 0
        
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(generate_matrix())