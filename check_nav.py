import os
import yaml

# Read mkdocs config
with open('mkdocs.yml', 'r') as f:
    config = yaml.safe_load(f)

# Extract all markdown file references from nav
def extract_md_files(nav_item):
    files = []
    if isinstance(nav_item, dict):
        for key, value in nav_item.items():
            if isinstance(value, str) and value.endswith('.md'):
                files.append(value)
            elif isinstance(value, (list, dict)):
                files.extend(extract_md_files(value))
    elif isinstance(nav_item, list):
        for item in nav_item:
            files.extend(extract_md_files(item))
    return files

nav_files = extract_md_files(config['nav'])
print('Files referenced in navigation:')
missing_files = []
existing_files = []

for f in sorted(set(nav_files)):
    exists = os.path.exists(os.path.join('docs', f))
    status = 'EXISTS' if exists else 'MISSING'
    print(f'  {f} - {status}')
    if exists:
        existing_files.append(f)
    else:
        missing_files.append(f)

print(f'\nSummary:')
print(f'  Total files in navigation: {len(nav_files)}')
print(f'  Existing files: {len(existing_files)}')
print(f'  Missing files: {len(missing_files)}')

if missing_files:
    print(f'\nMissing files that need to be created:')
    for f in missing_files:
        print(f'  - {f}')