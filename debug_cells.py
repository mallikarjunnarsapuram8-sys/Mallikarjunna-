import json

# Load the notebook
with open('Task6_Tableau_Export.ipynb', 'r') as f:
    notebook = json.load(f)

# Print cell types and first few lines
for i, cell in enumerate(notebook['cells']):
    print(f"Cell {i}: {cell['cell_type']}")
    if cell['cell_type'] == 'code':
        # Show first 3 lines of code
        lines = cell['source'][:3]
        print(f"  Lines: {lines}")
    elif cell['cell_type'] == 'markdown':
        # Show first 3 lines of markdown
        lines = cell['source'][:3]
        print(f"  Lines: {lines}")
    print()