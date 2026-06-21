import json
import sys

# Create a global namespace for all exec calls
global_ns = {
    '__builtins__': __builtins__,
}

# Load the notebook
with open('Task6_Tableau_Export.ipynb', 'r') as f:
    notebook = json.load(f)

# Extract cells in order and execute them one by one
for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code':
        # Join the lines of the cell
        code = ''.join(cell['source'])
        print(f"Executing cell {i}...")
        try:
            exec(code, global_ns)
            print(f"Cell {i} executed successfully")
        except Exception as e:
            print(f"Error in cell {i}: {e}")
            import traceback
            traceback.print_exc()
            # Continue with next cell instead of stopping
            continue

print("Notebook execution completed!")