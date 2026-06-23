import json, sys, datetime, os

def add_markdown_cell(nb_path, text):
    with open(nb_path, 'r', encoding='utf-8') as f:
        nb = json.load(f)
    # create a new cell
    new_cell = {
        "cell_type": "markdown",
        "metadata": {},
        "source": [text + "\n"]
    }
    # append to cells list
    nb.setdefault('cells', []).append(new_cell)
    # update "metadata" timestamp (optional)
    nb.setdefault('metadata', {})['updated'] = datetime.datetime.utcnow().isoformat() + 'Z'
    # write back
    with open(nb_path, 'w', encoding='utf-8') as f:
        json.dump(nb, f, indent=2)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print('Usage: update_notebook.py <notebook_path> <text>')
        sys.exit(1)
    nb_path = sys.argv[1]
    text = sys.argv[2]
    add_markdown_cell(nb_path, text)
    print(f'Updated {nb_path}')
