# Notebooks exploration

## But I see no notebook there ?

Notebooks are exported to .py files using VSCode "Import notebook to script" functionality.

That way, this is easier to version control.

If you need to recreate the notebook, you can run all cells from any `vsc_notebook.py` file and save result in a notebook using VSCode plugin.

This is similar to Jupytext plugin for JupyterLab.

## Requirements

A [python install](https://www.python.org/downloads/) (recommended python >= 3.12) with pip.

Just install dependencies through pip. (still recommended in a new virtual environment)

```
pip install -r requirements.txt
```

Pip was chosen to do some quick exploration. No need for more complex package handling.
