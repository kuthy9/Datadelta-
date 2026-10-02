"""
__main__.py — Support `python -m datadelta`.

Useful when the console script is not on PATH (e.g. a venv that was
not activated). prog_name keeps the usage line reading "datadelta".
"""

from .cli import app

if __name__ == "__main__":
    app(prog_name="datadelta")
