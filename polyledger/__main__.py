"""Allows `python -m polyledger` as an alias for the `polyledger` command."""

import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
