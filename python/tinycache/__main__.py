"""``python -m tinycache``: runs the shared cache server (see ``tinycache._cli``)."""

import sys

from ._cli import main

sys.exit(main())
