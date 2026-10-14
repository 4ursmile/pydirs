"""``python -m pydirs``: runs the shared cache server (see ``pydirs._cli``)."""

import sys

from ._cli import main

sys.exit(main())
