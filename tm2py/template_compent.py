""" See also base template.py for docstring and type hint examples.

module docsting
"""


# from contextlib import contextmanager as _context
# import os as _os
from tm2py.core.component import Component as _Component

# import tm2py.core.tools as _tools
# import tm2py.core.emme as _emme_tools


# _join, _dir = _os.path.join, _os.path.dirname


class Component(_Component):
    """docstring for component"""

    def __init__(self, controller):
        """Initialized the Component class

        Args:
            controller: Controller object which controls the model run.
        """
        super().__init__(controller)
        self._parameter = None

    def run(self):
        """Runs the component steps"""
        self._step1()
        self._step2()

    def _step1(self):
        """docstring for step1"""

    def _step2(self):
        """docstring for step2"""
