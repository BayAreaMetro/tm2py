"""Logging module docsting

"""


class Logger:
    """Logger docstring"""

    def __init__(self, controller):
        super().__init__()
        self._controller = controller
        self._config = controller.config

    def log(self, text):
        """Placeholder logging method"""
        print(text)
