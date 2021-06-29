"""Logging module docsting

"""


class Logger:
    """Logger docstring"""
    # Skip too-few-public methods recomendation
    # pylint-disable=R0903

    def __init__(self, controller):
        super().__init__()
        self._controller = controller
        self._config = controller.config

    @staticmethod
    def log(text):
        """Placeholder logging method"""
        print(text)
