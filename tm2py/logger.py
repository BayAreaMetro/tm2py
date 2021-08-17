"""Logging module
"""


class Logger:
    """Logger"""

    def __init__(self, controller):
        super().__init__()
        self._controller = controller

    @property
    def controller(self):
        """Parent controller"""
        return self._controller

    @property
    def config(self):
        """Configuration settings loaded from config files"""
        return self.controller.config

    @staticmethod
    def log(text):
        """Placeholder logging method"""
        print(text)
