"""Logging module

"""
from contextlib import contextmanager as _context
from datetime import datetime
import functools


class Logger:
    """Logger"""

    def __init__(self, controller):
        super().__init__()
        self._controller = controller
        self._indentation = 0

    @property
    def controller(self):
        """Parent controller"""
        return self._controller

    @property
    def config(self):
        """Configuration settings loaded from config files"""
        return self.controller.config

    @staticmethod
    def log(msg, level=1):
        """Placeholder logging method"""
        print(msg)

    def log_time(self, msg, level=1):
        """Log message with timestamp"""
        timestamp = datetime.now().strftime("%d-%b-%Y (%H:%M:%S)")
        self.log(f"{timestamp}: {msg}", level)

    def log_start(self, msg, level=1):
        """Log message with timestamp"""
        indent = "  " * self._indentation
        self.log_time(f"{indent}Start {msg}", level)
        self._indentation += 1

    def log_end(self, msg, level=1):
        self._indentation -= 1
        indent = "  " * self._indentation
        self.log_time(f"{indent}End {msg}", level)

    @_context
    def log_start_end(self, msg, level=1):
        self.log_start(msg, level)
        yield
        self.log_end(msg, level)

class LogStartEnd:
    """Log the start and end time with optional message.

    Can be used as a Component method decorator or in with statement
    """

    def __init__(self, msg=None, level=1):
        self.msg = msg
        self.level = level

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(obj, *args, **kwargs):
            msg = self.msg or obj.__class__.__name__ + " " + func.__name__
            obj.logger.log_start(msg, self.level)
            value = func(obj, *args, **kwargs)
            obj.logger.log_end(msg, self.level)
            return value

        return wrapper
