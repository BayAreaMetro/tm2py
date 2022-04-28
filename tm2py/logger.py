"""Logging module."""

import functools
from contextlib import contextmanager as _context
from datetime import datetime

from tm2py.controller import Controller


class Logger:
    """Logger class."""

    def __init__(self, controller: Controller):
        """Logger constructor.

        Args:
            controller (Controller): Link to model run controller.
        """
        super().__init__()
        self._controller = controller
        self._indentation = 0

    @staticmethod
    def log(text: str, level: str = "INFO"):
        """Placeholder logging method.

        Args:
            text (str): text to log
            level (str): logging level of the message text
        """
        if level:
            print(text)

    def log_time(self, msg: str, level: str = "INFO", indent: bool = True):
        """Log message with timestamp.

        Args:
            msg (str): message text
            level (str): logging level
            indent (bool): if true indent any messages based on the number of open contexts
        """
        timestamp = datetime.now().strftime("%d-%b-%Y (%H:%M:%S)")
        if indent:
            indent = "  " * self._indentation
            self.log(f"{timestamp}: {indent}{msg}", level)
        else:
            self.log(f"{timestamp}: {msg}", level)

    def log_start(self, msg: str, level: str = "INFO"):
        """Log message with timestamp and 'Start'.

        Args:
            msg (str): message text
            level (str): logging level
        """
        self.log_time(f"Start {msg}", level, indent=True)
        self._indentation += 1

    def log_end(self, msg: str, level: str = "INFO"):
        """Log message with timestamp and 'End'.

        Args:
            msg (str): message text
            level (str): logging level
        """
        self._indentation -= 1
        self.log_time(f"End {msg}", level, indent=True)

    @_context
    def log_start_end(self, msg: str, level: str = "INFO"):
        """Use with 'with' statement to log the start and end time with message.

        Args:
            msg (str): message text
            level (str): logging level
        """
        self.log_start(msg, level)
        yield
        self.log_end(msg, level)


# pylint: disable=too-few-public-methods


class LogStartEnd:
    """Log the start and end time with optional message.

    Used as a Component method decorator. If msg is not provided a default message
    is generated with the object class and method name.
    """

    def __init__(self, msg: str = None, level: str = "INFO"):
        """Contructor for Logger.

        Args:
            msg (str, optional):  Message text to use in the start and end record.
                Defaults to None.
            level (str, optional): logging level. Defaults to "INFO".
        """
        self.msg = msg
        self.level = level

    def __call__(self, func):
        """Ability to call logger.

        Args:
            func (_type_): _description_

        Returns:
            _type_: _description_
        """

        @functools.wraps(func)
        def wrapper(obj, *args, **kwargs):
            msg = self.msg or obj.__class__.__name__ + " " + func.__name__
            obj.logger.log_start(msg, self.level)
            value = func(obj, *args, **kwargs)
            obj.logger.log_end(msg, self.level)
            return value

        return wrapper
