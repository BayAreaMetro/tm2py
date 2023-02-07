"""Logging module.

Note the general definition of logging levels as used in tm2py:

TRACE: highly detailed level information which would rarely be of interest
    except for detailed debugging by a developer
DEBUG: diagnostic information which would generally be useful to a developer
    debugging the model code; this may also be useful to a model operator in
    some cases.
DETAIL: more detail than would normally be of interest, but might be useful
    to a model operator debugging a model run / data or understanding
    model results
INFO: detail which would normally be worth recording about the model operation
STATUS: top-level, model is running type messages. There should be
    relatively few of these, generally one per component, or one per time
    period if the procedure is long.
WARN: warning messages where there is a possibility of a problem
ERROR: problem causing operation to halt which is normal
    (or not unexpected) in scope, e.g. file does not exist
    Includes general Python exceptions.
FATAL: severe problem requiring operation to stop immediately.
"""

from __future__ import annotations

import functools
import os
import socket
import traceback as _traceback
from abc import abstractmethod
from contextlib import contextmanager as _context
from datetime import datetime
from pprint import pformat
from typing import TYPE_CHECKING, Union

import requests
from typing_extensions import Literal, get_args

if TYPE_CHECKING:
    from tm2py.controller import RunController

LogLevel = Literal[
    "TRACE", "DEBUG", "DETAIL", "INFO", "STATUS", "WARN", "ERROR", "FATAL"
]
LEVELS_STR_TO_INT = dict((k, i) for i, k in enumerate(get_args(LogLevel)))
LEVELS_INT_TO_STR = dict((i, k) for i, k in enumerate(get_args(LogLevel)))

# pylint: disable=too-many-instance-attributes


class Logger:
    """Logging of message text for display, text file, and Emme logbook, as well as notify to slack.

    The log message levels can be one of:
    TRACE, DEBUG, DETAIL, INFO, STATUS, WARN, ERROR, FATAL
    Which will filter all messages of that severity and higher.
    See module note on use of descriptive level names.

    logger.log("a message")
    with logger.log_start_end("Running a set of steps"):
        logger.log("Message with timestamp")
        logger.log("A debug message", level="DEBUG")
        # equivalently, use the .debug:
        logger.debug("Another debug message")
        if logger.debug_enabled:
            # only generate this report if logging DEBUG
            logger.log("A debug report that takes time to produce", level="DEBUG")
        logger.notify_slack("A slack message")

    Methods can also be decorated with LogStartEnd (see class for more).

    Note that the Logger should only be initialized once per model run.
    In places where the controller is not available, the last Logger
    initialized can be obtained from the class method get_logger::

        logger = Logger.get_logger()

    Internal properties:
        _log_cache: the LogCache object
        _log_formatters: list of objects that format text and record, either
            to file, display (print to screen) or cache for log on error
        _use_emme_logbook: whether Emme logbook is enabled
        _slack_notifier: SlackNotifier object for sending messages to slack
    """

    # used to cache last initialized Logger
    _instance = None

    def __new__(cls, controller: RunController):
        """Logger __new__ method override. TODO.

        Args:
            controller (RunController): TODO.
        """
        # pylint: disable=unused-argument
        cls._instance = super(Logger, cls).__new__(cls)
        return cls._instance

    def __init__(self, controller: RunController):
        """Constructor for Logger object.

        Args:
            controller (RunController): Associated RunController instance.
        """
        self.controller = controller
        self._indentation = 0
        log_config = controller.config.logging
        iter_component_level = log_config.iter_component_level or []
        iter_component_level = dict(
            ((i, c), LEVELS_STR_TO_INT[l]) for i, c, l in iter_component_level
        )
        display_logger = LogDisplay(LEVELS_STR_TO_INT[log_config.display_level])
        run_log_formatter = LogFile(
            LEVELS_STR_TO_INT[log_config.run_file_level],
            os.path.join(controller.run_dir, log_config.run_file_path),
        )
        standard_log_formatter = LogFileLevelOverride(
            LEVELS_STR_TO_INT[log_config.log_file_level],
            os.path.join(controller.run_dir, log_config.log_file_path),
            iter_component_level,
            controller,
        )
        self._log_cache = LogCache(
            os.path.join(controller.run_dir, log_config.log_on_error_file_path)
        )
        self._log_formatters = [
            display_logger,
            run_log_formatter,
            standard_log_formatter,
            self._log_cache,
        ]

        self._use_emme_logbook = self.controller.config.logging.use_emme_logbook

        self._slack_notifier = SlackNotifier(self)

        # open log formatters
        for log_formatter in self._log_formatters:
            if hasattr(log_formatter, "open"):
                log_formatter.open()

    def __del__(self):
        """
        Destructor for logger object
        """
        for log_formatter in self._log_formatters:
            if hasattr(log_formatter, "close"):
                log_formatter.close()

    @classmethod
    def get_logger(cls):
        """Return the last initialized logger object."""
        return cls._instance

    def notify_slack(self, text: str):
        """Send message to slack if enabled by config.

        Args:
            text (str): text to send to slack
        """
        if self.controller.config.logging.notify_slack:
            self._slack_notifier.post_message(text)

    def log(self, text: str, level: LogLevel = "INFO", indent: bool = True):
        """Log text to file and display depending upon log level and config.

        Args:
            text (str): text to log
            level (str): logging level
            indent (bool): if true indent text based on the number of open contexts
        """
        timestamp = datetime.now().strftime("%d-%b-%Y (%H:%M:%S) ")
        for log_formatter in self._log_formatters:
            log_formatter.log(text, LEVELS_STR_TO_INT[level], indent, timestamp)
        if self._use_emme_logbook and self.controller.has_emme:
            self.controller.emme_manager.logbook_write(text)

    def trace(self, text: str, indent: bool = False):
        """Log text with level=TRACE.

        Args:
            text (str): text to log
            indent (bool): if true indent text based on the number of open contexts
        """
        self.log(text, "TRACE", indent)

    def debug(self, text: str, indent: bool = False):
        """Log text with level=DEBUG.

        Args:
            text (str): text to log
            indent (bool): if true indent text based on the number of open contexts
        """
        self.log(text, "DEBUG", indent)

    def detail(self, text: str, indent: bool = False):
        """Log text with level=DETAIL.

        Args:
            text (str): text to log
            indent (bool): if true indent text based on the number of open contexts
        """
        self.log(text, "DETAIL", indent)

    def info(self, text: str, indent: bool = False):
        """Log text with level=INFO.

        Args:
            text (str): text to log
            indent (bool): if true indent text based on the number of open contexts
        """
        self.log(text, "INFO", indent)

    def status(self, text: str, indent: bool = False):
        """Log text with level=STATUS.

        Args:
            text (str): text to log
            indent (bool): if true indent text based on the number of open contexts
        """
        self.log(text, "STATUS", indent)

    def warn(self, text: str, indent: bool = False):
        """Log text with level=WARN.

        Args:
            text (str): text to log
            indent (bool): if true indent text based on the number of open contexts
        """
        self.log(text, "WARN", indent)

    def error(self, text: str, indent: bool = False):
        """Log text with level=ERROR.

        Args:
            text (str): text to log
            indent (bool): if true indent text based on the number of open contexts
        """
        self.log(text, "ERROR", indent)

    def fatal(self, text: str, indent: bool = False):
        """Log text with level=FATAL.

        Args:
            text (str): text to log
            indent (bool): if true indent text based on the number of open contexts
        """
        self.log(text, "FATAL", indent)

    def log_time(self, text: str, level=1, indent=False):
        """Log message with timestamp"""
        timestamp = datetime.now().strftime("%d-%b-%Y (%H:%M:%S)")
        if indent:
            indent = "  " * self._indentation
            self.log(f"{timestamp}: {indent}{text}", level)
        else:
            self.log(f"{timestamp}: {text}", level)

    def _log_start(self, text: str, level: LogLevel = "INFO"):
        """Log message with timestamp and 'Start'.

        Args:
            text (str): message text
            level (str): logging level
        """
        self.log(f"Start {text}", level, indent=True)
        for log_formatter in self._log_formatters:
            log_formatter.increase_indent(LEVELS_STR_TO_INT[level])

    def _log_end(self, text: str, level: LogLevel = "INFO"):
        """Log message with timestamp and 'End'.

        Args:
            text (str): message text
            level (str): logging level
        """
        for log_formatter in self._log_formatters:
            log_formatter.decrease_indent(LEVELS_STR_TO_INT[level])
        self.log(f"End {text}", level, indent=True)

    @_context
    def log_start_end(self, text: str, level: LogLevel = "STATUS"):
        """Use with 'with' statement to log the start and end time with message.

        If using the Emme logbook (config.logging.use_emme_logbook is True), will
        also create a logbook nest in the tree view using logbook_trace.

        Args:
            text (str): message text
            level (str): logging level
        """
        with self._skip_emme_logging():
            self._log_start(text, level)
        if self._use_emme_logbook:
            with self.controller.emme_manager.logbook_trace(text):
                yield
        else:
            yield
        with self._skip_emme_logging():
            self._log_end(text, level)

    def log_dict(self, mapping: dict, level: LogLevel = "DEBUG"):
        """Format dictionary to string and log as text."""
        self.log(pformat(mapping, indent=1, width=120), level)

    @_context
    def _skip_emme_logging(self):
        """Temporary disable Emme logging (if enabled) and restore on exit.

        Intended use is with the log_start_end context and LogStartEnd decorator
        to allow use of the Emme context without double logging of the
        messages in the Emme logbook.
        """
        self._use_emme_logbook, use_emme = False, self._use_emme_logbook
        yield
        self._use_emme_logbook = use_emme

    def clear_msg_cache(self):
        """Clear all log messages from cache."""
        self._log_cache.clear()

    @property
    def debug_enabled(self) -> bool:
        """Returns True if DEBUG is currently filtered for display or print to file.

        Can be used to enable / disable debug logging which may have a performance
        impact.
        """
        debug = LEVELS_STR_TO_INT["DEBUG"]
        for log_formatter in self._log_formatters:
            if log_formatter is not self._log_cache and log_formatter.level <= debug:
                return True
        return False

    @property
    def trace_enabled(self) -> bool:
        """Returns True if TRACE is currently filtered for display or print to file.

        Can be used to enable / disable trace logging which may have a performance
        impact.
        """
        trace = LEVELS_STR_TO_INT["TRACE"]
        for log_formatter in self._log_formatters:
            if log_formatter is not self._log_cache and log_formatter.level <= trace:
                return True
        return False


class LogFormatter:
    """Base class for recording text to log.

    Properties:
        indent: current indentation level for the LogFormatter
        level: log filter level (as an int)
    """

    def __init__(self, level: int):
        """Constructor for LogFormatter.

        Args:
            level (int): log filter level (as an int)
        """
        self._level = level
        self.indent = 0

    @property
    def level(self):
        """The current filter level for the LogFormatter."""
        return self._level

    def increase_indent(self, level: int):
        """Increase current indent if the log level is filtered in."""
        if level >= self.level:
            self.indent += 1

    def decrease_indent(self, level: int):
        """Decrease current indent if the log level is filtered in."""
        if level >= self.level:
            self.indent -= 1

    @abstractmethod
    def log(
        self,
        text: str,
        level: int,
        indent: bool,
        timestamp: Union[str, None],
    ):
        """Format and log message text.

        Args:
            text (str): text to log
            level (int): logging level
            indent (bool): if true indent text based on the number of open contexts
            timestamp (str): formatted datetime as a string or None
        """

    def _format_text(
        self,
        text: str,
        level: int,
        indent: bool,
        timestamp: Union[str, None],
    ):
        """Format text for logging.

        Args:
            text (str): text to format
            level (int): logging level
            indent (bool): if true indent text based on the number of open contexts and
                timestamp width
            timestamp (str): formatted datetime as a string or None for timestamp
        """
        if timestamp is None:
            timestamp = "                        " if indent else ""
        if indent:
            num_indents = self.indent
            indent = "  " * max(num_indents, 0)
        else:
            indent = ""
        level_str = "{0:>6}".format(LEVELS_INT_TO_STR[level])
        return f"{timestamp}{level_str}: {indent}{text}"


class LogFile(LogFormatter):
    """Format and write log text to file.

    Properties:
        - level: the log level as an int
        - file_path: the absolute file path to write to
    """

    def __init__(self, level: int, file_path: str):
        """Constructor for LogFile object.

        Args:
            level (int): the log level as an int.
            file_path (str): the absolute file path to write to.
        """
        super().__init__(level)
        self.file_path = file_path
        self.log_file = None

    def open(self):
        """Open the log file for writing."""
        self.log_file = open(self.file_path, "w", encoding="utf8")

    def log(self, text: str, level: int, indent: bool, timestamp: Union[str, None]):
        """Log text to file and display depending upon log level and config.

        Note that log will not write to file until opened with a context.

        Args:
            text (str): text to log
            level (int): logging level
            indent (bool): if true indent text based on the number of open contexts
            timestamp (str): formatted datetime as a string or None for timestamp
        """
        if level >= self.level and self.log_file is not None:
            text = self._format_text(text, level, indent, timestamp)
            self.log_file.write(f"{text}\n")
            self.log_file.flush()

    def close(self):
        """Close the open log file."""
        self.log_file.close()
        self.log_file = None


class LogFileLevelOverride(LogFile):
    """Format and write log text to file.

    Properties:
        - level: the log level as an int
        - file_path: the absolute file path to write to
        - iter_component_level: TODO
        - controller: TODO
    """

    def __init__(self, level, file_path, iter_component_level, controller):
        """Constructor for LogFileLevelOverride object.

        Args:
            level (_type_): TODO
            file_path (_type_): TODO
            iter_component_level (_type_): TODO
            controller (_type_): TODO
        """
        super().__init__(level, file_path)
        self.iter_component_level = iter_component_level
        self.controller = controller

    @property
    def level(self):
        """Current log level with iter_component_level config override."""
        return self.iter_component_level.get(
            self.controller.iter_component, self._level
        )


class LogDisplay(LogFormatter):
    """Format and print log text to console / Notebook.

    Properties:
        - level: the log level as an int
    """

    def log(self, text: str, level: int, indent: bool, timestamp: Union[str, None]):
        """Format and display text on screen (print).

        Args:
            text (str): text to log
            level (int): logging level
            indent (bool): if true indent text based on the number of open contexts
            timestamp (str): formatted datetime as a string or None
        """
        if level >= self.level:
            print(self._format_text(text, level, indent, timestamp))


class LogCache(LogFormatter):
    """Caches all messages for later recording in on error logfile.

    Properties:
        - file_path: the absolute file path to write to
    """

    def __init__(self, file_path: str):
        """Constructor for LogCache object.

        Args:
            file_path (str): the absolute file path to write to.
        """
        super().__init__(level=0)
        self.file_path = file_path
        self._msg_cache = []

    def open(self):
        """Initialize log file (remove)."""
        if os.path.exists(self.file_path):
            os.remove(self.file_path)

    def log(self, text: str, level: int, indent: bool, timestamp: Union[str, None]):
        """Format and store text for later recording.

        Args:
            text (str): text to log
            level (int): logging level
            indent (bool): if true indent text based on the number of open contexts
            timestamp (str): formatted datetime as a string or None
        """
        self._msg_cache.append(
            (level, self._format_text(text, level, indent, timestamp))
        )

    def write_cache(self):
        """Write all cached messages."""
        with open(self.file_path, "w", encoding="utf8") as file:
            for level, text in self._msg_cache:
                file.write(f"{LEVELS_INT_TO_STR[level]:6} {text}\n")
        self.clear()

    def clear(self):
        """Clear message cache."""
        self._msg_cache = []


# pylint: disable=too-few-public-methods


class LogStartEnd:
    """Log the start and end time with optional message.

    Used as a Component method decorator. If msg is not provided a default
    message is generated with the object class and method name.

    Example::
        @LogStartEnd("Highway assignment and skims", level="STATUS")
        def run(self):
            pass

    Properties:
        text (str): message text to use in the start and end record.
        level (str): logging level as a string.
    """

    def __init__(self, text: str = None, level: str = "INFO"):
        """Constructor for LogStartEnd object.

        Args:
            text (str, optional): message text to use in the start and end record.
                Defaults to None.
            level (str, optional): logging level as a string. Defaults to "INFO".
        """
        self.text = text
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
            text = self.text or obj.__class__.__name__ + " " + func.__name__
            with obj.logger.log_start_end(text, self.level):
                value = func(obj, *args, **kwargs)
            return value

        return wrapper


class SlackNotifier:
    r"""Notify slack of model run status.

    The slack channel can be input directly, or is configured via text file found at
    "M:\Software\Slack\TravelModel_SlackWebhook.txt" (if on MTC server)
    rr"C:\Software\Slack\TravelModel_SlackWebhook.txt" (if local)

    Properties:
        - logger (Logger): object for logging of trace messages
        - slack_webhook_url (str): optional, url to use for sending the message to slack
    """

    def __init__(self, logger: Logger, slack_webhook_url: str = None):
        r"""Constructor for SlackNotifier object.

        Args:
            logger (Logger): logger instance.
            slack_webhook_url (str, optional): . Defaults to None, which is replaced by either:
                - r"M:\Software\Slack\TravelModel_SlackWebhook.txt" (if on MTC server)
                - r"C:\Software\Slack\TravelModel_SlackWebhook.txt" (otherwise)
        """
        self.logger = logger
        if not logger.controller.config.logging.notify_slack:
            self._slack_webhook_url = None
            return
        if slack_webhook_url is None:
            hostname = socket.getfqdn()
            if hostname.endswith(".mtc.ca.gov"):
                slack_webhook_url_file = (
                    r"M:\Software\Slack\TravelModel_SlackWebhook.txt"
                )
                self.logger.log(
                    f"SlackNotifier running on mtc host; using {slack_webhook_url_file}",
                    level="TRACE",
                )
            else:
                slack_webhook_url_file = (
                    r"C:\Software\Slack\TravelModel_SlackWebhook.txt"
                )
                self.logger.log(
                    f"SlackNotifier running on non-mtc host; using {slack_webhook_url_file}",
                    level="TRACE",
                )
            if os.path.isfile(slack_webhook_url_file):
                with open(slack_webhook_url_file, "r", encoding="utf8") as url_file:
                    self._slack_webhook_url = url_file.read()
            else:
                self._slack_webhook_url = None
        else:
            self._slack_webhook_url = slack_webhook_url
        self.logger.log(
            f"SlackNotifier using slack webhook url {self._slack_webhook_url}",
            level="TRACE",
        )

    def post_message(self, text):
        """Posts text to the slack channel via the webhook if slack_webhook_url is found.

        Args:
           text: text message to send to slack
        """
        if self._slack_webhook_url is None:
            return
        headers = {"Content-type": "application/json"}
        data = {"text": text}
        self.logger.log(f"Sending message to slack: {text}", level="TRACE")
        response = requests.post(self._slack_webhook_url, headers=headers, json=data)
        self.logger.log(f"Receiving response: {response}", level="TRACE")
