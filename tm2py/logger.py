"""Logging module

Note the general definition of logging levels as used in tm2py:

TRACE: highly detailed level information which would rarely be of interest
    except for detailed debugging
DEBUG: diagnostic information which would generally be used by a developer
    debugging the model code; this may also be useful to a model operator
DETAIL: more detail than would normally be of interest, but might be useful
    to a model operator debugging a model run / data or understanding
    model results
INFO: detail which would normally be worth recording about the model operation
STATUS: top-level, model is running type messages. There should be
    relatively few of these, generally one per component, and one per time
    period if the procedure is long
WARN: warning messages where there is a possibility of a problem
ERROR: problem causing operation to halt which is normal
    (or not unexpected) in scope, e.g. file does not exist
    Includes general Python exceptions.
FATAL: severe problem requiring operation to stop immediately.
"""

from contextlib import contextmanager as _context
from datetime import datetime
import functools
import os
from pprint import pformat
import requests
import socket
import traceback as _traceback
from typing import TYPE_CHECKING, get_args
from typing_extensions import Literal

if TYPE_CHECKING:
    from tm2py.controller import RunController

LogLevel = Literal["TRACE", "DEBUG", "DETAIL", "INFO", "STATUS", "WARN", "ERROR", "FATAL"]
levels = dict(enumerate(get_args(LogLevel)))


class Logger:
    """Logging of message text for display, text file, and Emme logbook, as well as notify to slack.

    The log message levels can be one of:
    TRACE, DEBUG, DETAIL, INFO, STATUS, WARN, ERROR, FATAL
    Which will filter all messages of that severity and higher.
    See module note on use of descriptive level names.
    """

    def __init__(self, controller: RunController):
        super().__init__()
        self.controller = controller
        log_config = controller.config.logging
        self._indentation = 0
        self._log_file_path = os.path.join(controller.run_dir, log_config.log_file_path)
        self._error_file_path = os.path.join(controller.run_dir, log_config.error_file_path)
        self._log_file = None
        self._msg_cache = []
        self._display_level = levels[log_config.log_display_level]
        self.__print_level = levels[log_config.log_file_level]
        self._iter_component_level = dict(((i, c), levels[l]) for i, c, l in log_config.iter_component_level)
        self._use_emme_logbook = self.controller.config.logging.use_emme_logbook
        self._slack_notifier = SlackNotifier(self)

    def notify_slack(self, text: str):
        """Send message to slack if enabled by config

        Args:
            text (str): text to send to slack
        """
        if self.controller.config.logging.notify_slack:
            self._slack_notifier.post_message(text)

    def log(self, text: str, level: LogLevel = "INFO"):
        """Log text to file and display depending upon log level and config

        Args:
            text (str): text to log
            level (str): logging level
        """
        if levels[level] >= self._display_level:
            print(text)
        if levels[level] >= self._print_level:
            self._log_file.write(f"{text}\n")
            if self._use_emme_logbook:
                self.controller.emme_manager.logbook_write(text)
        self._msg_cache.append((level, text))

    @property
    def _print_level(self):
        level = self._iter_component_level.get(self.controller.iter_component)
        if level is not None:
            return level
        return self.__print_level

    def log_time(self, text: str, level: LogLevel = "INFO", indent: bool = True):
        """Log message with timestamp

        Args:
            text (str): text to log
            level (str): logging level
            indent (bool): if true indent any messages based on the number of open contexts
        """
        timestamp = datetime.now().strftime("%d-%b-%Y (%H:%M:%S)")
        if indent:
            indent = "  " * self._indentation
            self.log(f"{timestamp}: {indent}{text}", level)
        else:
            self.log(f"{timestamp}: {text}", level)

    def log_start(self, text: str, level: LogLevel = "INFO"):
        """Log message with timestamp and 'Start'.

        Args:
            text (str): message text
            level (str): logging level
        """
        self.log_time(f"Start {text}", level, indent=True)
        self._indentation += 1

    def log_end(self, text: str, level: LogLevel = "INFO"):
        """Log message with timestamp and 'End'.

        Args:
            text (str): message text
            level (str): logging level
        """
        self._indentation -= 1
        self.log_time(f"End {text}", level, indent=True)

    @_context
    def log_start_end(self, text: str, level: LogLevel = "INFO"):
        """Use with 'with' statement to log the start and end time with message.

        If using the Emme logbook (config.logging.use_emme_logbook is True), will
        also create a logbook nest in the tree view using logbook_trace.

        Args:
            text (str): message text
            level (str): logging level
        """
        with self._skip_emme_logging():
            self.log_start(text, level)
        if self._use_emme_logbook:
            with self.controller.emme_manager.logbook_trace(text):
                yield
        else:
            yield
        with self._skip_emme_logging():
            self.log_end(text, level)

    def log_dict(self, mapping: dict, level: LogLevel = "DEBUG"):
        """Format dictionary to string and log as text"""
        self.log(pformat(mapping, indent=1, width=120), level)

    @_context
    def _skip_emme_logging(self):
        """Temporary disable Emme logging (if enabled) and restore on exit

        Intended use is with the log_start_end context and LogStartEnd decorator
        to allow use of the Emme context without double logging of the 
        messages in the Emme logbook.
        """
        self._use_emme_logbook, use_emme = False, self._use_emme_logbook
        yield
        self._use_emme_logbook = use_emme

    def __enter__(self):
        self._log_file = open(self._log_file_path, "w", encoding="utf8")
        os.remove(self._error_file_path)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            with open(self._error_file_path, "w", encoding="utf8") as file:
                for level, text in self._msg_cache:
                    file.write(f"{level:6} {text}\n")
                _traceback.print_exception(exc_type, exc_val, exc_tb, file=file)
            self.log("Error during model run", level="ERROR")
            self.notify_slack(f"Error during model run in {self.controller.run_dir}.")
            self.notify_slack(f"{exc_val}")
            _traceback.print_exception(exc_type, exc_val, exc_tb, file=self._log_file)
        self._log_file.close()

    def clear_msg_cache(self):
        """Clear all log messages from cache."""
        self._msg_cache = []

    @property
    def debug_enabled(self) -> bool:
        """Returns True if DEBUG is currently filtered for display or print to file.

        Can be used to enable / disable debug logging which may have a performance
        impact.
        """
        return "DEBUG" in self._print_level or "DEBUG" in self._display_level

    @property
    def trace_enabled(self) -> bool:
        """Returns True if TRACE is currently filtered for display or print to file.

        Can be used to enable / disable trace logging which may have a performance
        impact.
        """
        return "TRACE" in self._print_level or "TRACE" in self._display_level


# pylint: disable=too-few-public-methods


class LogStartEnd:
    """Log the start and end time with optional message.

    Used as a Component method decorator. If msg is not provided a default message
    is generated with the object class and method name.

    Args:
        text (str): message text to use in the start and end record
        level (str): logging level
    """

    def __init__(self, text: str = None, level: str = "INFO"):
        self.text = text
        self.level = level

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(obj, *args, **kwargs):
            text = self.text or obj.__class__.__name__ + " " + func.__name__
            with obj.logger.log_start_end(text, self.level)
                value = func(obj, *args, **kwargs)
            return value

        return wrapper


class SlackNotifier:
    """Notify slack of model run status.

    The slack channel can be input directly, or is configured via text file found at
    "M:\\Software\\Slack\\TravelModel_SlackWebhook.txt" (if on MTC server)
    "C:\\Software\\Slack\\TravelModel_SlackWebhook.txt" (if local)

        Args:
            - logger (Logger): object for logging of trace messages
            - slack_webhook_url (str): optional, url to use for sending the message to slack
    """

    def __init__(self, logger: Logger, slack_webhook_url: str = None):
        self.logger = logger
        if slack_webhook_url is None:
            hostname = socket.getfqdn()
            if hostname.endswith(".mtc.ca.gov"):
                slack_webhook_url_file = r"M:\Software\Slack\TravelModel_SlackWebhook.txt"
                self.logger.log(f"SlackNotifier running on mtc host; using {slack_webhook_url_file}", level="TRACE")
            else:
                slack_webhook_url_file = r"C:\Software\Slack\TravelModel_SlackWebhook.txt"
                self.logger.log(f"SlackNotifier running on non-mtc host; using {slack_webhook_url_file}", level="TRACE")
            if os.path.isfile(slack_webhook_url_file):
                with open(slack_webhook_url_file, 'r', encoding="utf8") as f:
                    self._slack_webhook_url = f.read()
            else:
                self._slack_webhook_url = None
        else:
            self._slack_webhook_url = slack_webhook_url
        self.logger.log(f"SlackNotifier using slack webhook url {self._slack_webhook_url}", level="TRACE")

    def post_message(self, text):
        """
        Posts the given message to the slack channel via the webhook
        if slack_webhook_url is found.

        Args:
            - text: text message to send to slack
        """
        headers = {"Content-type": "application/json"}
        data = {"text": text}
        if self._slack_webhook_url:
            self.logger.log(f"Sending message to slack: {text}", level="TRACE")
            response = requests.post(self._slack_webhook_url, headers=headers, json=data)
            self.logger.log(f"Receiving response: {response}", level="TRACE")
