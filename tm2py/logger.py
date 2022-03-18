"""Logging module
"""
from contextlib import contextmanager as _context
from datetime import datetime
import functools
import os
import requests
import socket
import traceback as _traceback
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tm2py.controller import RunController

levels = ["TRACE", "DEBUG", "DETAIL", "INFO", "STATUS", "WARN", "ERROR", "FATAL"]


class Logger:
    """Logger"""

    def __init__(self, controller: RunController):
        super().__init__()
        self.controller = controller
        log_config = controller.config.logging
        self._indentation = 0
        self._log_file_path = log_config.log_file_path
        self._error_file_path = log_config.error_file_path
        self._log_file = None
        self._msg_cache = []
        self._display_level = levels[levels.index(log_config.log_display_level):]
        self._print_level = levels[levels.index(log_config.log_file_level):]
        self._slack_notifier = SlackNotifier(self)

    def notify_slack(self, text: str):
        self._slack_notifier.post_message(text)

    def log(self, text: str, level: str = "INFO"):
        """Log text to file and display depending upon log level and configuration.

        Args:
            text (str): text to log
            level (str): logging level of the message text
        """
        if level in self._display_level:
            print(text)
        if level in self._print_level:
            self._write_file(f"{text}\n")
        self._msg_cache.append((level, text))

    def log_time(self, text: str, level: str = "INFO", indent: bool = False):
        """Log message with timestamp

        Args:
            text (str): message text
            level (int): logging level
            indent (bool): if true indent any messages based on the number of open contexts
        """
        timestamp = datetime.now().strftime("%d-%b-%Y (%H:%M:%S)")
        if indent:
            indent = "  " * self._indentation
            self.log(f"{timestamp}: {indent}{text}", level)
        else:
            self.log(f"{timestamp}: {text}", level)

    def log_start(self, text: str, level: str = "INFO"):
        """Log message with timestamp and 'Start'.

        Args:
            text (str): message text
            level (str): logging level
        """
        self.log_time(f"Start {text}", level, indent=True)
        self._indentation += 1

    def log_end(self, text: str, level: str = "INFO"):
        """Log message with timestamp and 'End'.

        Args:
            text (str): message text
            level (str): logging level
        """
        self._indentation -= 1
        self.log_time(f"End {text}", level, indent=True)

    @_context
    def log_start_end(self, text: str, level: str = "INFO"):
        """Use with 'with' statement to log the start and end time with message.

        Args:
            text (str): message text
            level (str): logging level
        """
        self.log_start(text, level)
        # with self.controller.emme_manager.logbook_trace(text):
        #     yield
        self.log_end(text, level)

    def __enter__(self):
        self._log_file = open(self._log_file_path, "w", encoding="utf8")
        os.remove(self._error_file_path)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            with open(self._error_file_path, "w", encoding="utf8") as file:
                for level, text in self._msg_cache:
                    file.write(text)
                _traceback.print_exception(exc_type, exc_val, exc_tb, file=file)
            self.log("Error during model run", level="ERROR")
            self.notify_slack(f"Error during model run in {self.controller.run_dir}.")
            # self.notify_slack(f"{exc_val}")
            _traceback.print_exception(exc_type, exc_val, exc_tb, file=self._log_file)
        self._log_file.close()

    def _write_file(self, text: str):
        if self._log_file:
            self._log_file.write(text)

    # def write_all_msg_cache(self):
    #     """Write all messages to log file which have been recorded since the cache was last cleared."""
    #     for level, msg in self._msg_cache:
    #         self._write_file(msg)
    #     self.clear_msg_cache()

    def clear_msg_cache(self):
        """Clear all log messages from cache."""
        self._msg_cache = []


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
            obj.logger.log_start(text, self.level)
            value = func(obj, *args, **kwargs)
            obj.logger.log_end(text, self.level)
            return value

        return wrapper


class SlackNotifier:
    """Notify slack of model run status

        Args:
            - logger (Logger): object for logging of trace messages
    """

    def __init__(self, logger: Logger):
        self.logger = logger
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
