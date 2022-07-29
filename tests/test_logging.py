"""Test module for Logging."""
import os, pathlib
from datetime import datetime


def test_log(tmp_path: pathlib.Path):
    """Test basic log operation outside model operation."""
    import tempfile

    from tm2py.config import LoggingConfig
    from tm2py.logger import Logger

    # use a stand-in minimal Controller and Config class to operate Logger
    log_config = {
        "display_level": "STATUS",
        "run_file_path": "tm2py_run.log",
        "run_file_level": "STATUS",
        "log_file_path": "tm2py_debug.log",
        "log_file_level": "DEBUG",
        "log_on_error_file_path": "tm2py_error.log",
        "notify_slack": False,
        "use_emme_logbook": False,
        "iter_component_level": None,
    }

    class Config:
        logging = LoggingConfig(**log_config)

    class Controller:
        def __init__(self, run_dir):
            self.config = Config()
            self.run_dir = run_dir
            self.iter_component = None
            self.logger = Logger(self)

    class TestException(Exception):
        pass

    # we'll use the tmp_path for our logs
    print('tmp_path: {}'.format(tmp_path))
    assert tmp_path.is_dir()

    controller = Controller(tmp_path)
    logger = controller.logger
    # Use an error to test the recording of error messages
    # as well as the generation of the "log_on_error" file
    try:
        logger.log("a message")  # default log level is INFO
        logger.log("A status", level="STATUS")
        logger.log("detailed message", level="DETAIL")
        logger.clear_msg_cache() # what is this?  Why is it called here?
        with logger.log_start_end("Running a set of steps"):
            logger.log("Indented message with timestamp")
            logger.log(
                "Indented displayed message with timestamp", level="STATUS"
            )
            logger.log(
                "A debug message not indented",
                level="DEBUG",
                indent=False,
            )
            logger.log("A debug message", level="DEBUG")
            logger.log("A trace message", level="TRACE")
            if logger.debug_enabled:
                # only generate this report if logging DEBUG
                logger.log("A debug report that takes time to produce", level="DEBUG"
                )
        logger.warn("Warning")

        # raising error to test recording of error message in log
        raise TestException("an error")
    except TestException:
        # catching the error to continue testing the content of the logs

        # I think the context / logcache was meant to make the following line unnecessary and 
        # automate error logging when an exception is thrown during a logging context
        # But given that I don't like logging contexts, I think it's fine to explicitly log
        # errors when they're caught
        logger.error("TestException caught")
        pass

    # Check the run_file recorded the high-level "STATUS" messages and above
    print('Checking log messages in {}'.format(os.path.join(controller.run_dir, log_config["run_file_path"])))
    with open(os.path.join(controller.run_dir, log_config["run_file_path"]), "r") as f:
        text = []
        for line in f:
            text.append(line)
    print('Log run file: {}'.format(text))
    assert len(text) == 6 # 4 status, 1 warning, 1 error
    assert text[0].endswith("STATUS: A status\n")
    assert text[1].endswith("STATUS: Start Running a set of steps\n")
    assert text[4].endswith("Warning\n")
    assert text[5].endswith("TestException caught\n")

    # Check the main log file containing all messages at DEBUG and above
    with open(os.path.join(controller.run_dir, log_config["log_file_path"]), "r") as f:
        text = []
        for line in f:
            text.append(line)
    print('Log file: {}'.format(text))
    assert len(text) == 12 # INFO, STATUS, DETAIL, STATUS, INFO, STATUS, DEBUG x 3, STATUS, WARN, ERRORR
    assert text[0].endswith("INFO: a message\n")
    assert text[1].endswith("STATUS: A status\n")
    assert text[2].endswith("DETAIL: detailed message\n")
    assert text[3].endswith("STATUS: Start Running a set of steps\n")
    # debug messages should appear
    assert text[7].endswith("A debug message\n")
    assert text[8].endswith("A debug report that takes time to produce\n")
    # but not trace message
    for logline in text:
        assert "A trace message" not in logline
    # error message recorded
    
    # todo: resolve 
    # assert "Error during model run" in text[9]
    # assert text[10].startswith("Traceback")

    # Commenting out the following pending resolution of issue#87 
    # (Feature: Explain/justify or remove LogCache, special error file)
    # Check that the log_on_error is generated and has all messages
    # with open(
    #     os.path.join(controller.run_dir, log_config["log_on_error_file_path"]), "r"
    # ) as f:
    #     text = []
    #     for line in f:
    #         text.append(line)
    # assert len(text) == 14
    # assert text[0].startswith("STATUS")
    # assert text[0].endswith("Running a set of steps\n")
    # # debug and trace messages appear in post error log
    # assert "DEBUG  A debug message\n" in text
    # assert "DEBUG  A debug report that takes time to produce\n" in text
    # assert "TRACE  A trace message\n" in text
