from datetime import datetime
import os


def test_log():
    """Test basic log operation outside model operation"""
    from tm2py.config import LoggingConfig
    from tm2py.logger import Logger
    import tempfile

    # use a stand-in minimal Controller and Config class to operate Logger
    log_config = {
        "display_level": "STATUS",
        "run_file_path": "log_run.txt",
        "run_file_level": "STATUS",
        "log_file_path": "log.txt",
        "log_file_level": "DEBUG",
        "log_on_error_file_path": "error.txt",
        "notify_slack": False,
        "use_emme_logbook": False,
        "iter_component_level": None,
    }

    class Config:
        logging = LoggingConfig(**log_config)

    class Controller:
        config = Config()
        run_dir = ""
        iter_component = None

    class TestException(Exception):
        pass

    # Test that the expected log messages are recorded and formatted
    with tempfile.TemporaryDirectory() as temp_dir:
        controller = Controller()
        controller.run_dir = temp_dir
        # Use an error to test the recording of error messages
        # as well as the generation of the "log_on_error" file
        try:
            with Logger(controller) as logger:
                logger.log("a message")
                logger.log("A status", level="STATUS")
                logger.clear_msg_cache()
                with logger.log_start_end("Running a set of steps"):
                    logger.log_time("Indented message with timestamp")
                    logger.log_time(
                        "Indented displayed message with timestamp", level="STATUS"
                    )
                    logger.log(
                        "A detail message, no time, but indented",
                        level="DEBUG",
                        indent=True,
                    )
                    logger.log("A debug message", level="DEBUG")
                    logger.log("A trace message", level="TRACE")
                    if logger.debug_enabled:
                        # only generate this report if logging DEBUG
                        logger.log(
                            "A debug report that takes time to produce", level="DEBUG"
                        )
                # raising error to test recording of error message in log
                raise TestException("an error")
        except TestException:
            # catching the error to continue testing the content of the logs
            pass

        # Check the run_file recorded the high-level "STATUS" messages and above
        with open(os.path.join(temp_dir, log_config["run_file_path"]), "r") as f:
            text = []
            for line in f:
                text.append(line)
        assert len(text) == 5
        assert text[0] == "A status\n"
        # will raise if message is not formatted correctly
        datetime.strptime(
            text[1], "%d-%b-%Y (%H:%M:%S): Start Running a set of steps\n"
        )
        datetime.strptime(
            text[2], "%d-%b-%Y (%H:%M:%S):   Indented displayed message with timestamp\n"
        )
        assert "Error during model run" in text[4]

        # Check the main log file containing all messages at DEBUG and above
        with open(os.path.join(temp_dir, log_config["log_file_path"]), "r") as f:
            text = []
            for line in f:
                text.append(line)
        assert len(text) == 15
        assert text[0] == "a message\n"
        assert text[1] == "A status\n"
        assert text[2].endswith("Start Running a set of steps\n")
        # will raise if message is not formatted correctly
        datetime.strptime(
            text[3], "%d-%b-%Y (%H:%M:%S):   Indented message with timestamp\n"
        )
        # debug messages should appear
        assert "A debug message\n" in text
        assert "A debug report that takes time to produce\n" in text
        # but not trace message
        assert "A trace message\n" not in text
        # error message recorded
        assert "Error during model run" in text[9]
        assert text[10].startswith("Traceback")

        # Check that the log_on_error is generated and has all messages
        with open(os.path.join(temp_dir, log_config["log_on_error_file_path"]), "r") as f:
            text = []
            for line in f:
                text.append(line)
        assert len(text) == 14
        assert text[0].startswith("STATUS")
        assert text[0].endswith("Running a set of steps\n")
        # debug and trace messages appear in post error log
        assert "DEBUG  A debug message\n" in text
        assert "DEBUG  A debug report that takes time to produce\n" in text
        assert "TRACE  A trace message\n" in text
