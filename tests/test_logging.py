from datetime import datetime
import os


def test_log():
    """Test basic log operation outside model operation"""
    from tm2py.config import LoggingConfig
    import tempfile

    with tempfile.TemporaryDirectory() as temp_dir:
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
            run_dir = temp_dir
            iter_component = None

        class TestException(Exception):
            pass

        controller = Controller()

        from tm2py.logger import Logger

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
                raise TestException("an error")
        except TestException:
            pass

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
            text[2], "%d-%b-%Y (%H:%M:%S):   Indented displayed     message with timestamp\n"
        )

        assert "Error during model run" in text[4]

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
