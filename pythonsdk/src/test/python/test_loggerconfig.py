import logging
import os
import shutil
import tempfile
import time
from os.path import exists

from loggerconfig import LoggerConfig

# The IDE and the command line have different working directories
LOGGER_CONFIG_FILE = 'logger.yaml'
LOGGER_CONFIG_FILE_ALT = 'logger-alt.yaml'
if not exists(LOGGER_CONFIG_FILE):
    LOGGER_CONFIG_FILE = 'src/test/python/logger.yaml'
    LOGGER_CONFIG_FILE_ALT = 'src/test/python/logger-alt.yaml'


def test_basic_config():
    config = LoggerConfig(config_files=[LOGGER_CONFIG_FILE], monitor_interval=0)
    config.configure_logging()
    assert logging.getLogger("testservice").level == logging.INFO
    assert logging.getLogger("loggerconfig").level == logging.DEBUG


def test_config_override():
    # logger-alt.yaml sets testservice to DEBUG
    config = LoggerConfig(config_files=[LOGGER_CONFIG_FILE_ALT, LOGGER_CONFIG_FILE], monitor_interval=0)
    config.configure_logging()
    assert logging.getLogger("testservice").level == logging.DEBUG
    assert logging.getLogger("loggerconfig").level == logging.INFO

    # Now with a file that doesn't exist (back to the defaults)
    config = LoggerConfig(config_files=['nosuchfile.yaml', LOGGER_CONFIG_FILE], monitor_interval=0)
    config.configure_logging()
    assert logging.getLogger("testservice").level == logging.INFO
    assert logging.getLogger("loggerconfig").level == logging.DEBUG


def test_config_monitoring():
    # Copy .yaml file to a temporary location
    temp_dir = tempfile.mkdtemp()
    temp_file = os.path.join(temp_dir, 'logger.yaml')
    shutil.copy(LOGGER_CONFIG_FILE, temp_file)

    # Start with the original file
    config = LoggerConfig(config_files=[temp_file], monitor_interval=1)
    try:
        config.configure_logging()
        assert logging.getLogger("testservice").level == logging.INFO
        assert logging.getLogger("loggerconfig").level == logging.DEBUG

        # Now change the file
        shutil.copy(LOGGER_CONFIG_FILE_ALT, temp_file)

        # Wait for the monitor to pick up the change
        time.sleep(2)

        # Confirm that the change was picked up
        assert logging.getLogger("testservice").level == logging.DEBUG
        assert logging.getLogger("loggerconfig").level == logging.INFO
    finally:
        config.stop()
