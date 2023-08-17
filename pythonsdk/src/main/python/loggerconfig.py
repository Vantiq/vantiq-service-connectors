import logging
import logging.config
import os
import time
from os.path import exists
from threading import Thread
from typing import List

import yaml


class LoggerConfig:
    """
    This class is used to configure logging for a service.  It will look for the YAML files specified by the
    "config_files" list until one of them is found.  If none of those are found it will look for the file named
    "logger.yaml".  Once a configuration file is found, it will be used to configure logging via "dictConfig".
    If none of them are found, it will use the default logging configuration.

    If the "monitor_interval" is greater than 0, it will start a background thread to monitor the chosen configuration
    file.  This check will occur every "monitor_interval" seconds.  If the file is modified, it will be reloaded.
    """
    DEFAULT_MONITORING_INTERVAL = 2 * 60

    def __init__(self, config_files: List[str] = None, monitor_interval: int = None):
        self._logger = logging.getLogger(__name__)
        self._config_files = config_files or []
        self._config_files.append('logger.yaml')
        self._monitor_interval = monitor_interval or self.DEFAULT_MONITORING_INTERVAL
        self._active_config_file = None
        self._monitor_thread = None
        self._stopped = False

    def configure_logging(self) -> None:
        for config_file in self._config_files:
            if exists(config_file):
                config = yaml.load(open(config_file), Loader=yaml.SafeLoader)
                logging.config.dictConfig(config)
                self._active_config_file = config_file
                logging.getLogger().info("Logging configured using %s", config_file)
                break

        if self._active_config_file is not None and self._monitor_interval > 0:
            # Start a background task to monitor the log files
            logging.getLogger().info("Checking %s for changes every %ss", self._active_config_file,
                                     self._monitor_interval)
            self._monitor_thread = Thread(target=self._monitor_config_file, daemon=True, name='Log Config Monitor')
            self._monitor_thread.start()

    def stop(self) -> None:
        self._stopped = True
        if self._monitor_thread is not None:
            self._monitor_thread.join()

    def _monitor_config_file(self) -> None:
        last_timestamp = os.path.getmtime(self._active_config_file)
        while not self._stopped:
            current_timestamp = os.path.getmtime(self._active_config_file)
            if current_timestamp != last_timestamp:
                config = yaml.load(open(self._active_config_file), Loader=yaml.SafeLoader)
                logging.config.dictConfig(config)
                last_timestamp = current_timestamp
            time.sleep(self._monitor_interval)
