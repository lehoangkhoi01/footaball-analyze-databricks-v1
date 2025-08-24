import logging
import sys


class LoggerConfig:
    """
    A configurable logger class for Databricks pipelines.
    """
    
    def __init__(self, logger_name="DLTLogger", level=logging.INFO, use_stderr=False):
        """
        Initialize the logger configuration.
        
        Args:
            logger_name (str): Name of the logger
            level (int): Logging level (e.g., logging.INFO, logging.DEBUG)
            use_stderr (bool): If True, logs to stderr; if False, logs to stdout
        """
        self.logger_name = logger_name
        self.level = level
        self.use_stderr = use_stderr
        self._logger = None
    
    def get_logger(self):
        """
        Get or create the configured logger instance.
        
        Returns:
            logging.Logger: Configured logger instance
        """
        if self._logger is None:
            self._logger = self._setup_logger()
        return self._logger
    
    def _setup_logger(self):
        """
        Set up the logger with the specified configuration.
        
        Returns:
            logging.Logger: Configured logger instance
        """
        logger = logging.getLogger(self.logger_name)
        logger.setLevel(self.level)
        
        # Avoid duplicate handlers if rerun in notebook
        if not logger.handlers:
            # Choose output stream
            stream = sys.stderr if self.use_stderr else sys.stdout
            handler = logging.StreamHandler(stream)
            
            # Set formatter
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            
            logger.addHandler(handler)
        
        return logger
    
    @classmethod
    def create_default_logger(cls):
        """
        Create a logger with default Databricks-friendly settings.
        
        Returns:
            logging.Logger: Default configured logger
        """
        config = cls(logger_name="DLTLogger", level=logging.INFO, use_stderr=False)
        return config.get_logger()
    
    @classmethod
    def create_debug_logger(cls):
        """
        Create a logger configured for debug mode.
        
        Returns:
            logging.Logger: Debug configured logger
        """
        config = cls(logger_name="DLTDebugLogger", level=logging.DEBUG, use_stderr=True)
        return config.get_logger()


# Convenience function for quick setup
def get_pipeline_logger(name="DLTLogger", debug=False):
    """
    Quick function to get a pre-configured logger for Databricks pipelines.
    
    Args:
        name (str): Logger name
        debug (bool): If True, sets up debug logging to stderr
        
    Returns:
        logging.Logger: Configured logger instance
    """
    if debug:
        return LoggerConfig.create_debug_logger()
    else:
        config = LoggerConfig(logger_name=name)
        return config.get_logger()