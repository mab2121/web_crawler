"""
seed_loader.py
Mehran Ali Banka - Sep 2023
----------------------------
This is a Logging class created by using the logging library in python
For this purpose of this project, this will only output to a local file
"""
import logging

class Logger:
    def __init__(self, name, log_file=None):
        
        # create an instance of the logging object
        # with a specific name
        self.logger = logging.getLogger(name)
        # set the current level to debug
        self.logger.setLevel(logging.DEBUG)

        # Create a formatter for the log messages
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Create the file handler. The code ensures that a log file is supplied
        # It takes the default value from the Parameters.py file if not overriden
        
        fh = logging.FileHandler(log_file)
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        
    def warning(self, message):
        self.logger.warning(message)

    def info(self, message):
        self.logger.info(message)
    
    def critical(self, message):
        self.logger.critical(message)

    def error(self, message):
        self.logger.error(message)

    def end_section(self):
        self.logger.info(("=")*100)    

    

if __name__ == '__main__':
    # Create an instance of the logger
    logger = Logger('my_logger', 'my_log.log')


