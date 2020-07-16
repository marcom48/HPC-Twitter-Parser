import logging, sys

class MPILogger():

    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(self.setup_handler())
    
    def setup_handler(self):
        logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
        consoleHandler = logging.StreamHandler(sys.stdout)
        consoleHandler.setFormatter(logFormatter)
        return consoleHandler
    
    def log(self, output):
        self.logger.info(output)
    
    def error(self, output):
        self.logger.warn(output)