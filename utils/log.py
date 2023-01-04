import logging

def configure_logger(logger=None, level=logging.INFO , propagate=True):
    if logger is None:
        print(f"{__name__}: Creating a new logger")
        logger = logging.getLogger()
    else:
        print(f"{__name__}: Using an existing logger: {logger}")
    
    handler = logging.StreamHandler()
    formatter = logging.Formatter(fmt="{asctime} - {name} - {funcName} - {levelname} - {message}",
                                  datefmt='%Y-%m-%d %H:%M:%S',
                                  style='{'
                                 )
    
    handler.setFormatter(fmt=formatter)
    logger.addHandler(hdlr=handler)
    logger.setLevel(level=level)
    logger.propagate = propagate


class LogMixin:
    
    @property
    def logger(self) -> logging.Logger:
        try:
            return self._logger
        except AttributeError:
            self._logger = logging.getLogger(self.__class__.__module__ + '.' + self.__class__.__name__)
            return self._logger