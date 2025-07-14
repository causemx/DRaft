import sys
from loguru import logger

class LoggerFactory:
    
    @staticmethod
    def get_logger(name: str, level="INFO"):
        logger.remove()
        
        logger.add(
            sink=sys.stderr, #stderr
            level=level,
            colorize=True,
            format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        )
        return logger.bind(name=name)