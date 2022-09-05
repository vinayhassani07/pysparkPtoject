import logging

# logging.basicConfig(level=logging.DEBUG)
# logging.debug(" this is a debug message")
# logging.info(" this is a info message")
# logging.warning(" this is a warning message")
# logging.error(" this is a Error message")
# logging.critical(" this is a Critical message")

# try:
#     print(sal)
# except Exception as exp:
#     logging.basicConfig(level=logging.DEBUG, filename='loggingTest.log', filemode='w',
#                         format =' %(name)s - %(levelname)s - %(message)s')
#     logging.error('Error Occurred.', exc_info=

# CREATE THE CUSTOM LOGGER
logger = logging.getLogger(__name__)

print(__name__)

# CREATE THE HANDLER
f_handler= logging.FileHandler('loggingTest.log', mode='w')
f_handler.setLevel(logging.ERROR)

# CREATE THE FORMATTER
f_format= logging.Formatter(' %(name)s - %(levelname)s - %(message)s')
f_handler.setFormatter(f_format)

logger.addHandler(f_handler)

logger.error("This is the Error")

