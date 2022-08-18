# sal=100
# try:
#     print(sal)
# except IOError:
#     print("Exception occurs!!!")
# except :
#     print("Unknown Errors !!!")
# else:
#     print("No Error")
# finally:
#     print("This must execute")

# try:
#     print(sal)
# except Exception as arg:
#     print("Alert !!! " + str(arg))

# USER define Exception
import logging
import sys

from py4j.clientserver import logger

sal=-10
try:
    if sal<=0:
        raise Exception("Salary can not be less than 0. ")
except Exception as ext:
#print("ERROR !!! " + str(ext))
logger.error("Error Occured" + ext)
# Log the Error in the database
# Send an Email Notification
# raise
# sys.exit(0)

