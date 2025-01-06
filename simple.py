import logging
import sys
print("hello")
logging.basicConfig(filename=r'C:\\Users\\vszal\\Documents\\task_output.log', level=logging.INFO)

logging.info("This is an info message")
logging.error("This is an error message")
logging.shutdown()
sys.exit(0)