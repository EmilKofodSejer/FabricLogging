# FabrigLogger class

The FabricLogger class is the entrance for error messages, all LogRecords are passed through the FabricLogger class to the handlers.

# LogRecord class

The object for everything we log, it mostly just unites all the variables necessary to create a log entrance.

# CSVHandler 

# Quick guide

Essentially this is how you set it up 

logger = FabricLogger("Logger", workspace_name = True, notebook_name = True)
csv_log = CSVHandler("csvhandler")
logger.addHandler(csv_log)

logger.debug("Debug message")
logger.info("Info message")
logger.warning("Warning message")
logger.error("Error message")
logger.critical("Critical message")

csv_log.commit_csv_to_delta

