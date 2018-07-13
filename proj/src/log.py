#coding = utf-8
import logging
import logging.handlers
import time
import os

import config


info_log_file_path = os.path.join(config.PROJ_PATH, 'log/info.log')
error_log_file_path = os.path.join(config.PROJ_PATH, 'log/error.log')
formatter = logging.Formatter('%(asctime)s - %(filename)s - [line:%(lineno)d] - %(levelname)s - %(message)s')

info_logger = logging.getLogger("info_log")
error_logger = logging.getLogger("error_log")

info_logger.setLevel(logging.INFO)
error_logger.setLevel(logging.ERROR)

info_handler = logging.handlers.TimedRotatingFileHandler(info_log_file_path, when="midnight")
info_handler.setLevel(logging.INFO)
info_handler.setFormatter(formatter)

error_handler = logging.handlers.TimedRotatingFileHandler(error_log_file_path, when="midnight")
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(formatter)

test_handler = logging.StreamHandler()
test_handler.setFormatter(formatter)
test_handler.setLevel(logging.INFO)

info_logger.addHandler(info_handler)
info_logger.addHandler(test_handler)
error_logger.addHandler(error_handler)

#infoLogger.debug("debug message")
#infoLogger.info("info message")
#infoLogger.warn("warn message")
# # 下面这行会同时打印在文件和终端上
#infoLogger.error("error message")
#
#errorLogger.error("error message")
#errorLogger.critical("critical message")