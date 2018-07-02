#coding = utf-8
import logging
import time
import os

import config


info_log_file_path = os.path.join(config.PROJ_PATH, 'log/info_{}.log'.format(time.strftime("%Y-%b-%d")))
error_log_file_path = os.path.join(config.PROJ_PATH, 'log/error_{}.log'.format(time.strftime("%Y-%b-%d")))
formatter = logging.Formatter('%(asctime)s - %(filename)s - [line:%(lineno)d] - %(levelname)s - %(message)s')

info_logger = logging.getLogger("info_log")
error_logger = logging.getLogger("error_log")

info_logger.setLevel(logging.INFO)
error_logger.setLevel(logging.ERROR)

info_handler = logging.FileHandler(info_log_file_path, 'a')
info_handler.setLevel(logging.INFO)
info_handler.setFormatter(formatter)

error_handler = logging.FileHandler(error_log_file_path, 'a')
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(formatter)

test_handler = logging.StreamHandler()
test_handler.setFormatter(formatter)
test_handler.setLevel(logging.ERROR)

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