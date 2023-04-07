import logging.config
import os

class InternalLogging:
    def __init__(self):
        self.logger = logging.getLogger("bmkg_weather")
        logging.config.fileConfig(f"{os.environ['WORK_DIR']}/config/logging.ini", disable_existing_loggers=False)

    def logging_info(self, message=None, **kwargs):

        data = {
            "message": message,
            "err": "",
        }
        data.update(
            {
                "meta": {
                    **kwargs
                }
            }
        )
        self.logger.info(data)

    def logging_error(self, message=None, exception=None, **kwargs):

        data = {
            "message": message,
            "err": exception,
        }
        data.update(
            {
                "meta": {
                    **kwargs
                }
            }
        )
        self.logger.exception(data)
    
    def logging_warning(self, message=None, warning_msg=None, **kwargs):

        data = {
            "message": message,
            "err": "",
        }
        data.update(
            {
                "meta": {
                    "warning_msg": warning_msg,
                    **kwargs
                }
            }
        )
        self.logger.warning(data)