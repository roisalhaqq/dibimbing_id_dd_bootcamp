import requests
import json
from libs.internal_logging import InternalLogging


class APICaller:

    def __init__(self):
        self.log = InternalLogging()

    def api_caller_get(self, url, headers=None, enable_log=True, trial_threshold=3, **kwargs):

        trial = 0

        while trial < trial_threshold:
            try:
                response = requests.get(url, headers=headers)
                if enable_log:
                    self.log.logging_info(message="made an api invocation using api_caller.py", type="API_CALLER_GET", status_code=response.status_code, log=response.text.strip(), url=url)

                return response
            
            except Exception as err:
                trial += 1
                if trial < trial_threshold:
                    self.log.logging_warning(warning_msg=err, trial=trial, **kwargs)
                else:
                    self.log.logging_error(exception=err, trial=trial, **kwargs)
                continue