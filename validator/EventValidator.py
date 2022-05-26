from pydantic import ValidationError
import logging
from validator.BaseValidator import BaseValidator
####################import payloads#############################
from models.PageViewPayload import PageViewPayload
from models.RegisterPayload import RegisterPayload
################################################################

logger = logging.getLogger(__name__)

class EventValidator(BaseValidator):
    
    def __init__(self, configs):
        self.task = configs['task']
        self.payloads = {
            'PageView': PageViewPayload,
            'Register': RegisterPayload
        }
        super().__init__(configs, self.payloads[self.task])
    
    def verify(self, event):
        try:
            self.model(**event)
        except ValidationError as exc:
            logger.error(f"Invalid schema {exc}")
            return False
        
        return True