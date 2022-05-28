from pydantic import ValidationError
import logging
from validator.EventBuilder import EventBuilder

logger = logging.getLogger(__name__)

class EventValidator(EventBuilder):
    
    def __init__(self, configs, mapping):
        self.task = configs['task']
        super().__init__(configs, mapping[self.task])
    
    def verify(self, event):
        try:
            self.model(**event)
        except ValidationError as exc:
            logger.error(f"Invalid schema {exc}")
            return False
        
        return True