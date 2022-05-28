import json
from pydantic import BaseModel, create_model
from enum import Enum
from models.EventBase import EventBase
class EventBuilder:
    
    def __init__(self, configs, PayloadModel):
        self._EventBase = EventBase
        if configs['enums']:
            self._enums = self.get_enums(configs['enums'])
            self._payload_model = self.get_payload('{task}PayLoad'.format(task=configs['task']), PayloadModel, self._enums)
        else:
            self._payload_model = PayloadModel
        self.model = self.get_model(name=configs['task'], payload=self._payload_model)        

    def get_model(self, name, payload):
        return create_model(
            name,
            __base__=self._EventBase,
            event_payload=(payload, ...)
        )

    def get_enums(self, enum_configs):
        fields = {}
        
        for name, path in enum_configs.items():
            fields[name] = (Enum(name, self._load_enum(path)),...)
        
        return fields
    
    def get_payload(self, name, PayloadModel,fields):
        return create_model(
            name,
            __base__= PayloadModel,
            **fields
        )
    
    def _load_enum(self, enum_path):
        enums = json.load(open(enum_path))
        return {enum:enum for enum in enums}