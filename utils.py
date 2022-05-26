import yaml
import json
import logging

logger = logging.getLogger(__name__)

def load_configs(config):
    with open(config, "r") as f:
        try:
            configs = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            logger.error(f"Failed to load {config}, {exc}")
            raise
    return configs

def load_event(event_path):
    with open(event_path, "r") as f:
        try:
            event = json.load(f)
        except json.JSONDecodeError as exc:
            logger.error(f"ERROR: Invalid JSON: {exc.msg}, line {exc.lineno}, column {exc.colno}")
            raise

    try:
        event_type = event['event_type']
    except KeyError:
        logger.error(f"The key of event_type is not exist in incoming event")
        raise

    return event_type, event

def sink(event, path):
    with open(path, 'w+') as f:
        json.dump(event, f, indent=4)

def lookup_dest(dest_str, event):
    paths = dest_str.split('.')
    try:
        while paths:
            event = event[paths.pop(0)]
    except KeyError as e:
        logger.error(f"Wrong key {e}")
        raise

    return event