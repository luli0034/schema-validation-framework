import yaml
import json
import logging
####################import payloads#############################
from models.PageViewPayload import PageViewPayload
from models.RegisterPayload import RegisterPayload
################################################################

def get_logger(name):
    logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=logFormatter, level=logging.DEBUG)
    logger = logging.getLogger(name)
    return logger

logger = get_logger(__name__)

payloads_mapping = {
    'PageView': PageViewPayload,
    'Register': RegisterPayload
}


def load_configs(config):
    with open(config, "r") as f:
        try:
            configs = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            logger.error(f"Failed to load {config}, {exc}")
            raise
    return configs

def load_event(event_path):
    """Load event from path, suppose the events are JSON format

    Parameters
    ----------
    event_path : str
        Example files are under folder ./test/testcases/

    Returns
    -------
    event
        JSON object if load successfully else simple string
    """
    with open(event_path, "r") as f:
        return f.read()


def sink(event, path):
    with open(path, 'a+') as f:
        f.write('{event}\n'.format(event=json.dumps(event)))

def lookup_dest(dest_str, event):
    """Lookup destination in event, destination of each event type is defined in job.yaml
       'event_payload.destination_bq' means find the value in event JSON by the keys ['event_payload', 'destination_bq'].

    Parameters
    ----------
    dest_str : str
        Defined in job.yaml, string
    event : JSON
        JSON format event

    Returns
    -------
    dest
        None if destination not found, else destination string (flie name)
    """
    paths = dest_str.split('.')
    try:
        while paths:
            event = event[paths.pop(0)]
    except KeyError as e:
        logger.error(f"Wrong key {e}")
        return None

    return event


