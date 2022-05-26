import yaml
import logging
import click
from validator.EventValidator import EventValidator
from utils import load_configs, load_event, sink, lookup_dest

logger = logging.getLogger(__name__)

def _on_success_callback(configs, event):
    dest = lookup_dest(configs['succ_dest'], event)
    return 

def _on_fail_callback(configs, event):

    pass

@click.command()
@click.option('--jobpath', default='./job.yaml', help='Configuration path of job')
@click.option('--eventpath', default='./test/test_pageview_success.json', help='Json file path of event')
def main(jobpath, eventpath):
    # 1. Load job configuration
    configs = load_configs(jobpath)
    # 2. Load Event data (json format)
    event_type, event = load_event('./test/test_pageview_success.json')
    # 3. Create Data model by event_type
    validator = EventValidator(configs[event_type])
    # 4. Verify the event
    if validator.verify(event):
        logger.info("[Success] {event_type} is valid with model {model}".format(event_type=event_type, model=validator.__class__.__name__))
        _on_success_callback(configs[event_type], event)
    else:
        logger.info("[Unsuccess] Eventtype {event_type} is invalid with model {model}".format(event_type=event_type, model=validator.__name__))
        _on_fail_callback(configs[event_type], event)
        

if __name__ == '__main__':
    main()