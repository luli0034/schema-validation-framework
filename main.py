import yaml
import logging
import click
import json
import pkgutil
import importlib
from validator.EventValidator import EventValidator
from utils import get_logger, load_configs, sink, lookup_dest, load_event, payloads_mapping
SUCCESS_PATH='./test/success/'
UNSUCCESS_PATH='./test/fail/'

logger = get_logger(__name__)

def _on_success_callback(configs, event):
    # Write file into specified directory which is defined in job.yaml
    dest_str = configs['succ_dest']
    dest = lookup_dest(dest_str, event)
    if dest:
        dest = f'{SUCCESS_PATH}{dest}.txt'
    else:
        logger.error(f'Can not find the destination by {dest_str}, please check your job.conf')
        dest = f'{UNSUCCESS_PATH}db.event_error_log.txt'

    sink(event, dest)
    return 

def _on_fail_callback(event):
    # Write file into fixed directory
    dest = f'{UNSUCCESS_PATH}db.event_error_log.txt'
    sink(event, dest)
    return 

def process_event(jobpath, event):
    # 1. Load job configuration and import models
    configs = load_configs(jobpath)

    # 2-1. Check event is Json format
    try:
        event = json.loads(event)
    except json.JSONDecodeError as exc:
        logger.error(f"[Fail] Invalid JSON format")
        raise
    
    # 2-2. Check event_type exist
    try:
        event_type = event['event_type']
    except KeyError:
        logger.error("[Fail] The key 'event_type' is not exist in incoming event")
        _on_fail_callback(event)
        return

    # 3. Create Data model by event_type
    try:
        validator = EventValidator(configs[event_type], payloads_mapping)
    except KeyError as e:
        logger.error(f"[Fail] The key {e} is not exist in {payloads_mapping}")
        _on_fail_callback(event)
        return

    # 4. Verify the event
    if validator.verify(event):
        logger.info("[Success] {event_type} is valid with model {model}".format(event_type=event_type, model=validator.__class__.__name__))
        _on_success_callback(configs[event_type], event)
        return
    else:
        logger.info("[Fail] Eventtype {event_type} is invalid with model {model}".format(event_type=event_type, model=validator.__class__.__name__))
        _on_fail_callback(event)
        return

@click.group()
def main():
    pass

@click.command('process')
@click.option('--jobpath', default='./job.yaml', help='Configuration path of job')
@click.option('--eventpath', default=None, help='JSON file path of event')
def process_event_by_file(jobpath, eventpath):
    event = load_event(eventpath)
    process_event(jobpath, event)

@click.command('codegen')
@click.option('--jobpath', default='./job.yaml', help='Configuration path of job')
@click.option('--schema', default='./schema/EventBase.json', help='Configuration path of job')
def codegen(jobpath, schema):
    # 1. Load job configuration
    configs = load_configs(jobpath)['codegen']
    name = schema.split('/')[-1].split('.')[0]
    # 2. Generate model.py by user defined json
    import subprocess
    command = ["datamodel-codegen", "--input", f"{schema}", "--input-file-type", "jsonschema", "--output", f"{configs[name]}"]
    subprocess.run(command)

main.add_command(process_event_by_file)
main.add_command(codegen)

if __name__ == '__main__':
    main()