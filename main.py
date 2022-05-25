import yaml
from validator.EventValidator import EventValidator

with open("job.yaml", "r") as f:
    try:
        configs = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        print(exc)

# a = PageviewValidator(configs['PageView'])
b = EventValidator(configs['Register'])
print(123)