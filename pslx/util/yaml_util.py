import os
import yaml


def yaml_to_dict(file_name):
    if not os.path.exists(file_name):
        return {}
    else:
        with open(file_name, 'r') as infile:
            return yaml.safe_load(infile)


if __name__ == '__main__':
    print(yaml_to_dict(file_name='config/tool_config.yaml'))
