import yaml
import os

# def load_config(path="config/config.yaml"):
#     with open(path, 'r') as file:
#         return yaml.safe_load(file)
def load_config():
    project_root = os.path.dirname(os.path.dirname(__file__))  # refs_data_quality_pipeline
    config_path = os.path.join(project_root, 'config', 'config.yaml')
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)