import yaml
import os

def read_yaml_file(filename):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, filename)
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    return data

if __name__ == "__main__":
    content = read_yaml_file('./config.yml')
    print(content)
