# this file handles the application level configs and variables
import os


def read_env_var(name:str) -> str:
    return os.environ.get(name, f"Warning: The following environmental variable is not set .. {name}")


APP_ENV = 'development'