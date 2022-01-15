import argparse
import json
import os
from pathlib import Path
from typing import List, Optional, Tuple

from pydantic import BaseSettings, Extra


class Settings(BaseSettings):
    """"""

    class Config:
        extra = Extra.forbid


class Environment(Settings):
    name: str
    code: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None


class Server(Settings):
    name: str
    host: str
    defaultUser: Optional[str] = None
    defaultPassword: Optional[str] = None
    environments: List[Environment]


class ConfigFile(Settings):
    app: str
    databaseNamePattern: str
    servers: List[Server]


class DatabaseConfig(object):
    def __init__(self):
        self.confFile = Path("config") / "database-conf.json"
        if not self.confFile.is_file():
            print(f"Missing {self.confFile} configuration file. Copy the template and fill in the credentials")
            exit(1)

        cfg = json.loads(self.confFile.read_text("utf8"))

        self.conf = ConfigFile(**cfg)

    def getEnvironmentList(self) -> List[str]:
        envList = []

        for server in self.conf.servers:
            for env in server.environments:
                envList.append(env.name)

        return envList

    def getCredentials(self, envName: str) -> Tuple[Server, Environment, str, str]:

        for server in self.conf.servers:
            for env in server.environments:
                if env.name == envName:
                    username = env.user or server.defaultUser
                    password = env.password or server.defaultPassword
                    if username is None or password is None:
                        print(f"No user or password defined for environment: {envName}")
                        exit(1)
                    return (server, env, username, password)

        print(f"Cannot find environment {envName} in config file")
        exit(1)


if __name__ == "__main__":
    parentPath = Path(os.path.realpath(__file__)).parent.parent

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--generate-schema",
        action="store_true",
        help="Generate the json schema if the model has changed.",
    )
    args = parser.parse_args()
    generateSchema: bool = args.generate_schema

    if generateSchema:
        (parentPath / "config" / "json-schemas" / "database-conf.schema.json").write_text(
            ConfigFile.schema_json(indent=4)
        )
