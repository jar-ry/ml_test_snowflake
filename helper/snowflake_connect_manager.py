import os
import yaml
from typing import Dict, Optional
from pathlib import Path
from snowflake.snowpark import Session


class SnowflakeConnectionManager:
    def __init__(
        self,
        config_file: Optional[str] = None,
        profile: Optional[str] = "default",
        env_prefix: Optional[str] = "SNOWFLAKE_"
    ):
        """
        Manage Snowflake connection parameters and create a Snowpark Session.

        Args:
            config_file (str, optional): Path to YAML file with credentials.
            profile (str, optional): Profile name inside YAML. Defaults to "default".
            env_prefix (str, optional): Environment variable prefix. Defaults to "SNOWFLAKE_".
        """
        self.config_file = config_file or os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "../conf/local/credentials.yml"
        )
        self.profile = profile
        self.env_prefix = env_prefix

    def _from_yaml(self) -> Dict[str, str]:
        if not os.path.exists(self.config_file):
            return {}
        with open(self.config_file, "r") as f:
            data = yaml.safe_load(f)
        return data.get(self.profile, {})

    def _from_env(self) -> Dict[str, str]:
        keys = [
            "ACCOUNT", "USER", "PASSWORD", "ROLE", "WAREHOUSE", "DATABASE", "SCHEMA"
        ]
        return {
            key.lower(): os.getenv(f"{self.env_prefix}{key}")
            for key in keys
            if os.getenv(f"{self.env_prefix}{key}")
        }

    def get_connection_parameters(self) -> Dict[str, str]:
        """
        Merge YAML and environment-based config.
        Environment variables override YAML config.
        """
        params = self._from_yaml()
        params.update(self._from_env())
        return params

    def create_session(self) -> Session:
        """
        Create and return a new Snowpark Session.
        """
        config = self.get_connection_parameters()
        if not config:
            raise ValueError("No valid Snowflake connection configuration found.")
        return Session.builder.configs(config).create()
