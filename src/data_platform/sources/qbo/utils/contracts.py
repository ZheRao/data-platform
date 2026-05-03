"""
src.data_platform.sources.qbo.utils.contracts

Purpose:
    - standardized data structure for program-created inputs

Exposed API:
    - `read_tokens()`   - load token file and convert it into `dict[str, TokenState]`
    - `write_tokens()`  - hierarchically write `dict[str, TokenState]` into a JSON file
    - `read_secrets()` - load workspace client ID and secrets and convert it into `dict[str, AuthCredentials]`
    - `construct_workspace_config()` - link workspace secrets with included entities

Exposed Structures:
    - ingestion contracts:
        - `TaskRecord` - data structure for tasks created for Spark jobs
    - auth contracts:
        - `WorkspaceAuthConfig` - data class for answering which credentials apply to which entities
        - `AuthCredentials` - data class for raw QBO OAuth app credentials
        - `TokenState` - data class for mutable persisted token state per entity

"""
from __future__ import annotations
from typing import TypedDict
from dataclasses import dataclass, asdict
from pathlib import Path
import json

from data_platform.core.utils.filesystem import atomic_write_bytes, read_configs

class TaskRecord(TypedDict):
    company: str
    start: str
    end: str

@dataclass(frozen=True)
class AuthCredentials:
    client_id: str 
    client_secret: str
    redirect_url: str 

@dataclass(frozen=True)
class TokenState:
    access_token: str 
    refresh_token: str
    realm_id: str

@dataclass(frozen=True)
class WorkspaceAuthConfig:
    credentials: AuthCredentials
    included_entities: tuple[str, ...]

def read_tokens(token_path: Path) -> dict[str, TokenState]:
    """
    Input:
        - `token_path`: path to where token is stored, should end in `.json`
    Output:
        - a dictionary contains `entity_name` as keys and `TokenState` objects as values
    """
    raw = json.loads(token_path.read_text(encoding="utf-8"))

    return {
        entity_name: TokenState(**token_data) 
        for entity_name, token_data in raw.items()
    }

def write_tokens(token_path: Path, tokens: dict[str, TokenState]) -> None:
    """
    Input:
        - `token_path`: path to store the token file
        - `tokens`: dictionary of `entity_name` and `TokenState`
    """
    final = {
        entity_name: asdict(token_data)
        for entity_name, token_data in tokens.items()
    }

    raw = json.dumps(final, indent=2).encode("utf-8")
    atomic_write_bytes(dst=token_path, data=raw)

def read_secrets(secret_path: Path) -> dict[str, AuthCredentials]:
    """
    Input:
        - `secret_path`: path to where workspace/secrets are stored, should end in `.json`
    Output:
        - a dictionary contains `workspace_name` as keys and `AuthCredentials` objects as values
    """
    raw = json.loads(secret_path.read_text(encoding="utf-8"))

    return {
        workspace_name: AuthCredentials(**secrets)
        for workspace_name, secrets in raw.items()
    }

def construct_workspace_config(secret_path:Path) -> dict[str,WorkspaceAuthConfig]:
    """
    Input:
        - `secret_path`: path to where workspace/secrets are stored, should end in `.json`
    Output:
        - a dictionary contains `workspace_name` as key and `WorkspaceAuthConfig` objects as values that contains
            - credentials: `AuthCredentials` objects
            - included_entities: list of entity names as string
    Note:
        - the `AuthCredentials` that contains `client_id`, `client_secret` came from secret file (top-level shape: dict[str, AuthCredentials])
        - the `WorkspaceAuthConfig.included_entities` came from external config (top-level shape: dict[str, list[str]])
        - the `workspace_name` (top-level keys) from secrets file must match `workspace_name` (top-level keys) from external config about workspace entity 
    """
    secrets = read_secrets(secret_path=secret_path)
    workspace_entity_config = read_configs(source_system="qbo",config_type="contracts", name="workspace_entity.json")
    external_config_keys = workspace_entity_config.keys()
    secrets_keys = secrets.keys()
    not_in_secrets = list(set(external_config_keys) - set(secrets_keys))
    if not_in_secrets: 
        raise ValueError(
            f"Inconsistent/missing workspace naming. \n\n"
            f"Workspace names in 'qbo/json_configs/contracts/workspace_entity.json' but not in secret file: \n"
            f"  missing_names = '{not_in_secrets}'\n"
            f"  full_names_in_config = '{list(external_config_keys)}'\n"
            f"  full_names_in_secret = '{list(secrets_keys)}'\n"
            f"Please ensure the workspace naming is consistent or add missing names for secret file at '{secret_path}'"
        )
    not_in_config = list(set(secrets_keys) - set(external_config_keys))
    if not_in_config:
        raise ValueError(
            f"Inconsistent/missing workspace naming. \n\n"
            f"Workspace names in in secret file but not in 'qbo/json_configs/contracts/workspace_entity.json': \n"
            f"  missing_names = '{not_in_config}'\n"
            f"  full_names_in_config = '{list(external_config_keys)}'\n"
            f"  full_names_in_secret = '{list(secrets_keys)}'\n"
            f"Please ensure the workspace naming is consistent or add missing names for workspace config at 'qbo/json_configs/contracts/workspace_entity.json'"
        )
    workspace  = {}
    for workspace_name in secrets_keys:
        workspace_object = WorkspaceAuthConfig(credentials=secrets[workspace_name], included_entities=workspace_entity_config[workspace_name])
        workspace.update({workspace_name:workspace_object})
    return workspace


    

