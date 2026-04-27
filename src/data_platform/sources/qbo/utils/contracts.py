"""
src.data_platform.sources.qbo.utils.contracts

Purpose:
    - standardized data structure for program-created inputs

Exposed API:
    - `read_tokens()`   - load token file and convert it into `dict[str, TokenState]`
    - `write_tokens()`  - hierarchically write `dict[str, TokenState]` into a JSON file
    - `read_secrets()` - load workspace client ID and secrets and convert it into `dict[str, AuthCredentials]`

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

from data_platform.core.utils.filesystem import atomic_write_bytes

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
    workspace_name: str
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
    

