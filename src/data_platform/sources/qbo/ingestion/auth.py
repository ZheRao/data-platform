"""
src.data_platform.sources.qbo.ingestion.auth

Purpose:
    - auth system that rotates entities' tokens based on workspace secrets

Exposed API:
    - `refresh_entity()`: refreshes one entity, returns updated `TokenState`
    - `refresh_auth()`: refreshes entire workspaces or selected entities, writes updated token to disk and returns token dictionary
"""
from pathlib import Path
from collections import Counter
from intuitlib.client import AuthClient

from data_platform.sources.qbo.utils.contracts import AuthCredentials, TokenState, WorkspaceAuthConfig, write_tokens

def refresh_entity(client: AuthCredentials, old_token_state: TokenState) -> TokenState:
    """
    Input:
        - `client`: `AuthCredentials` object contains `['client_id', 'client_secret', 'redirect_url']` - workspace level
        - `old_token_state`: `TokenState` object contains `['access_token', 'refresh_token', 'realm_id']` - entity level
    Output:
        - `TokenState` object with updated tokens
    Purpose:
        - refreshes one entity and return the new `TokenState` object
    """
    auth_client = AuthClient(
        client_id=client.client_id, 
        client_secret=client.client_secret,
        redirect_uri=client.redirect_url,
        environment = "production"
    )
    auth_client.access_token = old_token_state.access_token
    auth_client.refresh_token = old_token_state.refresh_token
    auth_client.realm_id = old_token_state.realm_id
    auth_client.refresh()
    updated_token_state = TokenState(
        access_token=auth_client.access_token, 
        refresh_token=auth_client.refresh_token,
        realm_id=old_token_state.realm_id
    )
    return updated_token_state

def refresh_auth(
    workspace: dict[str,WorkspaceAuthConfig],
    token_dict: dict[str, TokenState],
    token_path: Path,
    rotate_all: bool = True,
    entities_to_rotate_input: list[str]|None = None,
) -> dict[str, TokenState]:
    """
    Input:
        - `workspace`: a dictionary where keys are workspace names and values are `WorkspaceAuthConfig` objects containing secrets to the workspace
        - `token_dict`: a dictionary where keys are entity names and values are `TokenState` objects containing auth tokens to the entities
        - `token_path`: path to store the token file
        - `rotate_all`: whether to rotate all workspace and all entities contained inside each workspace
        - `entities_to_rotate_input`: a list of entity names to refresh, default `None`
    Output:
        - a dictionary of `entity names : TokenState` with rotated auth tokens
    Note:
        - function will check and raise errors for the following scenarios
            0. An entity belongs to more than one workspaces.
            1. `token_dict` contains entity names that are not included in `workspace[workspace_name].included_entities` for all `workspace_name`
            2. `entities_to_rotate_input` is None or empty when `rotate_all` is `False`
            3. `entities_to_rotate_input` contains entity names not in `token_dict.keys()`
        - function will tolerate the following edge cases:
            1. if `workspace` contains more entities than in `token_dict`, function ignores it
        - function will check all conditions first before any token rotation
        - function will write immediately after every rotate to avoid stale file state preventing future re-rotate
    """
    # validation
    entities_workspace = []
    for workspace_name, workspace_config in workspace.items():
        entities_workspace.extend(workspace_config.included_entities)
    # check #0
    entity_counts = Counter(entities_workspace)
    duplicate_entities = sorted(
        entity for entity, count in entity_counts.items()
        if count > 1
    )

    if duplicate_entities:
        raise ValueError(
            f"Entities assigned to multiple workspaces.\n\n"
            f"  duplicate_entities = {duplicate_entities}\n"
            f"Each entity must belong to exactly one workspace."
        )
    # check #1
    entities_token_file = list(token_dict.keys())
    missing_entity = list(set(entities_token_file) - set(entities_workspace))
    if missing_entity:
        raise ValueError(
            f"Token dictionary contains entities not declared in workspace config. \n\n"
            f"  entities_missing = '{missing_entity}'\n"
            f"  entities_in_workspace_config = '{entities_workspace}'\n"
            f"  entities_in_token_dict = '{entities_token_file}'\n"
            f"Please double check token dictionary passed to function"
        )
    entities_to_rotate = entities_token_file
    if not rotate_all:
        if entities_to_rotate_input:
            # check #2
            missing_entity2 = list(set(entities_to_rotate_input) - set(entities_token_file))
            if missing_entity2:
                raise ValueError(
                    f"Missing Entity from Token Dictionary. \n\n"
                    f"  entities_missing = '{missing_entity2}'\n"
                    f"  entities_in_token_dict = '{entities_token_file}'\n"
                    f"  entities_in_rotation_input = '{entities_to_rotate_input}'\n"
                    f"Please ensure the desired rotation workspace input matches the workspace config input"
                )
            entities_to_rotate = entities_to_rotate_input
        else:
            # check #3
            raise ValueError(
                f"Missing 'entities_to_rotate_input' when 'rotate_all' set to 'False'"
            )
    # rotation
    for workspace_name, workspace_config in workspace.items():
        client = workspace_config.credentials
        for entity in workspace_config.included_entities:
            if entity not in entities_to_rotate:
                continue 
            else:
                new_token_state = refresh_entity(client=client, old_token_state=token_dict[entity])
                token_dict[entity] = new_token_state 
                write_tokens(token_path=token_path, tokens=token_dict)
    return token_dict
    