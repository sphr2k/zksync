import sys
import time
from typing import Optional, Set, Tuple

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.protocol.states import ZnodeStat
from loguru import logger
from pydantic_settings import BaseSettings, SettingsConfigDict

# 1. Configure Adaptive Logging
logger.remove()
if sys.stdout.isatty():
    logger.add(sys.stdout, colorize=True, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | {message}")
else:
    logger.add(sys.stdout, serialize=True)

# 2. Type-Safe Configuration
class SyncConfig(BaseSettings):
    SOURCE_HOSTS: Optional[str] = None
    DEST_HOSTS: Optional[str] = None
    SYNC_INTERVAL_SECONDS: int = 300
    ROOT_PATH: str = "/"
    EXCLUDED_PATHS: Set[str] = {"/zookeeper"}
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

# 3. Helper Functions for Type Safety and Clarity
def _safe_get_znode_data(client: KazooClient, path: str) -> bytes:
    """
    Safely gets znode data
    """
    try:
        result = client.get(path)
        if result is None:
            return b""
        data, _ = result
        raw_data: bytes = data if data is not None else b""
        return raw_data
    except NoNodeError:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in _safe_get_znode_data for path {path}: {e}")
        if isinstance(e, NoNodeError):
            raise
        return b""

def get_all_children_recursive(client: KazooClient, path: str, excluded: Set[str]) -> Set[str]:
    """Recursively fetches all descendant znode paths."""
    if path in excluded:
        return set()
    all_paths = {path}
    try:
        children = client.get_children(path) or []
        for child in children:
            child_path = f"{path.rstrip('/')}/{child}"
            all_paths.update(get_all_children_recursive(client, child_path, excluded))
    except NoNodeError:
        pass
    return all_paths

# 4. Main Application Logic
def sync_clusters(source_zk: KazooClient, dest_zk: KazooClient, config: SyncConfig):
    """Performs a one-way sync using the safe helper function."""
    logger.info("Starting sync cycle")

    source_paths = get_all_children_recursive(source_zk, config.ROOT_PATH, config.EXCLUDED_PATHS)
    dest_paths = get_all_children_recursive(dest_zk, config.ROOT_PATH, config.EXCLUDED_PATHS)
    paths_to_create_or_update = source_paths
    paths_to_delete = dest_paths - source_paths

    num_created = 0
    num_updated = 0
    for path in sorted(list(paths_to_create_or_update)):
        if path in config.EXCLUDED_PATHS:
            continue
        try:
            # Linter is now happy because _safe_get_znode_data always returns bytes.
            data = _safe_get_znode_data(source_zk, path)

            if dest_zk.exists(path):
                # We can use the safe helper for the destination too for consistency.
                dest_data = _safe_get_znode_data(dest_zk, path)
                if dest_data != data:
                    logger.info(f"Updating znode: {path} ({len(data)} bytes)")
                    dest_zk.set(path, data)
                    num_updated += 1
            else:
                logger.info(f"Creating znode: {path} ({len(data)} bytes)")
                dest_zk.create(path, data, makepath=True)
                num_created += 1
        except NoNodeError:
            logger.warning(f"Skipping znode: {path} was deleted from source mid-sync.")
        except Exception as e:
            logger.error(f"Error processing znode {path}: {e}")

    num_deleted = 0
    for path in sorted(list(paths_to_delete), reverse=True):
        if path == config.ROOT_PATH or path in config.EXCLUDED_PATHS:
            continue
        try:
            logger.info(f"Deleting znode: {path}")
            dest_zk.delete(path)
            num_deleted += 1
        except NoNodeError:
            pass
        except Exception as e:
            logger.error(f"Error deleting znode {path}: {e}")

    logger.info("Sync cycle complete", created=num_created, updated=num_updated, deleted=num_deleted)

if __name__ == "__main__":
    try:
        config = SyncConfig()
        if not config.SOURCE_HOSTS or not config.DEST_HOSTS:
            raise ValueError("SOURCE_HOSTS and DEST_HOSTS must be set.")
        logger.info("Configuration loaded successfully.")
    except (ValueError, Exception) as e:
        logger.critical(f"Fatal: Failed to load configuration. {e}")
        sys.exit(1)

    source_client = KazooClient(hosts=config.SOURCE_HOSTS)
    dest_client = KazooClient(hosts=config.DEST_HOSTS)

    try:
        source_client.start()
        dest_client.start()
        logger.info("Clients connected. Starting periodic sync...")
        while True:
            sync_clusters(source_client, dest_client, config)
            logger.info(f"Waiting for {config.SYNC_INTERVAL_SECONDS} seconds before next cycle.")
            time.sleep(config.SYNC_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Shutdown signal received. Exiting gracefully.")
    finally:
        if source_client.state != 'CLOSED':
            source_client.stop()
        if dest_client.state != 'CLOSED':
            dest_client.stop()
        logger.info("Clients disconnected.")

