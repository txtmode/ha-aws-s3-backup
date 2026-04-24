"""Backup platform for the AWS S3 integration."""

from collections.abc import AsyncIterator, Callable, Coroutine
import functools
import json
import logging
from time import time
from typing import Any, cast

from botocore.exceptions import BotoCoreError

from homeassistant.components.backup import (
    AgentBackup,
    BackupAgent,
    BackupAgentError,
    BackupNotFound,
    OnProgressCallback,
    suggested_filename,
)
from homeassistant.core import HomeAssistant, callback

from . import S3ConfigEntry
from .const import CONF_BUCKET, CONF_PREFIX, DATA_BACKUP_AGENT_LISTENERS, DOMAIN
from .helpers import async_list_backups_from_s3

_LOGGER = logging.getLogger(__name__)
CACHE_TTL = 300

# S3 part size requirements: 5 MiB to 5 GiB per part
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
# We set the threshold to 20 MiB to avoid too many parts.
# Note that each part is allocated in the memory.
MULTIPART_MIN_PART_SIZE_BYTES = 20 * 2**20


def handle_boto_errors[T](
    func: Callable[..., Coroutine[Any, Any, T]],
) -> Callable[..., Coroutine[Any, Any, T]]:
    """Handle BotoCoreError exceptions by converting them to BackupAgentError."""

    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> T:
        """Catch BotoCoreError and raise BackupAgentError."""
        try:
            return await func(*args, **kwargs)
        except BotoCoreError as err:
            error_msg = f"Failed during {func.__name__}"
            raise BackupAgentError(error_msg) from err

    return wrapper


async def async_get_backup_agents(
    hass: HomeAssistant,
) -> list[BackupAgent]:
    """Return a list of backup agents."""
    entries: list[S3ConfigEntry] = hass.config_entries.async_loaded_entries(DOMAIN)
    return [S3BackupAgent(hass, entry) for entry in entries]


@callback
def async_register_backup_agents_listener(
    hass: HomeAssistant,
    *,
    listener: Callable[[], None],
    **kwargs: Any,
) -> Callable[[], None]:
    """Register a listener to be called when agents are added or removed.

    :return: A function to unregister the listener.
    """
    hass.data.setdefault(DATA_BACKUP_AGENT_LISTENERS, []).append(listener)

    @callback
    def remove_listener() -> None:
        """Remove the listener."""
        hass.data[DATA_BACKUP_AGENT_LISTENERS].remove(listener)
        if not hass.data[DATA_BACKUP_AGENT_LISTENERS]:
            del hass.data[DATA_BACKUP_AGENT_LISTENERS]

    return remove_listener


def suggested_filenames(backup: AgentBackup) -> tuple[str, str]:
    """Return the suggested filenames for the backup and metadata files."""
    base_name = suggested_filename(backup).rsplit(".", 1)[0]
    return f"{base_name}.tar", f"{base_name}.metadata.json"


class S3BackupAgent(BackupAgent):
    """Backup agent for the S3 integration."""

    domain = DOMAIN

    def __init__(self, hass: HomeAssistant, entry: S3ConfigEntry) -> None:
        """Initialize the S3 agent."""
        super().__init__()
        self._client = entry.runtime_data.client
        self._bucket: str = entry.data[CONF_BUCKET]
        self.name = entry.title
        self.unique_id = entry.entry_id
        self._backup_cache: dict[str, AgentBackup] = {}
        self._cache_expiration = time()
        self._prefix: str = entry.data.get(CONF_PREFIX, "")

    def _with_prefix(self, key: str) -> str:
        """Add prefix to a key if configured."""
        if not self._prefix:
            return key
        return f"{self._prefix}/{key}"

    @handle_boto_errors
    async def async_download_backup(
        self,
        backup_id: str,
        **kwargs: Any,
    ) -> AsyncIterator[bytes]:
        """Download a backup file."""
        backup = await self._find_backup_by_id(backup_id)
        tar_filename, _ = suggested_filenames(backup)

        response = await self._client.get_object(
            Bucket=self._bucket, Key=self._with_prefix(tar_filename)
        )
        return response["Body"].iter_chunks()

    async def async_upload_backup(
        self,
        *,
        open_stream: Callable[[], Coroutine[Any, Any, AsyncIterator[bytes]]],
        backup: AgentBackup,
        on_progress: OnProgressCallback,
        **kwargs: Any,
    ) -> None:
        """Upload a backup."""
        tar_filename, metadata_filename = suggested_filenames(backup)

        try:
            if backup.size < MULTIPART_MIN_PART_SIZE_BYTES:
                await self._upload_simple(tar_filename, open_stream)
            else:
                await self._upload_multipart(tar_filename, open_stream, on_progress)

            metadata_content = json.dumps(backup.as_dict())
            await self._client.put_object(
                Bucket=self._bucket,
                Key=self._with_prefix(metadata_filename),
                Body=metadata_content,
            )
        except BotoCoreError as err:
            raise BackupAgentError("Failed to upload backup") from err
        else:
            self._cache_expiration = time()

    async def _upload_simple(
        self,
        tar_filename: str,
        open_stream: Callable[[], Coroutine[Any, Any, AsyncIterator[bytes]]],
    ) -> None:
        """Upload a small file using simple upload."""
        stream = await open_stream()
        file_data = bytearray()
        async for chunk in stream:
            file_data.extend(chunk)

        await self._client.put_object(
            Bucket=self._bucket,
            Key=self._with_prefix(tar_filename),
            Body=bytes(file_data),
        )

    async def _upload_multipart(
        self,
        tar_filename: str,
        open_stream: Callable[[], Coroutine[Any, Any, AsyncIterator[bytes]]],
        on_progress: OnProgressCallback,
    ) -> None:
        """Upload a large file using multipart upload."""
        multipart_upload = await self._client.create_multipart_upload(
            Bucket=self._bucket,
            Key=self._with_prefix(tar_filename),
        )
        upload_id = multipart_upload["UploadId"]
        try:
            parts: list[dict[str, Any]] = []
            part_number = 1
            buffer = bytearray()
            offset = 0
            bytes_uploaded = 0

            stream = await open_stream()
            async for chunk in stream:
                buffer.extend(chunk)

                view = memoryview(buffer)
                try:
                    while len(buffer) - offset >= MULTIPART_MIN_PART_SIZE_BYTES:
                        start = offset
                        end = offset + MULTIPART_MIN_PART_SIZE_BYTES
                        part_data = view[start:end]
                        offset = end

                        part = await cast(Any, self._client).upload_part(
                            Bucket=self._bucket,
                            Key=self._with_prefix(tar_filename),
                            PartNumber=part_number,
                            UploadId=upload_id,
                            Body=part_data.tobytes(),
                        )
                        parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                        bytes_uploaded += len(part_data)
                        on_progress(bytes_uploaded=bytes_uploaded)
                        part_number += 1
                finally:
                    view.release()

                if offset and offset >= MULTIPART_MIN_PART_SIZE_BYTES:
                    buffer = bytearray(buffer[offset:])
                    offset = 0

            if offset < len(buffer):
                remaining_data = memoryview(buffer)[offset:]
                part = await cast(Any, self._client).upload_part(
                    Bucket=self._bucket,
                    Key=self._with_prefix(tar_filename),
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=remaining_data.tobytes(),
                )
                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                bytes_uploaded += len(remaining_data)
                on_progress(bytes_uploaded=bytes_uploaded)

            await cast(Any, self._client).complete_multipart_upload(
                Bucket=self._bucket,
                Key=self._with_prefix(tar_filename),
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

        except BotoCoreError:
            try:
                await self._client.abort_multipart_upload(
                    Bucket=self._bucket,
                    Key=self._with_prefix(tar_filename),
                    UploadId=upload_id,
                )
            except BotoCoreError:
                _LOGGER.exception("Failed to abort multipart upload")
            raise

    @handle_boto_errors
    async def async_delete_backup(self, backup_id: str, **kwargs: Any) -> None:
        """Delete a backup file."""
        backup = await self._find_backup_by_id(backup_id)
        tar_filename, metadata_filename = suggested_filenames(backup)

        await self._client.delete_object(
            Bucket=self._bucket, Key=self._with_prefix(tar_filename)
        )
        await self._client.delete_object(
            Bucket=self._bucket, Key=self._with_prefix(metadata_filename)
        )

        self._cache_expiration = time()

    @handle_boto_errors
    async def async_list_backups(self, **kwargs: Any) -> list[AgentBackup]:
        """List backups."""
        backups = await self._list_backups()
        return list(backups.values())

    @handle_boto_errors
    async def async_get_backup(self, backup_id: str, **kwargs: Any) -> AgentBackup:
        """Return a backup."""
        return await self._find_backup_by_id(backup_id)

    async def _find_backup_by_id(self, backup_id: str) -> AgentBackup:
        """Find a backup by its backup ID."""
        backups = await self._list_backups()
        if backup := backups.get(backup_id):
            return backup
        raise BackupNotFound(f"Backup {backup_id} not found")

    async def _list_backups(self) -> dict[str, AgentBackup]:
        """List backups, using a cache if possible."""
        if time() <= self._cache_expiration:
            return self._backup_cache

        backups_list = await async_list_backups_from_s3(
            self._client, self._bucket, self._prefix
        )
        self._backup_cache = {b.backup_id: b for b in backups_list}
        self._cache_expiration = time() + CACHE_TTL

        return self._backup_cache
