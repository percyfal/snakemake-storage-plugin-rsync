import os
from pathlib import Path
import shutil
import subprocess
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, List
from urllib.parse import urlparse

import sysrsync

from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_provider import (  # noqa: F401
    StorageProviderBase,
    StorageQueryValidationResult,
    ExampleQuery,
    Operation,
    QueryType,
)
from snakemake_interface_storage_plugins.storage_object import (
    StorageObjectRead,
    StorageObjectWrite,
    StorageObjectGlob,
    StorageObjectTouch,
    retry_decorator,
)
from snakemake_interface_storage_plugins.io import (
    IOCacheStorageInterface,
    Mtime,
)
from snakemake_interface_common.utils import lutime


# Optional:
# Define settings for your storage plugin (e.g. host url, credentials).
# They will occur in the Snakemake CLI as --storage-<storage-plugin-name>-<param-name>
# Make sure that all defined fields are 'Optional' and specify a default value
# of None or anything else that makes sense in your case.
# Note that we allow storage plugin settings to be tagged by the user. That means,
# that each of them can be specified multiple times (an implicit nargs=+), and
# the user can add a tag in front of each value (e.g. tagname1:value1 tagname2:value2).
# This way, a storage plugin can be used multiple times within a workflow with different
# settings.
# @dataclass
# class StorageProviderSettings(StorageProviderSettingsBase):
#     myparam: Optional[int] = field(
#         default=None,
#         metadata={
#             "help": "Some help text",
#             # Optionally request that setting is also available for specification
#             # via an environment variable. The variable will be named automatically as
#             # SNAKEMAKE_<storage-plugin-name>_<param-name>, all upper case.
#             # This mechanism should only be used for passwords, usernames, and other
#             # credentials.
#             # For other items, we rather recommend to let people use a profile
#             # for setting defaults
#             # (https://snakemake.readthedocs.io/en/stable/executing/cli.html#profiles).
#             "env_var": False,
#             # Optionally specify a function that parses the value given by the user.
#             # This is useful to create complex types from the user input.
#             "parse_func": ...,
#             # If a parse_func is specified, you also have to specify an unparse_func
#             # that converts the parsed value back to a string.
#             "unparse_func": ...,
#             # Optionally specify that setting is required when the executor is in use.
#             "required": True,
#         },
#     )


# Required:
# Implementation of your storage provider
# This class can be empty as the one below.
# You can however use it to store global information or maintain e.g. a connection
# pool.
class StorageProvider(StorageProviderBase):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.
        pass

    @classmethod
    def example_queries(cls) -> List[ExampleQuery]:
        """Return an example queries with description for this storage provider (at
        least one)."""
        return [
            ExampleQuery(
                query="rsync://test/test.txt",
                type=QueryType.INPUT,
                description="A rsync URL",
            )
        ]


    def rate_limiter_key(self, query: str, operation: Operation) -> Any:
        """Return a key for identifying a rate limiter given a query and an operation.

        This is used to identify a rate limiter for the query.
        E.g. for a storage provider like http that would be the host name.
        For s3 it might be just the endpoint URL.
        """
        ...

    def default_max_requests_per_second(self) -> float:
        """Return the default maximum number of requests per second for this storage
        provider."""
        ...

    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        ...

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """Return whether the given query is valid for this storage provider."""
        # Ensure that also queries containing wildcards (e.g. {sample}) are accepted
        # and considered valid. The wildcards will be resolved before the storage
        # object is actually used.
        try:
            parsed = urlparse(query)
        except Exception as e:
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason=f"cannot be parsed as URL ({e})",
            )
        if not (parsed.scheme == "rsync"):
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason="scheme must be rsync",
            )
        return StorageQueryValidationResult(
            query=query,
            valid=True,
        )



# Required:
# Implementation of storage object. If certain methods cannot be supported by your
# storage (e.g. because it is read-only see
# snakemake-storage-http for comparison), remove the corresponding base classes
# from the list of inherited items.
class StorageObject(
    StorageObjectRead,
    StorageObjectWrite,
    StorageObjectGlob,
):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.
        parsed = urlparse(self.query)
        query = f"{parsed.netloc}{parsed.path}"
        self.query_path = Path(query)
        self.scheme = parsed.scheme

    async def inventory(self, cache: IOCacheStorageInterface):
        """From this file, try to find as much existence and modification date
        information as possible. Only retrieve that information that comes for free
        given the current object.
        """
        key = self.cache_key()

        if key in cache.exists_in_storage:
            return

        if not self.exists():
            cache.exists_in_storage[key] = False
            return
        stat = self._stat()
        if self.query_path.is_symlink():
            # get symlink stat
            lstat = self._stat(follow_symlinks=False)
        else:
            lstat = stat
        cache.mtime[key] = Mtime(storage=self._stat_to_mtime(lstat))
        cache.size[key] = stat.st_size
        cache.exists_in_storage[key] = True

    def get_inventory_parent(self) -> Optional[str]:
        """Return the parent directory of this object."""
        # this is optional and can be left as is
        return None

    def local_suffix(self) -> str:
        """Return a unique suffix for the local path, determined from self.query."""
        parsed = urlparse(self.query)
        return f"{parsed.netloc}{parsed.path}"


    def cleanup(self):
        """Perform local cleanup of any remainders of the storage object."""
        # self.local_path() should not be removed, as this is taken care of by
        # Snakemake.
        pass

    # Fallible methods should implement some retry logic.
    # The easiest way to do this (but not the only one) is to use the retry_decorator
    # provided by snakemake-interface-storage-plugins.
    @retry_decorator
    def exists(self) -> bool:
        # return True if the object exists
        return self.query_path.exists()

    @retry_decorator
    def mtime(self) -> float:
        # return the modification time
        return self._stat_to_mtime(self._stat(follow_symlinks=False))


    @retry_decorator
    def size(self) -> int:
        # return the size in bytes
        return self._stat().st_size

    @retry_decorator
    def retrieve_object(self):
        """Rsync object to local storage path if it does not exist."""
        # Ensure that the object is accessible locally under self.local_path()
        print("Trying to retrieve")
        print(self.scheme)
        cmd = sysrsync.get_rsync_command(
            str(self.query_path), str(self.local_path()), options=["-av"]
        )
        print(cmd)
        self._run_cmd(cmd)


    # The following to methods are only required if the class inherits from
    # StorageObjectReadWrite.

    @retry_decorator
    def store_object(self):
        # Ensure that the object is stored at the location specified by
        # self.local_path().
        ...

    @retry_decorator
    def remove(self):
        # Remove the object from the storage.
        ...

    def _run_cmd(self, cmd: list[str]):
        try:
            subprocess.run(
                cmd,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
        except subprocess.CalledProcessError as e:
            raise WorkflowError(e.stdout.decode())
        
    # The following to methods are only required if the class inherits from
    # StorageObjectGlob.

    @retry_decorator
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query."""
        # This is used by glob_wildcards() to find matches for wildcards in the query.
        # The method has to return concretized queries without any remaining wildcards.
        # Use snakemake_executor_plugins.io.get_constant_prefix(self.query) to get the
        # prefix of the query before the first wildcard.
        prefix = Path(get_constant_prefix(self.query))
        if prefix.is_dir():
            return map(str, prefix.rglob("*"))
        else:
            return (prefix,)

    def _stat(self, follow_symlinks: bool = True):
        # We don't want the cached variant (Path.stat), as we cache ourselves in
        # inventory and afterwards the information may change.
        return os.stat(self.query_path, follow_symlinks=follow_symlinks)

    @property
    def _timestamp_path(self):
        return self.query_path / ".snakemake_timestamp"

    def _stat_to_mtime(self, stat):
        if self.query_path.is_dir():
            # use the timestamp file if possible
            timestamp = self._timestamp_path
            if timestamp.exists():
                return os.stat(timestamp, follow_symlinks=False).st_mtime
        return stat.st_mtime
        
