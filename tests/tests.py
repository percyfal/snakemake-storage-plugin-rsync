import os
from typing import Optional, Type
import uuid
from snakemake_interface_storage_plugins.tests import TestStorageBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_storage_plugin_rsync import StorageProvider


class TestStorage(TestStorageBase):
    __test__ = True
    retrieve_only = False  # set to True if the storage is read-only
    store_only = False  # set to True if the storage is write-only
    delete = True  # set to False if the storage does not support deletion

    def get_query(self, tmp_path) -> str:
        parent = f"{tmp_path}/storage/test"
        os.makedirs(parent, exist_ok=True)
        with open(f"{parent}/test.txt", "w") as f:
            f.write("test")
        return f"rsync://{parent}/test.txt"

    def get_query_not_existing(self, tmp_path) -> str:
        return f"rsync://{tmp_path}/storage/test/{uuid.uuid4().hex}"

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        return StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        return None
