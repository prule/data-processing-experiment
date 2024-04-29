import urllib.request
import os
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class Files:
    src_base_path: str
    dest_base_path: str

    def __init__(self, src_base_path: str, dest_base_path: str):
        self.src_base_path = src_base_path
        self.dest_base_path = dest_base_path

    # def clean(self):
        # shutil.rmtree(self.dest_base_path)

    def copy_url(self, path: str):
        src = self.src_base_path + path
        dest = Path(self.dest_base_path + path)
        logger.info('Copying from ' + src + ' to ' + dest.absolute().__str__())

        os.makedirs(dest.parent, exist_ok=True)
        urllib.request.urlretrieve(
            src,
            dest.__str__()
        )
