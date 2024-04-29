import os
import unittest

from core_prule.Files import Files


class FilesTest(unittest.TestCase):
    files = Files(
        'https://raw.githubusercontent.com/prule/data-processing-experiment-python/main/src/',
        './tmp/'
    )

    def test_copy(self):
        """
        File should be copied to the local file system
        """

        file = 'core_prule/Configuration.py'
        file_path = self.files.dest_base_path + file

        # delete the file if it exists
        if os.path.exists(file_path):
            os.remove(file_path)

        assert not os.path.exists(file_path)

        # perform
        self.files.copy_url(file)

        # verify
        assert os.path.exists(file_path)


if __name__ == '__main__':
    unittest.main()
