import unittest
from unittest import mock
from pipel.pipelines.pipeline import Pipelines


class TestPipeline(unittest.TestCase):

    def test_path_dir(self):
        pipe = Pipelines("example")
        self.assertEqual(f"{pipe.task_dir}/example/",pipe._pathPipelines())
    
    def test_path_file(self):
        pipe = Pipelines("example")
        self.assertEqual(f"{pipe.task_dir}/example/__init__.py", pipe._pathPipelines("__init__.py"))
    
    @mock.patch('pipel.pipelines.pipeline.os.path.exists')
    def test_check_dir(self, listdir):
        pipe = Pipelines("example")

        # Test exist dir
        listdir.return_value = True
        self.assertTrue(pipe._checkDirPipelines())

        # Test not exists dir
        listdir.return_value = False
        self.assertFalse(pipe._checkDirPipelines())

