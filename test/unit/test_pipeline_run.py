import unittest
from unittest import mock
from pipel.pipelines.pipeline_run import RunPipelines

class TestRunPipeline(unittest.TestCase):

    @mock.patch('pipel.pipelines.pipeline_run.os.path.exists')
    def test_check_file_exists_true(self, mock_os):
        mock_os.return_value = True
        run = RunPipelines("example")
        self.assertIsNone(run.checkExistsFilePipelines())

    @mock.patch('pipel.pipelines.pipeline_run.os.path.exists')
    def test_check_file_exists_false(self, mock_os):
        mock_os.return_value = False
        run = RunPipelines("example")
        self.assertRaises(ValueError, run.checkExistsFilePipelines)




