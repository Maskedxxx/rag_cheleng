import sys
import unittest
from unittest.mock import patch
from run_pdf_preprocessor import main

class TestRagPipelineMain(unittest.TestCase):
    @patch('run_pdf_preprocessor.run_command', return_value=True)
    def test_main_success(self, mock_run_command):
        """
        Проверяем, что при корректных параметрах функция main() возвращает 0,
        что означает успешное выполнение пайплайна.
        """
        test_args = [
            "run_pdf_preprocessor.py",           # имя скрипта
            "--api_key", "dummy_openai_key",
            "--anthropic_api_key", "dummy_anthropic_key",
            "--output_dir", "/tmp/test_output",
            "--start_step", "1"
        ]
        with patch.object(sys, 'argv', test_args):
            retcode = main()
        self.assertEqual(retcode, 0)

if __name__ == "__main__":
    unittest.main()
