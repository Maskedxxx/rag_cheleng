import unittest
import json
from unittest.mock import patch, mock_open
from rag_pipeline.rag_metadata_questions_pipeline import MetadataQuestionsPipeline  # Укажите правильный импорт

class TestMetadataQuestionsPipeline(unittest.TestCase):
    def setUp(self):
        """
        Подготавливаем тестовые данные: метаданные компаний и вопросы.
        """
        self.metadata_json = json.dumps({
            "file1": {"sha1": "abc123", "meta": {"company_name": "Company A"}},
            "file2": {"sha1": "def456", "meta": {"company_name": "Company B"}}
        })

        self.questions_json = json.dumps([
            {"text": "What does Company A do?", "kind": "general"},
            {"text": "Tell me about Company C.", "kind": "general"}
        ])

        self.output_path = "results/test_output.json"

    @patch("builtins.open", new_callable=mock_open, read_data="")
    @patch("json.load")
    def test_extract_company_info(self, mock_json_load, mock_open_file):
        """
        Тестирует извлечение информации о компаниях.
        """
        mock_json_load.return_value = json.loads(self.metadata_json)

        pipeline = MetadataQuestionsPipeline("dummy_metadata.json", "dummy_questions.json", self.output_path)
        companies = pipeline.extract_company_info("dummy_metadata.json")

        expected_result = {
            "abc123": {"company_name": "Company A"},
            "def456": {"company_name": "Company B"}
        }
        self.assertEqual(companies, expected_result)

    @patch("builtins.open", new_callable=mock_open, read_data="")
    @patch("json.load")
    def test_check_questions_coverage(self, mock_json_load, mock_open_file):
        """
        Тестирует сопоставление вопросов с компаниями.
        """
        mock_json_load.side_effect = [json.loads(self.metadata_json), json.loads(self.questions_json)]

        pipeline = MetadataQuestionsPipeline("dummy_metadata.json", "dummy_questions.json", self.output_path)
        all_questions, matched, unmatched = pipeline.check_questions_coverage()

        self.assertEqual(len(all_questions), 2)
        self.assertEqual(len(matched), 1)  # Один вопрос связан с Company A
        self.assertEqual(len(unmatched), 1)  # Один вопрос не связан ни с одной компанией

    @patch("builtins.open", new_callable=mock_open, read_data="")
    @patch("json.load")
    def test_add_questions_to_companies(self, mock_json_load, mock_open_file):
        """
        Тестирует добавление вопросов к компаниям.
        """
        mock_json_load.return_value = json.loads(self.questions_json)

        pipeline = MetadataQuestionsPipeline("dummy_metadata.json", "dummy_questions.json", self.output_path)
        companies_dict = {
            "abc123": {"company_name": "Company A"},
            "def456": {"company_name": "Company B"}
        }

        updated_companies = pipeline.add_questions_to_companies(companies_dict, "dummy_questions.json")

        self.assertIn("questions", updated_companies["abc123"])
        self.assertEqual(len(updated_companies["abc123"]["questions"]), 1)  # Один вопрос связан с Company A
        self.assertEqual(len(updated_companies["def456"]["questions"]), 0)  # Company B не имеет вопросов
        self.assertTrue(updated_companies["abc123"]["has_questions"])
        self.assertFalse(updated_companies["def456"]["has_questions"])

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.dump")
    def test_run_pipeline(self, mock_json_dump, mock_open_file):
        """
        Тестирует полный запуск пайплайна.
        """
        pipeline = MetadataQuestionsPipeline("dummy_metadata.json", "dummy_questions.json", self.output_path)
        with patch.object(pipeline, "extract_company_info", return_value={"abc123": {"company_name": "Company A"}}):
            with patch.object(pipeline, "add_questions_to_companies", return_value={"abc123": {
                "company_name": "Company A", "questions": [], "has_questions": False}}):
                pipeline.run()

        mock_open_file.assert_called_with(self.output_path, 'w')  # Проверяем, что файл открывался для записи
        mock_json_dump.assert_called()  # Проверяем, что результат был записан

if __name__ == "__main__":
    unittest.main()