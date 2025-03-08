import unittest
import json
from unittest.mock import patch, mock_open, MagicMock
from rag_pipeline.rag_question_analysis_pipeline import QuestionAnalysisPipeline, analyze_question  # Укажите правильный импорт


class TestQuestionAnalysisPipeline(unittest.TestCase):
    def setUp(self):
        """
        Подготавливаем тестовые данные: компании с вопросами.
        """
        self.companies_json = json.dumps({
            "abc123": {
                "company_name": "Company A",
                "questions": [{"text": "What is the financial performance of Company A?"}]
            }
        })
        self.output_path = "results/test_output.json"

    @patch("builtins.open", new_callable=mock_open, read_data="")
    @patch("json.load")
    def test_load_companies(self, mock_json_load, mock_open_file):
        """
        Тестирует загрузку данных компаний.
        """
        mock_json_load.return_value = json.loads(self.companies_json)

        pipeline = QuestionAnalysisPipeline("dummy_companies.json", self.output_path)
        companies = pipeline.load_companies()

        self.assertIn("abc123", companies)
        self.assertEqual(companies["abc123"]["company_name"], "Company A")

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.dump")
    def test_save_results(self, mock_json_dump, mock_open_file):
        """
        Тестирует сохранение результатов анализа.
        """
        pipeline = QuestionAnalysisPipeline("dummy_companies.json", self.output_path)
        test_data = {"abc123": {"company_name": "Company A", "questions": []}}

        pipeline.save_results(test_data)

        mock_open_file.assert_called_with(self.output_path, 'w')  # Проверяем, что файл открыт на запись
        mock_json_dump.assert_called_with(test_data, mock_open_file(), indent=2, ensure_ascii=False)  # Проверяем запись

    @patch("rag_pipeline.rag_question_analysis_pipeline.analyze_question")  # Подмена вызова LLM
    def test_process_questions(self, mock_analyze_question):
        """
        Тестирует обработку вопросов для компаний.
        """
        mock_analyze_question.return_value = MagicMock(
            metadata_category="financial_performance",
            currency="USD",
            search_locations=["Financial Report"]
        )

        pipeline = QuestionAnalysisPipeline("dummy_companies.json", self.output_path)
        companies = json.loads(self.companies_json)
        updated_companies = pipeline.process_questions(companies)

        self.assertEqual(updated_companies["abc123"]["questions"][0]["metadata_category"], "financial_performance")
        self.assertEqual(updated_companies["abc123"]["questions"][0]["currency"], "USD")
        self.assertEqual(updated_companies["abc123"]["questions"][0]["search_locations"], ["Financial Report"])

    @patch("openai.OpenAI")
    def test_analyze_question(self, mock_openai):
        """
        Тестирует функцию analyze_question, проверяя, что API отвечает 200 OK.
        """
        mock_client = MagicMock()
        mock_response = MagicMock()
        
        # Мокаем успешный HTTP-ответ (код 200)
        mock_response.choices = [MagicMock()]
        mock_client.beta.chat.completions.parse.return_value = mock_response
        mock_openai.return_value = mock_client

        try:
            analyze_question("What is the financial performance of Company A?")
            status_code = 200  # Если не было исключений, считаем, что статус 200
        except Exception:
            status_code = 500  # Если выбросило исключение, считаем, что ошибка

        self.assertEqual(status_code, 200)  # Проверяем, что вызов API завершился успешно

    @patch("builtins.open", new_callable=mock_open, read_data="")
    @patch("json.load")
    @patch("rag_pipeline.rag_question_analysis_pipeline.QuestionAnalysisPipeline.process_questions", return_value={})
    @patch("rag_pipeline.rag_question_analysis_pipeline.QuestionAnalysisPipeline.save_results")
    def test_run_pipeline(self, mock_save_results, mock_process_questions, mock_json_load, mock_open_file):
        """
        Тестирует полный запуск конвейера.
        """
        mock_json_load.return_value = json.loads(self.companies_json)

        pipeline = QuestionAnalysisPipeline("dummy_companies.json", self.output_path)
        pipeline.run()

        mock_process_questions.assert_called()
        mock_save_results.assert_called()

if __name__ == "__main__":
    unittest.main()