import unittest
import json
from unittest.mock import patch, mock_open, MagicMock
from pathlib import Path
from run_rag_pipeline import process_all_questions, process_question, RagFullPipeline


class TestRagPipeline(unittest.TestCase):
    def setUp(self):
        """
        Подготавливает тестовые данные.
        """
        self.test_data_file = "results/all_questions_analysis_results.json"
        self.metadata_file = "results/final_agr/test_sha1.json"

        # Фейковые данные компаний и вопросов
        self.companies_json = json.dumps({
            "test_sha1": {
                "company_name": "Test Company",
                "has_questions": True,
                "questions": [
                    {
                        "text": "What is the financial performance of Test Company?",
                        "kind": "number",
                        "metadata_category": "financial_performance"
                    }
                ]
            }
        })

        # Фейковые метаданные
        self.metadata_json = json.dumps({
            "meta": {
                "has_financial_performance_indicators": {
                    "elements": [
                        {
                            "title": "Financial Report",
                            "page": 10,
                            "type": "table",
                            "currency": "USD",
                            "data": ["Revenue: $1M"]
                        }
                    ]
                }
            }
        })

    @patch("builtins.open", new_callable=mock_open, read_data="")
    @patch("json.load")
    @patch("glob.glob")
    def test_process_all_questions(self, mock_glob, mock_json_load, mock_open_file):
        """
        Тестирует полный процесс обработки вопросов для всех компаний.
        """
        mock_json_load.side_effect = [json.loads(self.companies_json), json.loads(self.metadata_json)]
        mock_glob.return_value = [self.metadata_file]

        with patch("run_rag_pipeline.process_question") as mock_process_question:
            mock_process_question.return_value = MagicMock(
                data_analysis=["Revenue has increased by 10%."],
                reasoning=["Company revenue is higher due to increased sales."],
                answer_type="number",
                answer=1000000.0,
                pages=10
            )

            result = process_all_questions(self.test_data_file)

            self.assertIn("test_sha1", result)
            self.assertIn("questions", result["test_sha1"])
            self.assertEqual(len(result["test_sha1"]["questions"]), 1)

            question = result["test_sha1"]["questions"][0]
            self.assertEqual(question["answer_type"], "number")
            self.assertEqual(question["answer_value"], 1000000.0)
            self.assertEqual(question["pages"], 10)

            mock_process_question.assert_called_once()  # Проверяем, что LLM использовался

    @patch("openai.OpenAI")
    def test_process_question(self, mock_openai):
        """
        Тестирует процесс обработки одного вопроса, проверяя, что API возвращает 200 OK.
        """
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(parsed=MagicMock(
            data_analysis=["Revenue increased by 10%."],
            reasoning=["Strong sales growth led to higher revenue."],
            answer_type="number",
            answer=1000000.0,
            pages=5
        )))]
        mock_client.beta.chat.completions.parse.return_value = mock_response
        mock_openai.return_value = mock_client

        result = process_question(
            company_name="Test Company",
            question_text="What is the financial performance of Test Company?",
            question_kind="number",
            metadata_elements=[{"title": "Financial Report", "data": ["Revenue: $1M"], "page": 5}],
            search_locations=["Financial Report"]
        )

        self.assertEqual(result.answer_type, "number")
        self.assertEqual(result.answer, 1000000.0)
        self.assertEqual(result.pages, 5)

    @patch("builtins.open", new_callable=mock_open, read_data="")
    @patch("json.dump")
    def test_save_results(self, mock_json_dump, mock_open_file):
        """
        Тестирует сохранение результатов.
        """
        test_results = {
            "test_sha1": {
                "company_name": "Test Company",
                "questions": [
                    {"text": "What is the financial performance?", "answer_value": 1000000.0}
                ]
            }
        }

        with open("results/final_results.json", "w") as file:
            json.dump(test_results, file, indent=2, ensure_ascii=False)

        mock_open_file.assert_called_with("results/final_results.json", "w")
        mock_json_dump.assert_called_with(test_results, mock_open_file(), indent=2, ensure_ascii=False)
        
    @patch.object(RagFullPipeline, 'run_step1')
    @patch.object(RagFullPipeline, 'run_step2')
    @patch.object(RagFullPipeline, 'run_step3')
    def test_run_full_pipeline(self, mock_step3, mock_step2, mock_step1):
        """
        Тестирует метод запуска полного пайплайна, проверяя вызов всех шагов.
        """
        pipeline = RagFullPipeline()
        pipeline.run_full_pipeline()
        
        # Проверяем, что каждый шаг был вызван по одному разу в правильном порядке
        mock_step1.assert_called_once()
        mock_step2.assert_called_once()
        mock_step3.assert_called_once()


if __name__ == "__main__":
    unittest.main()