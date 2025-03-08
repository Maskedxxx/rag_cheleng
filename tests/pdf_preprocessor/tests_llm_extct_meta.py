import os
import json
import tempfile
import unittest
from pathlib import Path

# Импортируем тестируемые функции из вашего модуля
from pdf_preprocessor.llm_extct_meta import (
    prepare_context_for_llm,
    prepare_batch_file,
    submit_batch_job,
    check_batch_status,
)

# ------------------------------------------------------------------------------
# Dummy-классы для имитации работы клиента OpenAI
# ------------------------------------------------------------------------------
class DummyFiles:
    def create(self, file, purpose):
        # Имитация загрузки файла в OpenAI, возвращает объект с dummy-идентификатором
        class DummyFile:
            id = "dummy_file_id"
        return DummyFile()

    def content(self, file_id):
        # Имитация получения содержимого файла с результатами пакетного задания.
        # Здесь возвращается одна строка в формате JSONL.
        class DummyFileResponse:
            text = '{"custom_id": "dummy.pdf__page-1", "response": {"choices": [{"message": {"content": "{\\"type\\": \\"dummy\\", \\"entity\\": {\\"documents\\": []}}"} }]}}'
        return DummyFileResponse()

class DummyBatch:
    def __init__(self, status, output_file_id=None):
        self.status = status
        self.output_file_id = output_file_id

class DummyBatches:
    def create(self, input_file_id, endpoint, completion_window, metadata):
        # Имитация создания пакетного задания, возвращает dummy-идентификатор задания
        class DummyBatchCreated:
            id = "dummy_batch_id"
        return DummyBatchCreated()

    def retrieve(self, batch_id):
        # Всегда возвращаем задание со статусом "completed" и dummy-идентификатором выходного файла
        return DummyBatch("completed", output_file_id="dummy_output_file_id")

class DummyClient:
    def __init__(self):
        self.files = DummyFiles()
        self.batches = DummyBatches()

# ------------------------------------------------------------------------------
# Тесты для функций модуля
# ------------------------------------------------------------------------------
class TestDocumentProcessor(unittest.TestCase):
    def test_prepare_context_for_llm(self):
        """
        Тест функции prepare_context_for_llm:
        - Передается словарь с данными по страницам PDF.
        - Для страницы с текстовым элементом ожидается, что контекст будет содержать номер страницы и текст.
        - Для страницы с таблицей ожидается, что контекст будет содержать пометку 'Table:' и результат анализа таблицы.
        """
        pdf_data = {
            "1": [{"category": "Text", "content": "Test content"}],
            "2": [{"category": "Table", "table_analysis": "Table analysis result"}]
        }
        contexts = prepare_context_for_llm(pdf_data)
        self.assertIn("1", contexts)
        self.assertIn("2", contexts)
        self.assertIn("Test content", contexts["1"])
        self.assertIn("Table: Table analysis result", contexts["2"])

    def test_prepare_batch_file(self):
        """
        Тест функции prepare_batch_file:
        - Передается dummy-данные для одного PDF.
        - Функция должна создать batch‑файл в формате JSONL, где для каждой страницы сформирован запрос.
        - Также возвращается метадата пакета.
        """
        analyzed_data = {
            "dummy.pdf": {
                "1": [{"category": "Text", "content": "Page 1 content"}],
                "2": [{"category": "Text", "content": "Page 2 content"}]
            }
        }
        system_prompt = "Extract metadata."
        model = "dummy-model"
        batch_file_path, batch_metadata = prepare_batch_file(analyzed_data, system_prompt, model)
        
        # Проверяем, что файл создан и содержит две строки (для двух страниц)
        with open(batch_file_path, "r", encoding="utf-8") as f:
            lines = f.read().strip().split("\n")
        self.assertEqual(len(lines), 2)
        # Проверяем, что метаданные содержат информацию по PDF
        self.assertIn("dummy.pdf", batch_metadata)
        
        # Удаляем временный файл
        os.remove(batch_file_path)

    def test_submit_batch_job(self):
        """
        Тест функции submit_batch_job:
        - Используем DummyClient, который имитирует методы files.create и batches.create.
        - Проверяем, что функция возвращает dummy-идентификатор задания.
        - Также проверяем, что файл с информацией о задании (batch_info.json) создан в state_dir.
        """
        dummy_client = DummyClient()
        with tempfile.TemporaryDirectory() as state_dir:
            # Создаем временный batch-файл с dummy содержимым
            batch_file_path = os.path.join(state_dir, "dummy_batch.jsonl")
            with open(batch_file_path, "w", encoding="utf-8") as f:
                f.write('{"dummy": "data"}')
            
            metadata = {"dummy": "meta"}
            batch_id = submit_batch_job(dummy_client, batch_file_path, state_dir, metadata)
            self.assertEqual(batch_id, "dummy_batch_id")
            
            # Проверяем, что файл batch_info.json создан
            batch_info_path = os.path.join(state_dir, "batch_info.json")
            self.assertTrue(os.path.exists(batch_info_path))
            # Очищаем временный файл
            os.remove(batch_info_path)
            os.remove(batch_file_path)

    def test_check_batch_status(self):
        """
        Тест функции check_batch_status:
        - Создаем временные директории для state и output.
        - Создаем dummy batch_info.json с dummy_batch_id.
        - DummyClient возвращает задание со статусом "completed" и dummy output_file_id.
        - Проверяем, что функция возвращает "completed_and_processed" и что в output_dir создан выходной файл.
        """
        dummy_client = DummyClient()
        with tempfile.TemporaryDirectory() as state_dir, tempfile.TemporaryDirectory() as output_dir:
            # Создаем dummy batch_info.json
            batch_info = {"batch_id": "dummy_batch_id"}
            batch_info_path = os.path.join(state_dir, "batch_info.json")
            with open(batch_info_path, "w", encoding="utf-8") as f:
                json.dump(batch_info, f)
            
            status = check_batch_status(dummy_client, state_dir, output_dir)
            self.assertEqual(status, "completed_and_processed")
            
            # Проверяем, что результат для PDF "dummy.pdf" создан в output_dir
            output_file = os.path.join(output_dir, "dummy_metadata.json")
            self.assertTrue(os.path.exists(output_file))
            # Читаем и проверяем содержимое
            with open(output_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            # В нашем dummy-ответе содержимое может быть пустым, проверяем наличие ключа для страницы "1"
            self.assertIn("1", data)
            
            # Файл batch_info.json должен быть удален после успешной обработки
            self.assertFalse(os.path.exists(batch_info_path))

# ------------------------------------------------------------------------------
# Запуск тестов
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    unittest.main()
