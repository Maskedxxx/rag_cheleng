import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path
import glob

# Импортируем тестируемые функции из вашего модуля (например, файл называется analysis_module.py)
from llm_img_tbl_job import (
    analyze_image_async,
    analyze_table_async,
    process_ocr_data_async,
)

# ------------------------------------------------------------------------------
# Создаем фиктивный клиент, имитирующий ответы API Anthropic.
# ------------------------------------------------------------------------------
class DummyClientMessages:
    async def create(self, **kwargs):
        # Всегда возвращаем фиктивный результат с текстом "dummy_analysis"
        class DummyContent:
            def __init__(self, text):
                self.text = text
        class DummyResponse:
            def __init__(self):
                self.content = [DummyContent("dummy_analysis")]
        return DummyResponse()

class DummyClient:
    def __init__(self):
        self.messages = DummyClientMessages()

# ------------------------------------------------------------------------------
# Тесты для асинхронных функций модуля
# ------------------------------------------------------------------------------
class TestAnalysisModule(unittest.IsolatedAsyncioTestCase):
    async def test_analyze_image_async_empty(self):
        """
        Тестируем analyze_image_async с пустым изображением.
        Ожидаем, что функция вернет (page_number, element_index, None)
        """
        dummy_client = DummyClient()
        result = await analyze_image_async(dummy_client, "1", 0, "")
        self.assertEqual(result, ("1", 0, None))
    
    async def test_analyze_image_async_valid(self):
        """
        Тестируем analyze_image_async с непустым base64-изображением.
        Ожидаем, что функция вернет результат анализа 'dummy_analysis'.
        """
        dummy_client = DummyClient()
        result = await analyze_image_async(dummy_client, "1", 0, "dummy_base64")
        self.assertEqual(result, ("1", 0, "dummy_analysis"))
    
    async def test_analyze_table_async_empty(self):
        """
        Тестируем analyze_table_async с пустым HTML.
        Ожидаем, что функция вернет (page_number, element_index, None)
        """
        dummy_client = DummyClient()
        result = await analyze_table_async(dummy_client, "2", 1, "")
        self.assertEqual(result, ("2", 1, None))
    
    async def test_analyze_table_async_valid(self):
        """
        Тестируем analyze_table_async с корректным HTML.
        Ожидаем, что функция вернет 'dummy_analysis'.
        """
        dummy_client = DummyClient()
        result = await analyze_table_async(dummy_client, "2", 1, "<table>dummy</table>")
        self.assertEqual(result, ("2", 1, "dummy_analysis"))
    
    async def test_process_ocr_data_async(self):
        """
        Тестируем process_ocr_data_async:
         - Создаем временную папку с одним тестовым OCR JSON-файлом.
         - Файл содержит одну страницу с двумя элементами:
           1) Изображение с данными base64, для которого должен добавиться ключ 'vision_analysis'
           2) Таблица с HTML, для которой должен добавиться ключ 'table_analysis'
         - Запускаем функцию и проверяем, что:
           • Для файла создается файл с анализом (_analyzed.json).
           • Данные в файле обновлены (добавлены ключи 'vision_analysis' и 'table_analysis').
           • Общий результат содержит ключ, сформированный заменой "_ocr.json" на ".pdf".
        """
        # Создаем временные папки для OCR и результатов анализа
        with tempfile.TemporaryDirectory() as temp_dir:
            ocr_folder = Path(temp_dir) / "ocr_data"
            output_folder = Path(temp_dir) / "analyzed_data"
            ocr_folder.mkdir(parents=True, exist_ok=True)
            output_folder.mkdir(parents=True, exist_ok=True)
            
            # Формируем тестовые OCR-данные: страница "1" с 2 элементами:
            #  - Первый элемент: изображение с base64-данными
            #  - Второй элемент: таблица с HTML
            dummy_ocr = {
                "1": [
                    {"category": "Image", "image_base64": "dummy_base64"},
                    {"category": "Table", "text_as_html": "<table>dummy</table>"}
                ]
            }
            ocr_file_path = ocr_folder / "dummy_ocr.json"
            with open(ocr_file_path, "w", encoding="utf-8") as f:
                json.dump(dummy_ocr, f, ensure_ascii=False, indent=4)
            
            # Используем фиктивного клиента
            dummy_client = DummyClient()
            
            # Запускаем асинхронную обработку OCR данных
            results = await process_ocr_data_async(dummy_client, str(ocr_folder), str(output_folder))
            
            # Проверяем, что создан файл с анализом
            analyzed_file = output_folder / "dummy_ocr_analyzed.json"
            self.assertTrue(analyzed_file.exists())
            
            # Загружаем обновленные данные и проверяем наличие ключей анализа
            with open(analyzed_file, "r", encoding="utf-8") as f:
                analyzed_data = json.load(f)
            
            # Для страницы "1":
            # - Первый элемент должен содержать ключ "vision_analysis" со значением "dummy_analysis"
            # - Второй элемент должен содержать ключ "table_analysis" со значением "dummy_analysis"
            self.assertIn("1", analyzed_data)
            page_elements = analyzed_data["1"]
            self.assertIn("vision_analysis", page_elements[0])
            self.assertEqual(page_elements[0]["vision_analysis"], "dummy_analysis")
            self.assertIn("table_analysis", page_elements[1])
            self.assertEqual(page_elements[1]["table_analysis"], "dummy_analysis")
            
            # Проверяем, что общий результат содержит ключ с именем PDF, которое формируется из OCR-файла.
            # В модуле имя PDF получается заменой "_ocr.json" на ".pdf", то есть "dummy_ocr.json" -> "dummy.pdf"
            self.assertIn("dummy.pdf", results)

# ------------------------------------------------------------------------------
# Запуск тестов
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    unittest.main()
