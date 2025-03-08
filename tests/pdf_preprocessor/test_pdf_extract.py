import os
import json
import tempfile
import zipfile
import unittest
from pathlib import Path

# Импортируем функции из вашего модуля, предполагается, что он сохранён в pdf_module.py
from pdf_preprocessor.pdf_extract import process_image, process_table, process_pdf, process_pdfs_in_zip, save_data_to_json

# ---------------------------------------------------------------------------
# Создаём фиктивные (dummy) классы для эмуляции объектов, которые возвращает partition_pdf
# ---------------------------------------------------------------------------
class DummyMetadata:
    def __init__(self, page_number=None, image_base64=None, text_as_html=None):
        self.page_number = page_number
        self.image_base64 = image_base64
        self.text_as_html = text_as_html

class DummyElement:
    def __init__(self, category, text, metadata):
        self.category = category
        self.text = text
        self.metadata = metadata

# ---------------------------------------------------------------------------
# Фиктивная функция, которая вместо настоящей partition_pdf возвращает тестовые элементы.
# В данной реализации возвращаются:
# - Изображение с более чем 10 словами на странице 1,
# - Таблица с HTML представлением на странице 2,
# - Простой текстовый элемент на странице 1.
# ---------------------------------------------------------------------------
def dummy_partition_pdf(filename, strategy, extract_image_block_to_payload, extract_image_block_types, infer_table_structure):
    image_metadata = DummyMetadata(page_number=1, image_base64="dummy_base64_string")
    image_element = DummyElement("Image", "This image element contains more than ten words for testing purposes", image_metadata)
    
    table_metadata = DummyMetadata(page_number=2, text_as_html="<table>dummy html</table>")
    table_element = DummyElement("Table", "Table content", table_metadata)
    
    text_metadata = DummyMetadata(page_number=1)
    text_element = DummyElement("Text", "Just plain text content", text_metadata)
    
    return [image_element, table_element, text_element]

# ---------------------------------------------------------------------------
# Класс тестов для нашего модуля
# ---------------------------------------------------------------------------
class TestPDFModule(unittest.TestCase):
    def setUp(self):
        """
        Перед каждым тестом заменяем функцию partition_pdf на фиктивную.
        """
        import pdf_extract  # Импортируем модуль, где находится оригинальная partition_pdf
        self.original_partition_pdf = pdf_extract.partition_pdf
        pdf_extract.partition_pdf = dummy_partition_pdf
    
    def tearDown(self):
        """
        После каждого теста возвращаем оригинальную функцию.
        """
        import pdf_extract
        pdf_extract.partition_pdf = self.original_partition_pdf
    
    def test_process_image(self):
        """
        Тестирование функции process_image:
        - Проверяем, что при наличии >10 слов возвращается словарь с ключом image_base64.
        - При коротком тексте ключ image_base64 отсутствует.
        """
        metadata = DummyMetadata(page_number=1, image_base64="base64data")
        # Текст больше 10 слов
        long_text = "This image element contains more than ten words for testing purposes"
        element = DummyElement("Image", long_text, metadata)
        result = process_image(element)
        self.assertIsNotNone(result)
        self.assertEqual(result.get("category"), "Image")
        self.assertIn("image_base64", result)
        
        # Текст короче 10 слов
        short_text = "Short text"
        element_short = DummyElement("Image", short_text, metadata)
        result_short = process_image(element_short)
        self.assertIsNotNone(result_short)
        self.assertEqual(result_short.get("category"), "Image")
        self.assertNotIn("image_base64", result_short)
    
    def test_process_table(self):
        """
        Тестирование функции process_table:
        - Если у таблицы есть HTML-представление, функция должна вернуть словарь с ключом text_as_html.
        - Если HTML отсутствует, возвращается None.
        """
        metadata = DummyMetadata(page_number=2, text_as_html="<table>dummy</table>")
        element = DummyElement("Table", "Table content", metadata)
        result = process_table(element)
        self.assertIsNotNone(result)
        self.assertEqual(result.get("category"), "Table")
        self.assertIn("text_as_html", result)
        
        metadata_no_html = DummyMetadata(page_number=2, text_as_html=None)
        element_no_html = DummyElement("Table", "Table content", metadata_no_html)
        result_no_html = process_table(element_no_html)
        self.assertIsNone(result_no_html)
    
    def test_process_pdf(self):
        """
        Тестирование функции process_pdf с использованием dummy_partition_pdf.
        Ожидается, что элементы распределятся по страницам:
         - Страница 1 должна содержать два элемента (изображение и текст).
         - Страница 2 должна содержать один элемент (таблица).
        """
        pages_data, elements = process_pdf("dummy.pdf")
        self.assertIn(1, pages_data)
        self.assertIn(2, pages_data)
        self.assertEqual(len(pages_data[1]), 2)
        self.assertEqual(len(pages_data[2]), 1)
    
    def test_process_pdfs_in_zip(self):
        """
        Тестирование функции process_pdfs_in_zip:
        - Создаём временный ZIP-архив, содержащий один тестовый PDF-файл.
        - Проверяем, что функция возвращает обработанные данные для этого файла.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            # Создаём тестовый PDF-файл (его содержимое не важно, т.к. используется dummy_partition_pdf)
            pdf_filename = "test_dummy.pdf"
            pdf_filepath = Path(temp_dir) / pdf_filename
            with open(pdf_filepath, "w") as f:
                f.write("Dummy PDF content")
            
            # Создаём ZIP-архив с нашим PDF
            zip_path = Path(temp_dir) / "dummy.zip"
            with zipfile.ZipFile(zip_path, "w") as zipf:
                zipf.write(pdf_filepath, arcname=pdf_filename)
            
            # Указываем временную директорию для извлечения файлов
            extract_dir = Path(temp_dir) / "extracted"
            result = process_pdfs_in_zip(str(zip_path), str(extract_dir))
            self.assertIn(pdf_filename, result)
            self.assertIsInstance(result[pdf_filename], dict)
    
    def test_save_data_to_json(self):
        """
        Тестирование функции save_data_to_json:
        - Сохраняем тестовые данные в JSON-файл.
        - Проверяем, что содержимое файла соответствует ожидаемому.
        """
        test_data = {"key": "value", "number": 123}
        with tempfile.NamedTemporaryFile("r+", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            save_data_to_json(test_data, tmp_path)
            with open(tmp_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.assertEqual(data, test_data)
        finally:
            os.remove(tmp_path)

# Запуск тестов, если файл запускается как основной
if __name__ == "__main__":
    unittest.main()
