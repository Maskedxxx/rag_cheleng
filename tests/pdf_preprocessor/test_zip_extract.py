# tests/test_zip_extract.py

import os
import json
import hashlib
import tempfile
import zipfile
import unittest
from pathlib import Path

# Импорт функций из вашего модуля
# Предположим, что код модуля сохранён в файле module.py
from pdf_preprocessor.zip_extract import (
    extract_sha1_from_filename,
    calculate_file_sha1,
    process_zip_file,
    find_files_in_dataset,
)

class TestRAGModule(unittest.TestCase):
    def test_extract_sha1_from_filename(self):
        valid_hash = "a" * 40
        # Файл с именем, состоящим только из SHA1
        self.assertEqual(extract_sha1_from_filename(f"{valid_hash}.pdf"), valid_hash)
        # Файл с именем SHA1_Company.pdf
        self.assertEqual(extract_sha1_from_filename(f"{valid_hash}_Company.pdf"), valid_hash)
        # Неверное имя файла
        self.assertIsNone(extract_sha1_from_filename("invalid.pdf"))

    def test_calculate_file_sha1(self):
        content = b"Test content"
        expected_sha1 = hashlib.sha1(content).hexdigest()
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name
        result = calculate_file_sha1(Path(tmp_path))
        os.remove(tmp_path)
        self.assertEqual(result, expected_sha1)

    def test_process_zip_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, "test.zip")
            with zipfile.ZipFile(zip_path, "w") as z:
                # Файл с валидным SHA1 в имени
                valid_hash = "a" * 40
                file1 = f"{valid_hash}.pdf"
                z.writestr(file1, b"PDF content 1")
                # Файл без SHA1 в имени, SHA1 будет вычислен по содержимому
                file2 = "file2.pdf"
                content2 = b"PDF content 2"
                z.writestr(file2, content2)
                expected_sha1 = hashlib.sha1(content2).hexdigest()
            result = process_zip_file(zip_path)
            self.assertEqual(len(result), 2)
            # Для первого файла имя компании будет совпадать с SHA1
            self.assertIn((valid_hash, valid_hash), result)
            # Для второго файла имя компании должно быть "file2"
            self.assertIn((expected_sha1, "file2"), result)

    def test_find_files_in_dataset(self):
        # Создаем временный dataset JSON с двумя записями
        dataset = {
            "doc1": {"sha1": "a" * 40, "data": "data1"},
            "doc2": {"sha1": "b" * 40, "data": "data2"}
        }
        with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as tmp:
            json.dump(dataset, tmp)
            tmp_path = tmp.name

        # Передаем один существующий SHA1 и один отсутствующий
        sha1_list = [( "a" * 40, "CompanyA"), ( "c" * 40, "CompanyC")]
        result = find_files_in_dataset(tmp_path, sha1_list)
        self.assertIn("doc1", result)
        self.assertNotIn("doc2", result)
        os.remove(tmp_path)

if __name__ == "__main__":
    unittest.main()
