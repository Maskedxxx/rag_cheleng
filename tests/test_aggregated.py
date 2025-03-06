import os
import json
import tempfile
import shutil
import unittest
from pathlib import Path

# Импортируем функции из нашего модуля агрегации (предположим, файл называется aggregation.py)
from aggregated import (
    find_target_object,
    create_empty_template,
    extract_metadata_by_type,
    aggregate_single_pdf,
    reorganize_structure,
    process_pdfs
)

# Dummy данные для тестирования

# Dummy целевые данные (target data)
DUMMY_TARGET_DATA = {
    "dummy.pdf": {
        "sha1": "dummy.pdf",
        "meta": {"company_name": "Dummy Company"}
    },
    "other.pdf": {
        "sha1": "other",
        "meta": {"company_name": "Other Company"}
    }
}

# Dummy результаты анализа для одного PDF (с двумя страницами с разными типами метаданных)
DUMMY_ANALYSIS_RESULTS = {
    "1": {
        "metadata": {
            "type": "merger_acquisition",
            "entity": {
                "documents": [
                    {
                        "page": 1,
                        "title": "Merger Announcement",
                        "data": [{"key": "Deal", "value": "Acquisition of X"}],
                        "currency": "USD"
                    }
                ]
            }
        }
    },
    "2": {
        "metadata": {
            "type": "leadership_change",
            "entity": {
                "documents": [
                    {
                        "page": 2,
                        "title": "New CEO",
                        "data": [{"key": "CEO", "value": "John Doe"}],
                        "currency": "N/A"
                    }
                ]
            }
        }
    }
}

# Dummy агрегированный результат для реорганизации
DUMMY_AGGREGATED = {
    "letters": "Some letters",
    "pages": {"1": "Page 1 content"},
    "meta": {
        "end_of_period": "2023-12-31",
        "company_name": "Dummy Company",
        "major_industry": "Tech",
        # Допустим, в исходном агрегированном результате уже стоит True для данного поля
        "mentions_recent_mergers_and_acquisitions": True  
    },
    "currency": "USD",
    "sha1": "dummy.pdf",
    "extracted_elements": [
        {
            "type": "merger_acquisition",
            "page": 1,
            "title": "Merger Announcement",
            "data": [{"key": "Deal", "value": "Acquisition of X"}],
            "currency": "USD"
        }
    ]
}

class TestAggregationModule(unittest.TestCase):
    def test_find_target_object(self):
        """
        Проверяем, что функция находит целевой объект по имени PDF или по совпадению company_name.
        """
        # По прямому ключу
        obj = find_target_object(DUMMY_TARGET_DATA, "dummy.pdf", sha1="dummy.pdf")
        self.assertIsNotNone(obj)
        self.assertEqual(obj.get("meta", {}).get("company_name"), "Dummy Company")
        
        # По совпадению company_name (имя PDF без расширения входит в имя компании)
        obj2 = find_target_object(DUMMY_TARGET_DATA, "other.pdf")
        self.assertIsNotNone(obj2)
        self.assertEqual(obj2.get("meta", {}).get("company_name"), "Other Company")
        
        # Если ничего не найдено
        obj3 = find_target_object(DUMMY_TARGET_DATA, "nonexistent.pdf")
        self.assertIsNone(obj3)

    def test_create_empty_template(self):
        """
        Проверяем, что шаблон создается с полями meta, сброшенными в False.
        """
        target_obj = DUMMY_TARGET_DATA["dummy.pdf"]
        template = create_empty_template(target_obj)
        self.assertIn("meta", template)
        # Проверяем, что для каждого поля из TYPE_TO_FIELD_MAP значение False
        for field in template["meta"]:
            # Если поле взято из TYPE_TO_FIELD_MAP, оно должно быть False
            if field in [v for v in template["meta"]]:
                self.assertFalse(template["meta"][field])

    def test_extract_metadata_by_type(self):
        """
        Проверяем, что метаданные группируются по типу.
        """
        grouped = extract_metadata_by_type(DUMMY_ANALYSIS_RESULTS)
        self.assertIn("merger_acquisition", grouped)
        self.assertIn("leadership_change", grouped)
        # Для merger_acquisition должен быть один элемент с title "Merger Announcement"
        self.assertEqual(grouped["merger_acquisition"][0]["title"], "Merger Announcement")
        # Для leadership_change должен быть один элемент с page == 2
        self.assertEqual(grouped["leadership_change"][0]["page"], 2)

    def test_aggregate_single_pdf(self):
        """
        Создаем временный файл с dummy анализом и проверяем агрегацию для одного PDF.
        """
        with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as temp_file:
            json.dump(DUMMY_ANALYSIS_RESULTS, temp_file)
            temp_file_path = temp_file.name
        try:
            result = aggregate_single_pdf("dummy.pdf", temp_file_path, DUMMY_TARGET_DATA)
            self.assertIsNotNone(result)
            # Проверяем, что для merger_acquisition (соответствующее поле) установлено True
            field = "mentions_recent_mergers_and_acquisitions"
            self.assertTrue(result["meta"].get(field))
            # Проверяем, что извлеченные элементы содержат документ с заголовком "Merger Announcement"
            self.assertTrue(any(elem.get("title") == "Merger Announcement" for elem in result.get("extracted_elements", [])))
        finally:
            os.remove(temp_file_path)

    def test_reorganize_structure(self):
        """
        Проверяем, что функция reorganize_structure создает итоговую структуру с нужными полями и группировкой.
        """
        reorganized = reorganize_structure(DUMMY_AGGREGATED)
        self.assertIn("letters", reorganized)
        self.assertIn("pages", reorganized)
        self.assertIn("meta", reorganized)
        self.assertIn("currency", reorganized)
        self.assertIn("sha1", reorganized)
        # Проверяем, что для поля merger_acquisition в meta есть вложенный словарь с ключами value и elements
        field = "mentions_recent_mergers_and_acquisitions"
        self.assertIn(field, reorganized["meta"])
        self.assertIsInstance(reorganized["meta"][field], dict)
        self.assertIn("value", reorganized["meta"][field])
        self.assertIn("elements", reorganized["meta"][field])
        self.assertTrue(reorganized["meta"][field]["value"])
        self.assertGreaterEqual(len(reorganized["meta"][field]["elements"]), 1)

    def test_process_pdfs(self):
        """
        Тест полного процесса process_pdfs:
         – Создаем временные директории для целевых данных, метаданных, промежуточных и финальных результатов.
         – Записываем dummy target data и dummy метаданные.
         – Вызываем process_pdfs и проверяем, что созданы промежуточные и финальные файлы.
        """
        # Создаем временные директории
        with tempfile.TemporaryDirectory() as temp_dir:
            target_file = os.path.join(temp_dir, "target.json")
            metadata_dir = os.path.join(temp_dir, "metadata")
            output_dir = os.path.join(temp_dir, "pre_agr")
            final_dir = os.path.join(temp_dir, "final_agr")
            os.makedirs(metadata_dir, exist_ok=True)
            # Записываем dummy target data
            with open(target_file, "w", encoding="utf-8") as f:
                json.dump(DUMMY_TARGET_DATA, f)
            # Создаем dummy метаданные для PDF "dummy.pdf"
            dummy_metadata_file = os.path.join(metadata_dir, "dummy_metadata.json")
            with open(dummy_metadata_file, "w", encoding="utf-8") as f:
                json.dump(DUMMY_ANALYSIS_RESULTS, f)
            
            # Вызываем функцию обработки PDF
            process_pdfs(target_file, metadata_dir, output_dir, final_dir)
            
            # Проверяем, что создан промежуточный агрегированный файл для dummy.pdf
            aggregated_file = os.path.join(output_dir, "dummy_aggregated.json")
            self.assertTrue(os.path.exists(aggregated_file))
            # Проверяем, что создан общий файл всех агрегированных результатов
            all_agg = os.path.join(output_dir, "all_aggregated_results.json")
            self.assertTrue(os.path.exists(all_agg))
            # Проверяем, что создан финальный результат для dummy.pdf
            final_file = os.path.join(final_dir, "dummy_final.json")
            self.assertTrue(os.path.exists(final_file))
            # Проверяем, что создан общий финальный файл
            all_final = os.path.join(final_dir, "all_final_results.json")
            self.assertTrue(os.path.exists(all_final))

if __name__ == "__main__":
    unittest.main()
