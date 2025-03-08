# pdf_preprocessor/zip_extract.py

import os
import json
import zipfile
import hashlib
import argparse
import tempfile
import logging
from pathlib import Path
from typing import Optional, List, Tuple, Dict

log_dir = Path("LOGS")
log_dir.mkdir(exist_ok=True)
log_results = Path("results")
log_results.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "zip_extract.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("zip_extract")


def extract_sha1_from_filename(filename: str) -> Optional[str]:
    base = os.path.basename(filename)
    name, _ = os.path.splitext(base)
    if len(name) == 40 and all(c in '0123456789abcdef' for c in name.lower()):
        return name.lower()
    parts = name.split('_')
    if parts and len(parts[0]) == 40 and all(c in '0123456789abcdef' for c in parts[0].lower()):
        return parts[0].lower()
    return None

def calculate_file_sha1(file_path: Path) -> str:
    sha1 = hashlib.sha1()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha1.update(chunk)
    return sha1.hexdigest()

def process_zip_file(zip_path: str) -> List[Tuple[str, str]]:
    result = []
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"{zip_path} не найден")
    with zipfile.ZipFile(zip_path, 'r') as z:
        for file in z.namelist():
            if file.endswith('/') or not file.lower().endswith('.pdf'):
                continue
            sha1 = extract_sha1_from_filename(file)
            if not sha1:
                with tempfile.TemporaryDirectory() as tmp:
                    z.extract(file, tmp)
                    file_path = Path(tmp) / file
                    sha1 = calculate_file_sha1(file_path)
            base = os.path.basename(file)
            name, _ = os.path.splitext(base)
            company = '_'.join(name.split('_')[1:]) if '_' in name else name
            result.append((sha1, company))
    return result

def find_files_in_dataset(dataset_path: str, sha1_list: List[Tuple[str, str]]) -> Dict:
    if not os.path.exists(dataset_path):
        raise FileNotFoundError(f"{dataset_path} не найден")
    with open(dataset_path, encoding='utf-8') as f:
        dataset = json.load(f)
    found = {}
    not_found = []
    for sha1, company in sha1_list:
        for key, data in dataset.items():
            if data.get('sha1') == sha1:
                found[key] = data
                break
        else:
            not_found.append((sha1, company))
    if not_found:
        logger.warning(f"Не найдены {len(not_found)} SHA1: {not_found}")
    return found

def main():
    parser = argparse.ArgumentParser(description="Обработка zip для RAG конвейера")
    parser.add_argument('--zip', default="data/<путь_к_zip_файлу>", help="Путь к zip-архиву с PDF")
    parser.add_argument('--dataset', default="data/dataset_v2.json", help="Путь к dataset_v2.json")
    parser.add_argument('--output', default="results/meta_result.json", help="Файл для сохранения результата")
    args = parser.parse_args()
    
    logger.info(f"Начало обработки архива: {args.zip}")
    sha1_list = process_zip_file(args.zip)
    logger.info(f"Найдено {len(sha1_list)} PDF файлов в архиве")
    
    logger.info(f"Поиск объектов в {args.dataset}")
    result = find_files_in_dataset(args.dataset, sha1_list)
    logger.info(f"Найдено {len(result)} объектов из {len(sha1_list)} PDF")
    
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logger.info(f"Результаты сохранены в {args.output}")
    logger.info("Шаг 1 завершён: статистика PDF (архив: {} шт., найдено объектов: {}), переход к шагу 2 (OCR PDF)".format(len(sha1_list), len(result)))
    return 0

if __name__ == "__main__":
    exit(main())
