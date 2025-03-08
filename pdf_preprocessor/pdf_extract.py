# pdf_preprocessor/pdf_extract.py

import os
import json
import zipfile
import argparse
import time
import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, Optional, List, DefaultDict, Any, Tuple

from unstructured.partition.pdf import partition_pdf

log_dir = Path("LOGS")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "pdf_extract.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("pdf_extract")

def process_image(element) -> Optional[Dict[str, Any]]:
    if element.category == "Image" and hasattr(element.metadata, "image_base64") and element.metadata.image_base64:
        if len(element.text.split()) > 10:
            return {"category": "Image", "content": element.text, "image_base64": element.metadata.image_base64}
        else:
            return {"category": "Image", "content": element.text}
    return None

def process_table(element) -> Optional[Dict[str, Any]]:
    if element.category == "Table" and hasattr(element.metadata, "text_as_html") and element.metadata.text_as_html:
        return {"category": "Table", "content": element.text, "text_as_html": element.metadata.text_as_html}
    return None

def process_pdf(pdf_path: str) -> Tuple[DefaultDict[int, List[Dict[str, Any]]], List[Any]]:
    elements = partition_pdf(
        filename=pdf_path,
        strategy="hi_res",
        extract_image_block_to_payload=True,
        extract_image_block_types=["Image", "Table"],
        infer_table_structure=True
    )
    pages = defaultdict(list)
    for element in elements:
        page_number = getattr(element.metadata, "page_number", None)
        image_data = process_image(element)
        if image_data:
            pages[page_number].append(image_data)
            continue
        table_data = process_table(element)
        if table_data:
            pages[page_number].append(table_data)
            continue
        pages[page_number].append({"category": element.category, "content": element.text})
    return pages, elements

def process_pdfs_in_zip(zip_path: str, extract_dir: Optional[str] = None) -> Dict[str, Dict]:
    if extract_dir is None:
        extract_dir = Path("data")
    else:
        extract_dir = Path(extract_dir)
    extract_dir.mkdir(exist_ok=True)

    all_pdfs_data = {}
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_list = [f for f in zip_ref.namelist() if f.lower().endswith('.pdf')]
            logger.info(f"Найдено {len(file_list)} PDF файлов в архиве {zip_path}.")
            for file_name in file_list:
                logger.info(f"Начало обработки файла: {file_name}")
                try:
                    start_time = time.time()
                    extracted_file_path = zip_ref.extract(file_name, extract_dir)
                    logger.info(f"Файл {file_name} извлечён за {time.time() - start_time:.2f} сек.")
                    
                    start_time = time.time()
                    pages_data, _ = process_pdf(str(Path(extracted_file_path)))
                    logger.info(f"Файл {file_name} обработан за {time.time() - start_time:.2f} сек.")
                    
                    all_pdfs_data[Path(file_name).name] = dict(pages_data)
                except Exception as e:
                    logger.error(f"Ошибка при обработке {file_name}: {e}")
                    all_pdfs_data[Path(file_name).name] = {"error": str(e)}
    except Exception as e:
        logger.error(f"Ошибка открытия zip-архива {zip_path}: {e}")
    return all_pdfs_data

def save_data_to_json(data: Dict, output_filename: str):
    with open(output_filename, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)

def main():
    parser = argparse.ArgumentParser(description='Обработка PDF-файлов из ZIP для RAG конвейера')
    parser.add_argument('--zip', default="data/<путь_к_zip_файлу>", help='Путь к ZIP-архиву с PDF-файлами')
    parser.add_argument('--output', default="results/ocr_data", help='Путь для сохранения результатов OCR')
    parser.add_argument('--extract_dir', default=None, help='Каталог для извлечения файлов (по умолчанию data)')
    args = parser.parse_args()
    
    logger.info(f"Начало обработки архива {args.zip}.")
    pdf_data = process_pdfs_in_zip(args.zip, args.extract_dir)
    
    output_dir = Path(args.output)
    output_dir.mkdir(exist_ok=True, parents=True)
    
    # Сохраняем результаты для каждого PDF-файла отдельно
    for pdf_name, pdf_content in pdf_data.items():
        output_filename = output_dir / f"{Path(pdf_name).stem}_ocr.json"
        save_data_to_json(pdf_content, str(output_filename))
        logger.info(f"Результаты OCR для {pdf_name} сохранены в {output_filename}.")
    
    # Сохраняем полный результат в один файл
    full_output = output_dir / "all_documents_ocr.json"
    save_data_to_json(pdf_data, str(full_output))
    logger.info(f"Все данные OCR сохранены в {full_output}.")
    
    logger.info("Шаг 2 завершён. Переход к следующему этапу обработки.")
    return 0

if __name__ == "__main__":
    exit(main())
