# run_pdf_preprocessor.py
"""
Запуск полного пайплайна для Enterprise RAG Challenge
"""

import os
import sys
import argparse
import subprocess
import logging
import time
from pathlib import Path

# Настройка логирования: вывод в консоль и в файл rag_pipeline.log
log_dir = Path("LOGS")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "run_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("run_pipeline")

def run_command(command, description):
    """
    Запускает команду, логирует ее вывод и время выполнения.
    
    Args:
        command (list): Команда и аргументы.
        description (str): Описание шага.
        
    Returns:
        bool: True, если команда выполнена успешно, иначе False.
    """
    logger.info(f"Шаг: {description}")
    logger.info("Команда: " + " ".join(command))
    start = time.time()
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        if result.stdout:
            logger.info(result.stdout.strip())
        if result.stderr:
            logger.error(result.stderr.strip())
        elapsed = time.time() - start
        logger.info(f"Шаг выполнен успешно: {description} (время: {elapsed:.2f} секунд)")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка шага {description}. Код: {e.returncode}")
        if e.stdout:
            logger.error(e.stdout.strip())
        if e.stderr:
            logger.error(e.stderr.strip())
        return False

def main():
    parser = argparse.ArgumentParser(description="Запуск полного пайплайна для Enterprise RAG Challenge")
    parser.add_argument("--zip", default="temp_pdfs/pdfs.zip", help="Путь к zip-архиву с PDF")
    parser.add_argument("--dataset", default="data/dataset_v2.json", help="Путь к dataset_v2.json")
    parser.add_argument("--output_dir", default="results", help="Директория для результатов")
    parser.add_argument("--api_key", required=True, help="API ключ для OpenAI")
    parser.add_argument("--anthropic_api_key", required=True, help="API ключ для Anthropic")
    parser.add_argument("--start_step", type=int, default=1, help="Номер шага, с которого начать (1-5)")
    args = parser.parse_args()
    
    os.environ["OPENAI_API_KEY"] = args.api_key
    os.environ["ANTHROPIC_API_KEY"] = args.anthropic_api_key

    # Создаем базовую директорию и необходимые поддиректории
    base_dir = Path(args.output_dir)
    dirs = {
        "metadata": base_dir,
        "ocr": base_dir / "ocr_data",
        "analyzed": base_dir / "analyzed_data",
        "llm_meta": base_dir / "llm_extr_meta",
        "aggregated": base_dir / "aggregated"  # Объединенная агрегация (промежуточная и финальная)
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    
    metadata_json = str(dirs["metadata"] / "selected_metadata.json")
    ocr_all_json = str(dirs["ocr"] / "all_documents_ocr.json")
    analyzed_all_json = str(dirs["analyzed"] / "all_documents_analyzed.json")
    
    # Шаг 1: Извлечение метаданных из dataset_v2.json
    if args.start_step <= 1:
        if not run_command(
            ["python", "zip_extract.py", "--zip", args.zip, "--dataset", args.dataset, "--output", metadata_json],
            "Извлечение метаданных (Шаг 1)"
        ):
            logger.error("Пайплайн остановлен на шаге 1")
            return 1

    # Шаг 2: OCR обработка PDF
    if args.start_step <= 2:
        if not run_command(
            ["python", "pdf_extract.py", "--zip", args.zip, "--output", str(dirs["ocr"])],
            "OCR обработка PDF (Шаг 2)"
        ):
            logger.error("Пайплайн остановлен на шаге 2")
            return 1

    # Шаг 3: Анализ изображений и таблиц с использованием Claude
    if args.start_step <= 3:
        if not run_command(
            ["python", "llm_img_tbl_job.py", "--ocr_folder", str(dirs["ocr"]), "--output", str(dirs["analyzed"])],
            "Анализ изображений и таблиц (Шаг 3)"
        ):
            logger.error("Пайплайн остановлен на шаге 3")
            return 1

    # Шаг 4: Генерация метаданных с помощью OpenAI
    if args.start_step <= 4:
        if not run_command(
            ["python", "llm_extct_meta.py", "--analyzed_data", analyzed_all_json, "--output_dir", str(dirs["llm_meta"]), "--batch_size", "10"],
            "Генерация метаданных LLM (Шаг 4)"
        ):
            logger.error("Пайплайн остановлен на шаге 4")
            return 1

    # Шаг 5: Агрегация метаданных (объединенная)
    if args.start_step <= 5:
        if not run_command(
            ["python", "aggregated.py", "--target", metadata_json, "--metadata_dir", str(dirs["llm_meta"]), "--output_dir", str(dirs["aggregated"])],
            "Агрегация метаданных (Шаг 5)"
        ):
            logger.error("Пайплайн остановлен на шаге 5")
            return 1

    logger.info("Все шаги пайплайна успешно выполнены!")
    logger.info(f"Финальные результаты сохранены в {dirs['aggregated']}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
