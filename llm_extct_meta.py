# llm_extct_meta.py

import os
import json
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
from openai import OpenAI
from schema_and_prompts import schema_1, system_prompts

log_dir = Path("LOGS")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "llm_extct_meta.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("llm_extct_meta")



def prepare_context_for_llm(pdf_data: Dict[str, List]) -> Dict[str, str]:
    
    """
    Подготавливает текстовый контекст для каждой страницы PDF.
    Для каждой страницы объединяются тексты элементов (если элемент является таблицей или изображением, берется его анализ).
    """
    global schema_1
    from schema_and_prompts import schema_1
    
    
    contexts = {}
    logger.info(f"Preparing context for {len(pdf_data)} pages")
    for page, items in pdf_data.items():
        parts = [f"Page {page}:"]
        for item in items:
            if item.get("category") == "Table":
                parts.append(f"Table: {item.get('table_analysis', '')}")
            elif item.get("category") == "Image":
                parts.append(f"Image: {item.get('vision_analysis', '')}")
            else:
                parts.append(item.get("content", ""))
        contexts[page] = " ".join(parts)
    return contexts


def prepare_batch_file(pdf_data: Dict[str, Dict], system_prompt: str, model: str) -> (str, Dict):
    """
    Формирует batch‑файл в формате JSONL из данных PDF.
    Для каждой страницы создаётся запрос с кастомным ID.
    """
    logger.info("Preparing batch file for PDF documents")
    batch_lines = []
    batch_metadata = {}
    for pdf_name, pages in pdf_data.items():
        contexts = prepare_context_for_llm(pages)
        pdf_meta = {"pages": {}}
        for page, context in contexts.items():
            custom_id = f"{pdf_name}__page-{page}"
            request = {
                "custom_id": custom_id,
                "body": {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": context}
                    ],
                    "max_completion_tokens": 6000
                }
            }
            batch_lines.append(json.dumps(request, ensure_ascii=False))
            pdf_meta["pages"][page] = {"status": "queued"}
        batch_metadata[pdf_name] = pdf_meta
    batch_file_path = "batch_requests.jsonl"
    with open(batch_file_path, "w", encoding="utf-8") as f:
        f.write("\n".join(batch_lines))
    logger.info(f"Batch file created: {batch_file_path} with {len(batch_lines)} requests")
    return batch_file_path, batch_metadata


def submit_batch_job(client, batch_file_path, state_dir, metadata):
    """
    Отправляет пакетное задание через OpenAI API:
      – Загружает batch‑файл.
      – Создает задание.
      – Сохраняет информацию о задании в state_dir.
    """
    logger.info("Submitting batch job to OpenAI API")
    try:
        with open(batch_file_path, "rb") as f:
            batch_input_file = client.files.create(file=f, purpose="batch")
        batch = client.batches.create(
            input_file_id=batch_input_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
            metadata={"description": f"Batch job submitted at {datetime.now().isoformat()}"}
        )
        logger.info(f"Batch job created: {batch.id}")
        batch_info = {
            "batch_id": batch.id,
            "input_file_id": batch_input_file.id,
            "created_at": datetime.now().isoformat(),
            "status": "submitted",
            "metadata": metadata
        }
        batch_info_path = os.path.join(state_dir, "batch_info.json")
        with open(batch_info_path, "w", encoding="utf-8") as f:
            json.dump(batch_info, f, ensure_ascii=False, indent=2)
        logger.info(f"Batch info saved to {batch_info_path}")
        return batch.id
    except Exception as e:
        logger.error(f"Error submitting batch job: {e}")
        return None


def check_batch_status(client, state_dir, output_dir):
    """
    Проверяет статус пакетного задания.
    Если задание завершено, загружает результаты и сохраняет их в output_dir.
    """
    batch_info_path = os.path.join(state_dir, "batch_info.json")
    if not os.path.exists(batch_info_path):
        logger.info("Batch info not found")
        return "not_found"
    try:
        with open(batch_info_path, "r", encoding="utf-8") as f:
            batch_info = json.load(f)
        batch_id = batch_info.get("batch_id")
        if not batch_id:
            logger.error("No batch_id in batch info")
            return "error"
        batch = client.batches.retrieve(batch_id)
        status = batch.status
        logger.info(f"Batch job {batch_id} status: {status}")
        batch_info["status"] = status
        with open(batch_info_path, "w", encoding="utf-8") as f:
            json.dump(batch_info, f, ensure_ascii=False, indent=2)
        if status == "completed" and batch.output_file_id:
            logger.info("Batch job completed, downloading results")
            file_response = client.files.content(batch.output_file_id)
            # Разбиваем результаты по строкам и группируем по PDF
            batch_results = [json.loads(line) for line in file_response.text.strip().split('\n')]
            pdf_results = {}
            for res in batch_results:
                custom_id = res["custom_id"]
                pdf, page_part = custom_id.split("__")
                page = page_part.replace("page-", "")
                if pdf not in pdf_results:
                    pdf_results[pdf] = {}
                pdf_results[pdf][page] = res.get("response", {})
            for pdf, data in pdf_results.items():
                output_file = os.path.join(output_dir, f"{Path(pdf).stem}_metadata.json")
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                logger.info(f"Results for {pdf} saved in {output_file}")
            os.remove(batch_info_path)
            logger.info("Batch info removed")
            return "completed_and_processed"
        return status
    except Exception as e:
        logger.error(f"Error checking batch status: {e}")
        return "error"


def process_all_pdfs(analyzed_data_file: str, api_key: str, output_dir: str, model: str = "gpt-4o-mini"):
    """
    Обрабатывает все PDF из файла с результатами анализа:
      – Загружает данные.
      – Исключает уже обработанные PDF.
      – Готовит batch‑файл и отправляет пакетное задание.
    """
    os.makedirs(output_dir, exist_ok=True)
    state_dir = os.path.join(output_dir, "state")
    os.makedirs(state_dir, exist_ok=True)
    client = OpenAI(api_key=api_key)

    # Если уже есть информация о задании, проверяем его статус
    batch_info_path = os.path.join(state_dir, "batch_info.json")
    if os.path.exists(batch_info_path):
        logger.info("Existing batch job found. Checking status...")
        status = check_batch_status(client, state_dir, output_dir)
        if status in ["completed", "completed_and_processed"]:
            logger.info("Batch job already processed.")
            return
        elif status in ["submitted", "in_progress"]:
            logger.info("Batch job is still processing. No new job will be submitted.")
            return
        else:
            logger.warning(f"Previous batch job failed: {status}. Submitting new job.")
            os.remove(batch_info_path)

    logger.info(f"Loading analyzed data from {analyzed_data_file}")
    with open(analyzed_data_file, "r", encoding="utf-8") as f:
        analyzed_data = json.load(f)

    # Исключаем PDF, для которых уже созданы результаты
    for pdf in list(analyzed_data.keys()):
        output_file = os.path.join(output_dir, f"{Path(pdf).stem}_metadata.json")
        if os.path.exists(output_file):
            logger.info(f"{pdf} already processed, skipping")
            del analyzed_data[pdf]
    if not analyzed_data:
        logger.info("No new PDFs to process")
        return

    system_prompt = system_prompts
    batch_file, batch_metadata = prepare_batch_file(analyzed_data, system_prompt, model)
    batch_id = submit_batch_job(client, batch_file, state_dir, batch_metadata)
    if batch_id:
        logger.info(f"Batch job submitted: {batch_id}")
    else:
        logger.error("Failed to submit batch job")
    if os.path.exists(batch_file):
        os.remove(batch_file)
        logger.info(f"Temporary batch file {batch_file} removed")


def main():
    """
    Основная функция для запуска обработки.
    Принимает аргументы:
      --analyzed_data: путь к файлу с результатами анализа PDF
      --output_dir: директория для сохранения результатов
      --api_key: API-ключ OpenAI (либо через переменную окружения)
      --schema: путь к файлу со схемой (необязательно)
      --check_only: если указан, только проверяет статус существующего задания
      --model: имя модели OpenAI для обработки
    """
    parser = argparse.ArgumentParser(description="Generate metadata for PDF documents")
    parser.add_argument("--analyzed_data", default="results/update_data/all_documents_analyzed.json",
                        help="Path to all_documents_analyzed.json")
    parser.add_argument("--output_dir", default="results/llm_meta_exct_data",
                        help="Directory to save results")
    parser.add_argument("--api_key", help="API key for OpenAI (or set OPENAI_API_KEY environment variable)")
    parser.add_argument("--schema", default = schema_1, help="Path to JSON schema file for OpenAI response")
    parser.add_argument("--check_only", action="store_true", help="Only check current batch job status")
    parser.add_argument("--model", default="gpt-4o-mini", help="OpenAI model to use")
    args = parser.parse_args()

    api_key = args.api_key or os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("API key for OpenAI is required")
    
    global schema_1 # ЗАКОМЕНТИРОВАТЬ ВО ВОРЕМЯ ЗАПУСКА ТЕСТОВ
    if args.schema:
        with open(args.schema, "r", encoding="utf-8") as f:
            schema_1 = json.load(f)

    client = OpenAI(api_key=api_key)
    os.makedirs(args.output_dir, exist_ok=True)
    state_dir = os.path.join(args.output_dir, "state")
    os.makedirs(state_dir, exist_ok=True)

    if args.check_only:
        logger.info("Checking status of current batch job...")
        status = check_batch_status(client, state_dir, args.output_dir)
        print(f"Batch job status: {status}")
    else:
        logger.info(f"Starting processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        try:
            process_all_pdfs(args.analyzed_data, api_key, args.output_dir, args.model)
        except Exception as e:
            logger.error(f"Critical error processing PDFs: {e}")
            return 1
    return 0


if __name__ == "__main__":
    main()
