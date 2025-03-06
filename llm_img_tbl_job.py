# llm_img_tbl_job.py

import asyncio
import os
import json
import argparse
from pathlib import Path
from typing import Dict, Optional, Any
import glob
import logging
import anthropic

log_dir = Path("LOGS")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "llm_img_tbl_job.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("llm_img_tbl_job")

# Максимальное число одновременных запросов к API
MAX_CONCURRENT_REQUESTS = 5

def create_anthropic_client() -> Any:
    """
    Создает и возвращает асинхронного клиента Anthropic.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("Не установлена переменная окружения ANTHROPIC_API_KEY")
    return anthropic.AsyncAnthropic(api_key=api_key)

async def analyze_image_async(client: Any, page_number: str, element_index: int, base64_image: str) -> tuple:
    """
    Асинхронно анализирует изображение с помощью модели Anthropic.
    Если изображение пустое, возвращает None в качестве результата.
    """
    if not base64_image or not base64_image.strip():
        return (page_number, element_index, None)
    
    prompt_text = (
        "You are a business report and literature analyst, specializing in the analysis of visual data from business documents, "
        "such as PDFs containing reports from banks and companies. You have been provided with an image extracted from such a document. "
        "Your task is to analyze the image. If the image contains a chart or a table, "
        "provide a structured response in the form of a valid JSON object with the following fields:\n"
        "  • \"type\": a string, either \"chart\" or \"table\"\n"
        "  • \"title\": the title of the chart or table\n"
        "  • \"data\": for charts - an array of objects, where each object contains values for both axes "
        "(e.g., [{\"date\": \"2023-01\", \"value\": 100}, ...]). For tables - an array of objects, where each object represents a row\n"
        "  • \"metadata\": (optional) additional information (e.g., axis labels, legend, etc.)\n"
        "If the image does not contain a chart or a table, briefly describe what is depicted in the image in text form.\n"
        "YOUR response must be a valid JSON object that can be safely initialized using json.loads()."
    )
    
    try:
        response = await client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=8000,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/jpeg",
                                "data": base64_image
                            }
                        },
                        {
                            "type": "text",
                            "text": prompt_text
                        }
                    ]
                }
            ]
        )
        analysis_result = response.content[0].text
        return (page_number, element_index, analysis_result)
    except Exception as e:
        logger.error(f"Ошибка при анализе изображения (страница {page_number}, элемент {element_index}): {e}")
        return (page_number, element_index, None)

async def analyze_table_async(client: Any, page_number: str, element_index: int, html_content: str) -> tuple:
    """
    Асинхронно анализирует HTML-таблицу с помощью модели Anthropic.
    Если HTML-контент пустой, возвращает None.
    """
    if not html_content or not html_content.strip():
        return (page_number, element_index, None)
    
    table_prompt = (
        "You are an expert in analyzing HTML tables extracted from business reports and financial documents of banks and companies. "
        "You are provided with an HTML snippet representing a table extracted from such a document. Your task is to analyze the table and "
        "return a structured JSON object that summarizes its content. The JSON object must include the following fields:\n"
        "  - 'data': an array of objects, where each object represents a row in the table. If the table includes headers, use them as keys; "
        "    otherwise, use numeric indices as keys.\n"
        "  - 'summary': a brief summary of the table content, describing what the table shows and any notable trends or figures.\n"
        "  - Optionally, a 'title' field if the table contains a title.\n\n"
        "Ensure your output is a valid JSON string (i.e. it can be safely parsed using json.loads())."
    )
    
    try:
        response = await client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=8000,
            messages=[
                {
                    "role": "user", 
                    "content": [
                        {
                            "type": "text",
                            "text": table_prompt + "\n\nHTML Table:\n" + html_content
                        }
                    ]
                }
            ]
        )
        analysis_result = response.content[0].text
        return (page_number, element_index, analysis_result)
    except Exception as e:
        logger.error(f"Ошибка при анализе таблицы (страница {page_number}, элемент {element_index}): {e}")
        return (page_number, element_index, None)

async def process_ocr_data_async(client: Any, ocr_folder_path: str, output_folder: Optional[str] = None) -> Dict[str, Dict]:
    """
    Асинхронно обрабатывает файлы с результатами OCR:
    - Сканирует папку с OCR-данными (исключая общий файл).
    - Пропускает файлы, для которых уже есть анализ (файл с суффиксом _analyzed.json).
    - Для каждого файла собирает изображения и таблицы, требующие анализа.
    - Выполняет асинхронный анализ с ограничением по числу одновременных запросов.
    - Сохраняет обновленные данные для каждого файла и общий результат.
    """
    if not output_folder:
        output_folder = ocr_folder_path
        
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    
    # Получаем список всех OCR JSON файлов (исключая общий файл)
    ocr_files = [f for f in glob.glob(os.path.join(ocr_folder_path, "*.json"))
                 if os.path.basename(f) != "all_documents_ocr.json"]
    
    # Определяем файлы, для которых уже создан анализ (_analyzed.json)
    processed_files = { os.path.basename(f).replace("_analyzed.json", "_ocr.json")
                        for f in glob.glob(os.path.join(output_folder, "*_analyzed.json")) }
    ocr_files_to_process = [f for f in ocr_files if os.path.basename(f) not in processed_files]
    
    logger.info(f"Найдено {len(ocr_files)} OCR файлов, {len(ocr_files_to_process)} из них нужно обработать.")
    
    all_results = {}
    images_count = 0
    tables_count = 0
    
    # Загружаем уже обработанные файлы в общий результат
    for analyzed_file in glob.glob(os.path.join(output_folder, "*_analyzed.json")):
        base_name = os.path.basename(analyzed_file).replace("_analyzed.json", "_ocr.json")
        pdf_name = base_name.replace("_ocr.json", ".pdf")
        try:
            with open(analyzed_file, "r", encoding="utf-8") as f:
                all_results[pdf_name] = json.load(f)
            logger.info(f"Загружен ранее обработанный файл: {analyzed_file}")
        except Exception as e:
            logger.error(f"Ошибка при загрузке файла {analyzed_file}: {e}")
    
    # Обработка каждого OCR файла
    for ocr_file in ocr_files_to_process:
        file_name = os.path.basename(ocr_file)
        logger.info(f"Обработка файла: {file_name}")
        
        with open(ocr_file, "r", encoding="utf-8") as f:
            ocr_data = json.load(f)
        
        images_to_analyze = []
        tables_to_analyze = []
        
        # Проходим по страницам и элементам, собираем изображения и таблицы для анализа
        for page_number, elements in ocr_data.items():
            for i, element in enumerate(elements):
                if element.get("category") == "Image" and element.get("image_base64", "").strip() and "vision_analysis" not in element:
                    images_to_analyze.append((page_number, i, element["image_base64"]))
                elif element.get("category") == "Table" and element.get("text_as_html") and "table_analysis" not in element:
                    tables_to_analyze.append((page_number, i, element["text_as_html"]))
        
        logger.info(f"В файле {file_name} найдено: {len(images_to_analyze)} изображений, {len(tables_to_analyze)} таблиц для анализа.")
        images_count += len(images_to_analyze)
        tables_count += len(tables_to_analyze)
        
        # Асинхронный анализ изображений партиями
        if images_to_analyze:
            for i in range(0, len(images_to_analyze), MAX_CONCURRENT_REQUESTS):
                batch = images_to_analyze[i:i+MAX_CONCURRENT_REQUESTS]
                results = await asyncio.gather(*[
                    analyze_image_async(client, page, idx, img) for page, idx, img in batch
                ])
                for page, idx, analysis in results:
                    if analysis:
                        ocr_data[page][idx]["vision_analysis"] = analysis
                logger.info(f"Обработано изображений: {min(i+MAX_CONCURRENT_REQUESTS, len(images_to_analyze))}/{len(images_to_analyze)}")
        
        # Асинхронный анализ таблиц партиями
        if tables_to_analyze:
            for i in range(0, len(tables_to_analyze), MAX_CONCURRENT_REQUESTS):
                batch = tables_to_analyze[i:i+MAX_CONCURRENT_REQUESTS]
                results = await asyncio.gather(*[
                    analyze_table_async(client, page, idx, html) for page, idx, html in batch
                ])
                for page, idx, analysis in results:
                    if analysis:
                        ocr_data[page][idx]["table_analysis"] = analysis
                logger.info(f"Обработано таблиц: {min(i+MAX_CONCURRENT_REQUESTS, len(tables_to_analyze))}/{len(tables_to_analyze)}")
        
        # Сохраняем обновленные данные OCR для файла
        output_file = os.path.join(output_folder, f"{os.path.splitext(file_name)[0]}_analyzed.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(ocr_data, f, ensure_ascii=False, indent=4)
        logger.info(f"Сохранён анализ файла {file_name} в {output_file}")
        
        pdf_name = file_name.replace("_ocr.json", ".pdf")
        all_results[pdf_name] = ocr_data
    
    # Сохраняем общий результат
    overall_file = os.path.join(output_folder, "all_documents_analyzed.json")
    with open(overall_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=4)
    logger.info(f"Итого: проанализировано {images_count} изображений и {tables_count} таблиц в {len(ocr_files_to_process)} файлах.")
    
    return all_results

async def main_async():
    """
    Асинхронная основная функция для анализа данных OCR.
    """
    parser = argparse.ArgumentParser(description='Анализ изображений и таблиц из результатов OCR для RAG конвейера')
    parser.add_argument('--ocr_folder', default="results/ocr_data", help='Папка с результатами OCR')
    parser.add_argument('--output', default="results/analyzed_data", help='Папка для сохранения результатов анализа')
    args = parser.parse_args()
    
    try:
        client = create_anthropic_client()
        logger.info(f"Начало анализа OCR данных из папки {args.ocr_folder}...")
        results = await process_ocr_data_async(client, args.ocr_folder, args.output)
        logger.info(f"Шаг 3 завершён. Обработано {len(results)} PDF-файлов. Результаты сохранены в {args.output}.")
        return 0
    except Exception as e:
        logger.error(f"Ошибка в main_async: {e}", exc_info=True)
        return 1

def main():
    """
    Точка входа для запуска асинхронного анализа.
    """
    return asyncio.run(main_async())

if __name__ == "__main__":
    exit(main())
