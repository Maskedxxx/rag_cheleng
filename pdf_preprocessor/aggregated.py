# pdf_preprocessor/aggregated.py

import os
import json
import copy
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional

# Настройка логирования: вывод в консоль и в файл aggregation.log
log_dir = Path("LOGS")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "aggregated.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("aggregated")

# Соответствие типов метаданных полям результата
TYPE_TO_FIELD_MAP = {
    "merger_acquisition": "mentions_recent_mergers_and_acquisitions",
    "leadership_change": "has_leadership_changes",
    "layoff": "has_layoffs",
    "executive_compensation": "has_executive_compensation",
    "rnd_investment": "has_rnd_investment_numbers",
    "product_launch": "has_new_product_launches",
    "capital_expenditure": "has_capital_expenditures",
    "financial_performance": "has_financial_performance_indicators",
    "dividend_policy": "has_dividend_policy_changes",
    "share_buyback": "has_share_buyback_plans",
    "capital_structure": "has_capital_structure_changes",
    "risk_factor": "mentions_new_risk_factors",
    "guidance_update": "has_guidance_updates",
    "regulatory_litigation": "has_regulatory_or_litigation_issues",
    "strategic_restructuring": "has_strategic_restructuring",
    "supply_chain_disruption": "has_supply_chain_disruptions",
    "esg_initiative": "has_esg_initiatives"
}

def find_target_object(data: Dict[str, Any], pdf_name: str, sha1: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Ищет целевой объект в данных по имени PDF или SHA1.
    """
    if pdf_name in data:
        return data[pdf_name]
    for item in data.values():
        if isinstance(item, dict):
            if sha1 and item.get("sha1") == sha1:
                return item
            company = item.get("meta", {}).get("company_name") or item.get("company_name")
            if company and Path(pdf_name).stem.lower() in company.lower():
                return item
    return None

def create_empty_template(target_obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Создает пустой шаблон на основе целевого объекта, сбрасывая все поля meta
    до значений False для ключей, заданных в TYPE_TO_FIELD_MAP.
    """
    template = copy.deepcopy(target_obj)
    template["meta"] = {field: False for field in TYPE_TO_FIELD_MAP.values()}
    return template

def extract_metadata_by_type(analysis_results: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Группирует извлеченные метаданные по типу из результатов анализа.
    """
    grouped = {}
    for page, data in analysis_results.items():
        if not (isinstance(data, dict) and "metadata" in data):
            continue
        meta = data["metadata"]
        mtype = meta.get("type")
        if not mtype or mtype == "empty":
            continue
        if not (meta.get("entity") and isinstance(meta["entity"], dict) and isinstance(meta["entity"].get("documents"), list)):
            continue
        for doc in meta["entity"]["documents"]:
            element = {
                "type": mtype,
                "page": doc.get("page"),
                "title": doc.get("title"),
                "data": doc.get("data", []),
                "currency": doc.get("currency")
            }
            grouped.setdefault(mtype, []).append(element)
    return grouped

def aggregate_single_pdf(pdf_name: str, metadata_file: str, target_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Агрегирует результаты анализа для одного PDF:
      – Находит целевой объект,
      – Создает шаблон,
      – Загружает результаты анализа и обновляет шаблон.
    """
    logger.info(f"Aggregating results for {pdf_name}")
    target_obj = find_target_object(target_data, pdf_name, sha1=pdf_name)
    if not target_obj:
        logger.warning(f"Target object not found for {pdf_name}")
        return None
    result_obj = create_empty_template(target_obj)
    try:
        with open(metadata_file, 'r', encoding='utf-8') as f:
            analysis_results = json.load(f)
    except Exception as e:
        logger.error(f"Error loading analysis results for {pdf_name}: {e}")
        return result_obj
    metadata_by_type = extract_metadata_by_type(analysis_results)
    for mtype, field in TYPE_TO_FIELD_MAP.items():
        if mtype in metadata_by_type:
            result_obj["meta"][field] = True
            result_obj.setdefault("extracted_elements", []).extend(metadata_by_type[mtype])
    return result_obj

def reorganize_structure(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Преобразует структуру объекта, группируя извлеченные элементы по соответствующим полям.
    """
    result = {
        "letters": data.get("letters"),
        "pages": data.get("pages"),
        "meta": {
            "end_of_period": data.get("meta", {}).get("end_of_period"),
            "company_name": data.get("meta", {}).get("company_name"),
            "major_industry": data.get("meta", {}).get("major_industry")
        },
        "currency": data.get("currency"),
        "sha1": data.get("sha1")
    }
    for field in TYPE_TO_FIELD_MAP.values():
        result["meta"][field] = {"value": data.get("meta", {}).get(field, False), "elements": []}
    for element in data.get("extracted_elements", []):
        etype = element.get("type")
        if etype in TYPE_TO_FIELD_MAP:
            field = TYPE_TO_FIELD_MAP[etype]
            result["meta"][field]["elements"].append(element)
    for field, info in result["meta"].items():
        if isinstance(info, dict) and "elements" in info:
            info["elements"].sort(key=lambda x: x.get("page", 0))
    return result

def process_pdfs(target_data_file: str, metadata_dir: str, output_dir: str, final_dir: str):
    """
    Обрабатывает все PDF:
      – Загружает целевые данные,
      – Агрегирует результаты по каждому PDF,
      – Сохраняет промежуточные и финальные результаты.
    """
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(final_dir, exist_ok=True)
    try:
        with open(target_data_file, 'r', encoding='utf-8') as f:
            target_data = json.load(f)
    except Exception as e:
        logger.error(f"Error loading target data: {e}")
        return
    metadata_files = list(Path(metadata_dir).glob("*_metadata.json"))
    logger.info(f"Found {len(metadata_files)} metadata files")
    aggregated_results = {}
    for m_file in metadata_files:
        pdf_name = m_file.stem.replace("_metadata", "")
        result = aggregate_single_pdf(pdf_name, str(m_file), target_data)
        if result:
            aggregated_results[pdf_name] = result
            pdf_output = os.path.join(output_dir, f"{Path(pdf_name).stem}_aggregated.json")
            with open(pdf_output, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
    with open(os.path.join(output_dir, "all_aggregated_results.json"), 'w', encoding='utf-8') as f:
        json.dump(aggregated_results, f, ensure_ascii=False, indent=2)
    logger.info(f"Aggregation complete. Results saved in {output_dir}")
    for pdf_name, result in aggregated_results.items():
        final_result = reorganize_structure(result)
        final_output = os.path.join(final_dir, f"{Path(pdf_name).stem}_final.json")
        with open(final_output, 'w', encoding='utf-8') as f:
            json.dump(final_result, f, ensure_ascii=False, indent=2)
    all_final = {k: reorganize_structure(v) for k, v in aggregated_results.items()}
    with open(os.path.join(final_dir, "all_final_results.json"), 'w', encoding='utf-8') as f:
        json.dump(all_final, f, ensure_ascii=False, indent=2)
    logger.info(f"Reorganization complete. Final results saved in {final_dir}")

def main():
    """
    Основная функция для запуска агрегации.
    """
    parser = argparse.ArgumentParser(description="Aggregate and reorganize document analysis results")
    parser.add_argument("--target", default="data/dataset_v2.json", help="Path to target data file")
    parser.add_argument("--metadata_dir", default="results/llm_meta_exct_data/", help="Directory with metadata files")
    parser.add_argument("--output_dir", default="results/pre_agr", help="Directory for aggregated results")
    parser.add_argument("--final_dir", default="results/final_agr", help="Directory for final results")
    args = parser.parse_args()
    process_pdfs(args.target, args.metadata_dir, args.output_dir, args.final_dir)
    logger.info("Aggregation process completed")

if __name__ == "__main__":
    main()
