import json
import os
import glob
import logging
from pathlib import Path
from typing import List, Union, Literal, Optional
from pydantic import BaseModel, Field
from openai import OpenAI

# Настройка логирования: вывод в консоль и в файл rag_pipeline.log
log_dir = Path("LOGS")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "rag_qa_final.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("rag_qa_final")

# Инициализация клиента OpenAI (замените "****" на реальный API-ключ или используйте переменные окружения)
client = OpenAI()

class QuestionAnswer(BaseModel):
    data_analysis: List[str] = Field(
        ...,
        description=(
            "Summarize the data analysis performed on the metadata. Highlight relevant figures, "
            "trends, or any discrepancies observed in the documents."
        )
    )
    reasoning: List[str] = Field(
        ...,
        description=(
            "Provide a detailed explanation of your thought process and analysis "
            "derived from the metadata. Include key insights that justify your answer."
        )
    )
    answer_type: Literal["number", "boolean", "name", "names"] = Field(
        ...,
        description=(
            "Specify the type of the answer. Use 'number' for numerical answers, 'boolean' for yes/no responses, "
            "'name' for a single name, and 'names' for multiple names."
        )
    )
    answer: Union[float, str, bool, List[str], Literal["N/A"]] = Field(
        ...,
        description=(
            "Provide the final answer corresponding to the question with strict type constraints:\n"
            "- For 'name' answer_type: Return ONLY the specific name of the person mentioned in the question, Or tell me the name of this position if the question requires it.\n"
            "- For 'names' answer_type: Return ONLY the list of names of personnel mentioned in the question, Or tell me the name of this position if the question requires it.\n"
            "- For 'number' answer_type: Return a float value without commas\n"
            "- For 'boolean' answer_type: Return True or False explicitly\n"
            "- Return 'N/A' if metadata is insufficient or inconsistent for text/number answers\n"
            "- Return False for boolean answers when information is unclear"
        )
    )
    pages: Union[int, Literal[0]] = Field(
        ...,
        description=(
            "Indicate the page number from the source document that contains the most relevant information. "
            "Return 0 if no specific page is applicable."
        )
    )

# Системный промпт для модели
SYSTEM_PROMPT = """
You are a seasoned business analyst working for a leading corporate firm. Your role is to answer questions based on metadata extracted from annual reports and other business documents.
Analyze the question and the provided metadata context carefully, then produce a structured answer that strictly conforms to the following schema:

- reasoning: Provide a detailed explanation of your analysis and thought process.
- data_analysis: Summarize any data-driven insights, trends, or discrepancies from the documents.
- answer_type: Indicate the type of answer required (choose from "number", "boolean", "name", or "names").
- answer: Provide the final answer. If the metadata does not sufficiently address the question or contains conflicts, return "N/A" (or False if the answer should be boolean).
- pages: Specify the page number from the source document containing the key information, or "N/A" if no specific page is relevant.

Ensure your answer is precise, contextually grounded in the provided metadata, and correctly fills out the schema.
"""

def process_question(company_name: str, question_text: str, question_kind: str, metadata_elements: list, search_locations: Optional[List] = None) -> QuestionAnswer:
    """
    Обрабатывает вопрос с помощью LLM и возвращает структурированный ответ.

    Args:
        company_name (str): Название компании
        question_text (str): Текст вопроса
        question_kind (str): Тип вопроса
        metadata_elements (list): Метаданные из годового отчета
        search_locations (Optional[List]): Предполагаемые заголовки метаданных из предыдущего анализа

    Returns:
        QuestionAnswer: Структурированный ответ с полями reasoning, data_analysis, answer_type, answer, pages
    """
    # Формируем контекст для модели
    context = f"""
    Compane: {company_name}
    Query: {question_text}
    Type answer: {question_kind}

    Метаданные из годового отчета:
    {json.dumps(metadata_elements, indent=2, ensure_ascii=False)}
    """
    # Добавляем информацию о предполагаемых заголовках, если они присутствуют
    if search_locations:
        context += f"\nIntended metadata headers (not guaranteed, just a guess): {json.dumps(search_locations, indent=2, ensure_ascii=False)}\n"

    try:
        completion = client.beta.chat.completions.parse(
            model="gpt-4o",
            temperature = 0,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": context}
            ],
            response_format=QuestionAnswer,
        )
        return completion.choices[0].message.parsed
    except Exception as e:
        logger.error(f"Ошибка при обработке вопроса: {e}")
        # Возвращаем значение по умолчанию в случае ошибки
        return QuestionAnswer(
            reasoning="Произошла ошибка при обработке вопроса.",
            data_analysis="Не удалось проанализировать данные.",
            answer_type="boolean",
            answer=False,
            pages="N/A"
        )

def process_all_questions(data_file: str) -> dict:
    """
    Обрабатывает вопросы для всех компаний без ограничения по количеству.
    
    Args:
        data_file (str): Путь к файлу с данными (например, enriched_analysis_results.json)
    
    Returns:
        dict: Обновленный словарь с ответами
    """
    
    processed_questions = 0

    # Загружаем данные
    with open(data_file, 'r') as file:
        companies_dict = json.load(file)
    
    # Папка с финальными метаданными
    metadata_folder = 'results/final_agr'
    
    # Маппинг категорий из ответа модели к полям метаданных
    category_mapping = {
        'merger_acquisition': 'mentions_recent_mergers_and_acquisitions',
        'leadership_change': 'has_leadership_changes',
        'layoff': 'has_layoffs',
        'executive_compensation': 'has_executive_compensation',
        'rnd_investment': 'has_rnd_investment_numbers',
        'product_launch': 'has_new_product_launches',
        'capital_expenditure': 'has_capital_expenditures',
        'financial_performance': 'has_financial_performance_indicators',
        'dividend_policy': 'has_dividend_policy_changes',
        'share_buyback': 'has_share_buyback_plans',
        'capital_structure': 'has_capital_structure_changes',
        'risk_factor': 'mentions_new_risk_factors',
        'guidance_update': 'has_guidance_updates',
        'regulatory_litigation': 'has_regulatory_or_litigation_issues',
        'strategic_restructuring': 'has_strategic_restructuring',
        'supply_chain_disruption': 'has_supply_chain_disruptions',
        'esg_initiative': 'has_esg_initiatives'
    }
    
    # Обработка каждой компании
    for sha1, company_info in companies_dict.items():
        if not company_info.get('has_questions', False):
            continue
        
        logger.info("Обработка компании: %s", company_info['company_name'])
        
        # Ищем соответствующий файл метаданных
        metadata_files = glob.glob(f"{metadata_folder}/{sha1}*")
        if not metadata_files:
            logger.warning("Файл метаданных для SHA1 %s не найден", sha1)
            continue
        
        metadata_file = metadata_files[0]
        with open(metadata_file, 'r') as file:
            metadata = json.load(file)
        
        # Обработка каждого вопроса компании
        for question in company_info.get('questions', []):
            # Если в вопросе нет анализа метаданных, пропускаем
            if 'metadata_category' not in question:
                continue
            
            category = question['metadata_category']
            mapped_category = category_mapping.get(category)
            if not mapped_category:
                logger.warning("Категория %s не имеет соответствующего маппинга", category)
                question['metadata_elements'] = []
                continue
            
            # Проверяем наличие категории в метаданных
            if 'meta' in metadata and mapped_category in metadata['meta']:
                category_data = metadata['meta'][mapped_category]
                if isinstance(category_data, dict) and 'elements' in category_data:
                    elements = category_data['elements']
                    detailed_elements = []
                    for item in elements:
                        element_info = {
                            'title': item.get('title', ''),
                            'page': int(item.get('page', 0)),
                            'type': item.get('type', ''),
                            'currency': item.get('currency', 'N/A'),
                            'data': item.get('data', [])
                        }
                        detailed_elements.append(element_info)
                    question['metadata_elements'] = detailed_elements
                    logger.info("Найдено %d элементов для категории %s", len(detailed_elements), category)
                else:
                    logger.warning("Категория %s (mapped: %s) не содержит элементов в ожидаемом формате", category, mapped_category)
                    question['metadata_elements'] = []
            else:
                logger.warning("Категория %s (mapped: %s) не найдена в метаданных", category, mapped_category)
                question['metadata_elements'] = []
            
            # Если имеются метаданные, обрабатываем вопрос через LLM
            if question.get('metadata_elements'):
                search_locations = question.get('search_locations')
                answer = process_question(
                    company_info['company_name'],
                    question.get('text', ''),
                    question.get('kind', ''),
                    question['metadata_elements'],
                    search_locations=search_locations
                )
                question['answer_data_analysis'] = answer.data_analysis
                question['answer_reasoning'] = answer.reasoning
                question['answer_type'] = answer.answer_type
                question['answer_value'] = answer.answer
                question['pages'] = answer.pages
                processed_questions += 1
                logger.info("Обработано вопросов: %d", processed_questions)
            else:
                # Если отсутствуют метаданные и указан тип вопроса (kind)
                if question.get('kind'):
                    logger.info("Отсутствуют метаданные для вопроса: %s. Устанавливаем значение по типу и pdf_sha1 в references.", question.get('text', '')[:50])
                    question['answer_reasoning'] = "Метаданные не найдены для данного вопроса."
                    question['answer_data_analysis'] = "Метаданные отсутствуют."
                    # Если тип boolean, устанавливаем False, иначе "N/A"
                    if question.get('kind') == "boolean":
                        question['answer_value'] = False
                    else:
                        question['answer_value'] = "N/A"
                    question['answer_type'] = question.get('kind')
                    question['pages'] = "N/A"
                    question['references'] = sha1  # pdf_sha1 для данной компании
                    processed_questions += 1
                else:
                    logger.info("Пропуск вопроса без метаданных: %s", question.get('text', '')[:50])
    
    logger.info("Обработка завершена! Всего обработано вопросов: %d", processed_questions)
    return companies_dict


if __name__ == "__main__":
    data_file = "results/all_questions_analysis_results.json"
    
    updated_companies = process_all_questions(data_file)
    
    with open('results/final_results.json', 'w') as file:
        json.dump(updated_companies, file, indent=2, ensure_ascii=False)
    
    logger.info("Результаты сохранены в results/final_results.json")
    print("Результаты сохранены в results/final_results.json")
