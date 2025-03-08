# rag_pipeline/rag_question_analysis_pipeline.py

import json
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import Literal, List
from openai import OpenAI
from prompts.schema_and_prompts import SYSTEM_PROMPT

# Настройка логирования: вывод в консоль и в файл rag_pipeline.log
log_dir = Path("LOGS")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "rag_question_analysis_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("rag_question_analysis_pipeline")

# Инициализация клиента OpenAI (замените "****" на реальный API-ключ или используйте переменные окружения)
client = OpenAI(api_key = "")

class QuestionAnalysis(BaseModel):
    metadata_category: Literal[
        "merger_acquisition", "leadership_change", "layoff", "executive_compensation",
        "rnd_investment", "product_launch", "capital_expenditure", "financial_performance",
        "dividend_policy", "share_buyback", "capital_structure", "risk_factor",
        "guidance_update", "regulatory_litigation", "strategic_restructuring",
        "supply_chain_disruption", "esg_initiative"
    ] = Field(..., description="Выберите наиболее подходящую категорию метаданных для заданного вопроса")
    currency: Literal[
        "USD", "EUR", "AUD", "CAD", "GBP", "ZAR", "RUB", "INR", "JPY", "CNY",
        "NOK", "BRL", "RMB", "N/A"
    ] = Field(..., description="Связан с денежными значениями, определите используемую валюту")
    search_locations: List[str] = Field(..., description="Определите примерно в каких заголовках документов искать ответ на вопрос (разделы, заголовки, типы данных)")

SYSTEM_PROMPTS = SYSTEM_PROMPT

def analyze_question(question_text: str) -> QuestionAnalysis:
    """
    Анализирует вопрос с помощью модели LLM и возвращает структурированный ответ.

    Args:
        question_text (str): Текст вопроса для анализа

    Returns:
        QuestionAnalysis: Структурированный результат анализа
    """
    try:
        completion = client.beta.chat.completions.parse(
            model="gpt-4o-2024-11-20",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPTS},
                {"role": "user", "content": question_text}
            ],
            response_format=QuestionAnalysis,
        )
        return completion.choices[0].message.parsed
    except Exception as e:
        logger.error(f"Ошибка при анализе вопроса: {e}")
        raise e

class QuestionAnalysisPipeline:
    def __init__(self, companies_file: str, output_file: str):
        """
        Инициализация конвейера анализа вопросов.

        Args:
            companies_file (str): Путь к файлу с данными компаний и вопросами
            output_file (str): Путь для сохранения результатов анализа
        """
        self.companies_file = companies_file
        self.output_file = output_file

    def load_companies(self) -> dict:
        logger.info("Загрузка данных компаний из файла: %s", self.companies_file)
        with open(self.companies_file, 'r') as file:
            companies = json.load(file)
        return companies

    def process_questions(self, companies: dict) -> dict:
        """
        Обрабатывает все вопросы для всех компаний.

        Args:
            companies (dict): Словарь с информацией о компаниях и вопросами

        Returns:
            dict: Обновлённый словарь компаний с результатами анализа вопросов
        """
        logger.info("Начало анализа вопросов для всех компаний.")
        for sha1, company_info in companies.items():
            company_name = company_info.get('company_name', 'Unknown')
            logger.info("Обработка компании: %s", company_name)
            for i, question in enumerate(company_info.get('questions', [])):
                question_text = question.get('text', '')
                if question_text:
                    try:
                        analysis = analyze_question(question_text)
                        question['metadata_category'] = analysis.metadata_category
                        question['currency'] = analysis.currency
                        question['search_locations'] = analysis.search_locations
                        logger.info("Вопрос %d обработан: категория - %s, валюта - %s", i+1, analysis.metadata_category, analysis.currency)
                    except Exception as e:
                        logger.error("Ошибка при обработке вопроса для компании %s: %s", company_name, e)
        logger.info("Анализ вопросов завершён.")
        return companies

    def save_results(self, companies: dict):
        logger.info("Сохранение результатов в файл: %s", self.output_file)
        with open(self.output_file, 'w') as file:
            json.dump(companies, file, indent=2, ensure_ascii=False)
        logger.info("Результаты сохранены.")

    def run(self):
        companies = self.load_companies()
        processed_companies = self.process_questions(companies)
        self.save_results(processed_companies)

if __name__ == "__main__":
    companies_file = 'results/companies_with_questions.json'
    output_file = 'results/all_questions_analysis_results.json'

    pipeline = QuestionAnalysisPipeline(companies_file, output_file)
    pipeline.run()
