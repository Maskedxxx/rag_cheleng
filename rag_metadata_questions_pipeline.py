import json
import logging
from pathlib import Path


# Настройка логирования: вывод в консоль и в файл rag_pipeline.log
log_dir = Path("LOGS")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "rag_metadata_questions_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("rag_metadata_questions_pipeline")


class MetadataQuestionsPipeline:
    def __init__(self, metadata_path: str, questions_path: str, output_path: str):
        """
        Инициализация конвейера с указанием путей к файлам метаданных, вопросов и выходного файла.
        """
        self.metadata_path = metadata_path
        self.questions_path = questions_path
        self.output_path = output_path

    def check_questions_coverage(self) -> tuple:
        """
        Проверяет, все ли вопросы соотносятся с какой-либо компанией.
        
        Возвращает:
            tuple: (все вопросы, сопоставленные вопросы, несопоставленные вопросы)
        """
        logger.info("Проверка покрытия вопросов по метаданным.")
        # Загружаем метаданные и формируем список компаний
        with open(self.metadata_path, 'r') as file:
            metadata = json.load(file)

        company_names = []
        for file_info in metadata.values():
            if 'meta' in file_info and 'company_name' in file_info['meta']:
                company_names.append(file_info['meta']['company_name'])

        # Загружаем вопросы
        with open(self.questions_path, 'r') as file:
            questions = json.load(file)

        matched_questions = []
        unmatched_questions = []

        for question in questions:
            matched = False
            for company in company_names:
                if company in question.get('text', ''):
                    matched = True
                    matched_questions.append((question, company))
                    break
            if not matched:
                unmatched_questions.append(question)

        logger.info("Проверка завершена: сопоставлено вопросов: %d, не сопоставлено: %d",
                    len(matched_questions), len(unmatched_questions))
        return questions, matched_questions, unmatched_questions

    @staticmethod
    def extract_company_info(file_path: str) -> dict:
        """
        Извлекает поля sha1 и company_name из JSON файла и сохраняет их в словарь.
        
        Аргументы:
            file_path (str): Путь к JSON файлу.
            
        Возвращает:
            dict: Словарь с ключами sha1 и значениями с информацией о компании.
        """
        logger.info("Извлечение информации о компаниях из файла: %s", file_path)
        result = {}
        with open(file_path, 'r') as file:
            data = json.load(file)

        for file_name, file_info in data.items():
            sha1 = file_info.get('sha1')
            company_name = file_info.get('meta', {}).get('company_name')
            if sha1 and company_name:
                result[sha1] = {'company_name': company_name}
        logger.info("Извлечено информации о %d компаниях", len(result))
        return result

    @staticmethod
    def add_questions_to_companies(companies_dict: dict, questions_path: str) -> dict:
        """
        Дополняет словарь компаний связанными вопросами из файла вопросов.
        
        Аргументы:
            companies_dict (dict): Словарь с информацией о компаниях.
            questions_path (str): Путь к файлу с вопросами.
            
        Возвращает:
            dict: Обновленный словарь компаний с прикреплёнными вопросами.
        """
        logger.info("Дополнение компаний вопросами из файла: %s", questions_path)
        with open(questions_path, 'r') as file:
            questions = json.load(file)

        for sha1, company_info in companies_dict.items():
            company_name = company_info['company_name']
            company_info['questions'] = []
            for question in questions:
                if company_name in question.get('text', ''):
                    company_info['questions'].append({
                        'text': question.get('text', ''),
                        'kind': question.get('kind', '')
                    })
            company_info['has_questions'] = len(company_info['questions']) > 0
        logger.info("Дополнение вопросов завершено.")
        return companies_dict

    def run(self):
        """
        Запускает конвейер: извлекает информацию о компаниях, добавляет вопросы и сохраняет результат.
        """
        logger.info("Запуск конвейера вопросов по метаданным.")
        companies_dict = self.extract_company_info(self.metadata_path)
        companies_with_questions = self.add_questions_to_companies(companies_dict, self.questions_path)

        # Выводим результат в консоль и сохраняем в файл
        output_json = json.dumps(companies_with_questions, indent=2, ensure_ascii=False)
        print(output_json)
        with open(self.output_path, 'w') as f:
            json.dump(companies_with_questions, f, indent=2, ensure_ascii=False)
        logger.info("Результаты сохранены в файл: %s", self.output_path)


if __name__ == "__main__":
    # Пути к файлам
    metadata_path = 'results/selected_metadata.json'
    questions_path = 'data/questions.json'
    output_path = 'result/companies_with_questions.json'

    pipeline = MetadataQuestionsPipeline(metadata_path, questions_path, output_path)
    pipeline.run()
