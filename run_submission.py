# rag_pipeline/rag_submission.py

from typing import Union, List, Literal
from pydantic import BaseModel, Field
import json

# Определяем модели Pydantic в соответствии с требуемой схемой
class SourceReference(BaseModel):
    pdf_sha1: str = Field(..., description="SHA1 hash of the PDF file")
    page_index: int = Field(..., description="Physical page number in the PDF file")

class Answer(BaseModel):
    question_text: str = Field(..., description="Text of the question")
    kind: Literal["number", "name", "boolean", "names"] = Field(..., description="Kind of the question")
    value: Union[float, str, bool, List[str], Literal["N/A"]] = Field(..., description="Answer to the question, according to the question schema")
    references: List[SourceReference] = Field([], description="References to the source material in the PDF file")

class AnswerSubmission(BaseModel):
    answers: List[Answer] = Field(..., description="List of answers to the questions")
    team_email: str = Field(..., description="Email that your team used to register for the challenge")
    submission_name: str = Field(..., description="Unique name of the submission (e.g. experiment name)")

# Функция для преобразования данных из final_results в формат AnswerSubmission
def convert_to_submission_format(final_results_file: str) -> AnswerSubmission:
    # Загружаем данные из файла
    with open(final_results_file, 'r') as file:
        final_results = json.load(file)
    
    # Список для хранения ответов
    answers_list = []
    
    # Проходим по каждой компании
    for sha1, company_info in final_results.items():
        if not company_info.get('has_questions', False):
            continue
        
        # Проходим по каждому вопросу
        for question in company_info['questions']:
            # Получаем текст вопроса и тип
            question_text = question['text']
            kind = question.get('kind')
            
            # Получаем значение ответа
            value = question.get('answer_value', "N/A")
            
            # Преобразуем тип ответа если необходимо
            if kind == "boolean" and isinstance(value, str):
                if value.lower() == "true":
                    value = True
                elif value.lower() == "false":
                    value = False
            elif kind == "number" and isinstance(value, str):
                try:
                    value = float(value)
                except:
                    value = "N/A"
            
            # Получаем ссылку на страницу из ответа модели (используем только значение поля 'pages')
            references = []
            page_value = question.get('pages')
            if isinstance(page_value, int) and page_value != 0:
                references.append(SourceReference(pdf_sha1=sha1, page_index=page_value))
            else:
                references.append(SourceReference(pdf_sha1=sha1, page_index=0))
            
            # Создаем объект Answer
            answer = Answer(
                question_text=question_text,
                kind=kind,
                value=value,
                references=references
            )
            
            # Добавляем в список ответов
            answers_list.append(answer)
    
    # Создаем объект AnswerSubmission
    submission = AnswerSubmission(
        answers=answers_list,
        team_email="aangers07@gmail.com",
        submission_name="dizilx"
    )
    
    return submission

# Точка входа
if __name__ == "__main__":
    # Путь к файлу с результатами
    final_results_file = "results/final_results.json"
    
    # Преобразуем данные
    submission = convert_to_submission_format(final_results_file)
    
    # Сохраняем результаты в JSON
    with open('submission.json', 'w') as file:
        file.write(submission.model_dump_json(indent=2))
    
    print("Результаты преобразованы и сохранены в submission.json")
