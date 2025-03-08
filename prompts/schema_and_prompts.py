# prompts/schema_and_prompts.py

schema_1 = {
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "description": """Тип извлекаемых метаданных из документа. Возможные значения:
                        - merger_acquisition: Метаданные о слияниях и поглощениях в деловых отчетах компаний и банков
                        - leadership_change: Метаданные об изменениях в руководстве, и об самом нынешнем! руководстве в деловых отчетах компаний и банков
                        - layoff: Метаданные о сокращениях персонала в деловых отчетах компаний и банков
                        - executive_compensation: Метаданные о вознаграждениях (премия) руководителей компаний в деловых отчетах компаний и банков
                        - rnd_investment: Метаданные об инвестициях в исследования и разработки в деловых отчетах компаний и банков
                        - product_launch: Метаданные о запуске новых продуктов в деловых отчетах компаний и банков
                        - capital_expenditure: Метаданные о капитальных затратах в деловых отчетах компаний и банков
                        - financial_performance: Метаданные о финансовых показателях в деловых отчетах компаний и банков
                        - dividend_policy: Метаданные об изменениях в дивидендной политике в деловых отчетах компаний и банков
                        - share_buyback: Метаданные о программе обратного выкупа акций в деловых отчетах компаний и банков
                        - capital_structure: Метаданные об изменениях в структуре капитала в деловых отчетах компаний и банков
                        - risk_factor: Метаданные о новых факторах риска в деловых отчетах компаний и банков
                        - guidance_update: Метаданные об обновлении прогнозов в деловых отчетах компаний и банков
                        - regulatory_litigation: Метаданные о регуляторных или судебных проблемах в деловых отчетах компаний и банков
                        - strategic_restructuring: Метаданные о стратегической реструктуризации в деловых отчетах компаний и банков
                        - supply_chain_disruption: Метаданные о сбоях в цепочке поставок в деловых отчетах компаний и банков
                        - esg_initiative: Метаданные об инициативах ESG в деловых отчетах компаний и банков (экология, социальная ответственность, управление)""",
                    "enum": [
                        "merger_acquisition",
                        "leadership_change",
                        "layoff",
                        "executive_compensation",
                        "rnd_investment",
                        "product_launch",
                        "capital_expenditure",
                        "financial_performance",
                        "dividend_policy",
                        "share_buyback",
                        "capital_structure",
                        "risk_factor",
                        "guidance_update",
                        "regulatory_litigation",
                        "strategic_restructuring",
                        "supply_chain_disruption",
                        "esg_initiative"
                    ]
                },
                "entity": {
                    "type": "object",
                    "description": "Базовая структура для данных документа",
                    "properties": {
                        "documents": {
                            "type": "array",
                            "description": "Массив документов с извлеченными данными",
                            "items": {
                                "type": "object",
                                "description": "Структура отдельного документа",
                                "properties": {
                                    "page": {
                                        "type": "integer",
                                        "description": "Номер страницы документа, на которой найдена информация"
                                    },
                                    "title": {
                                        "type": "string",
                                        "description": "Заголовок или краткое описание блока информации"
                                    },
                                    "data": {
                                        "type": "array",
                                        "description": "СПИСОК List Словарей точных данных с ключами и значениями `key` `value` (`key` загаловки данных, `value` точные значения), извлеченными из документа. ЕСЛИ ДАННЫЕ ПРЕДСТАВЛЯЮТ СОБОЙ АБСТРАКТНЫЙ ТЕКСТ (ПРЕДИСЛОВИЕ, ПОСЛЕСЛОВИЕ, СОДЕРЖАНИЕ, ТО ВОЗВРАЩАЙТЕ ПРОСТО ВЫЖИМКУ В ОДНОМ `key` `value`). ЗАПОЛНЯЙТЕ ВСЕ ДАННЫЕ КОТОРЫЕ ИМЕЮТ ЦЕННОСТЬ ЭТО ВАЖНО, НЕ ПРОПУСКАЙТЕ",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "key": {
                                                    "type": "string",
                                                    "description": "Заголовок или название данных"
                                                },
                                                "value": {
                                                    "type": "string",
                                                    "description": "Значение данных"
                                                }
                                            },
                                            "required": ["key", "value"],
                                            "additionalProperties": False
                                        }
                                    },
                                    "currency": {
                                        "type": "string",
                                        "description": "Валюта, используемая в денежных значениях в документе. Верните N/A если не уверены в значении валюты и где нет пояснения в валюте к финансовым значениям",
                                        "enum": [
                                            "USD", "EUR", "AUD", "CAD", "GBP",
                                            "ZAR", "RUB", "INR", "JPY", "CNY",
                                            "NOK", "BRL", "RMB", "N/A"
                                        ]
                                    }
                                },
                                "required": ["page", "title", "data", "currency"],
                                "additionalProperties": False
                            }
                        }
                    },
                    "required": ["documents"],
                    "additionalProperties": False
                }
            },
            "required": ["type", "entity"],
            "additionalProperties": False
        }

system_prompts = (
        "You are an expert document analyzer specializing in extracting structured metadata from business documents. "
        "Your task is to analyze PDF document content and extract key information according to a predefined schema to facilitate document search and classification. "
        "Select the most relevant schema based on the content of the page. Below are the available schemas and their descriptions: \n\n"
        
        "- **merger_acquisition**: Metadata on mergers and acquisitions in business reports of companies and banks.\n"
        "- **leadership_change**: Metadata on leadership changes and the current! leadership in business reports of companies and banks.\n"
        "- **layoff**: Metadata on workforce reductions in business reports of companies and banks.\n"
        "- **executive_compensation**: Metadata on executive compensation (bonuses) in business reports of companies and banks.\n"
        "- **rnd_investment**: Metadata on investments in research and development in business reports of companies and banks.\n"
        "- **product_launch**: Metadata on new product launches in business reports of companies and banks.\n"
        "- **capital_expenditure**: Metadata on capital expenditures in business reports of companies and banks.\n"
        "- **financial_performance**: Metadata on financial performance indicators in business reports of companies and banks.\n"
        "- **dividend_policy**: Metadata on changes in dividend policy in business reports of companies and banks.\n"
        "- **share_buyback**: Metadata on share repurchase programs in business reports of companies and banks.\n"
        "- **capital_structure**: Metadata on changes in capital structure in business reports of companies and banks.\n"
        "- **risk_factor**: Metadata on new risk factors in business reports of companies and banks.\n"
        "- **guidance_update**: Metadata on forecast updates in business reports of companies and banks.\n"
        "- **regulatory_litigation**: Metadata on regulatory or litigation issues in business reports of companies and banks.\n"
        "- **strategic_restructuring**: Metadata on strategic restructuring in business reports of companies and banks.\n"
        "- **supply_chain_disruption**: Metadata on supply chain disruptions in business reports of companies and banks.\n"
        "- **esg_initiative**: Metadata on ESG (Environmental, Social, and Governance) initiatives in business reports of companies and banks.\n"
    )


SYSTEM_PROMPT = """
You are an expert analyst specializing in the analysis of company business reports.
Your task is to determine which metadata category a given question belongs to, and identify the currency (if applicable).

Metadata categories:
- merger_acquisition: Metadata about mergers and acquisitions in business reports of companies and banks.
- leadership_change: Metadata about leadership changes and current leadership in business reports of companies and banks.
- layoff: Metadata about workforce reductions in business reports of companies and banks.
- executive_compensation: Metadata about executive compensation (bonuses) in business reports of companies and banks.
- rnd_investment: Metadata about investments in research and development in business reports of companies and banks.
- product_launch: Metadata about new product launches in business reports of companies and banks.
- capital_expenditure: Metadata about capital expenditures in business reports of companies and banks.
- financial_performance: Metadata about financial performance indicators in business reports of companies and banks.
- dividend_policy: Metadata about changes in dividend policy in business reports of companies and banks.
- share_buyback: Metadata about share buyback programs in business reports of companies and banks.
- capital_structure: Metadata about changes in capital structure in business reports of companies and banks.
- risk_factor: Metadata about new risk factors in business reports of companies and banks.
- guidance_update: Metadata about forecast updates in business reports of companies and banks.
- regulatory_litigation: Metadata about regulatory issues or litigation in business reports of companies and banks.
- strategic_restructuring: Metadata about strategic restructuring in business reports of companies and banks.
- supply_chain_disruption: Metadata about supply chain disruptions in business reports of companies and banks.
- esg_initiative: Metadata about ESG (Environmental, Social, and Governance) initiatives in business reports of companies and banks.

Select the most appropriate metadata category for the given question.
If the question is related to monetary values, determine the currency used (USD, EUR, AUD, CAD, GBP, ZAR, RUB, INR, JPY, CNY, NOK, BRL, RMB).
If currency is not specified or not applicable, use "N/A".
Provide reasoning about which document headings to look for to answer the question (sections, headings, data types)
"""