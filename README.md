# Recipes → SQLite (ETL + SQL)

Невеликий навчальний проєкт: імпорт CSV з рецептами у **SQLite** та виконання кількох SQL‑запитів.

## Структура

```
recipes-sqlite/
├─ src/
│  └─ main.py
├─ data/
│  └─ recipes.csv
├─ outputs/
│  └─ recipes.db         # з'явиться після запуску
└─ requirements.txt
```

## Запуск

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt

python src/main.py
# або з параметрами:
python src/main.py --csv data/recipes.csv --db outputs/recipes.db --table recipes
```

> Скрипт пробує кодування: utf-8, utf-8-sig, latin1, cp1251. Назви колонок нормалізуються: пробіли → '\_', нижній регістр.
