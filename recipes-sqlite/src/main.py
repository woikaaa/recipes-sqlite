#!/usr/bin/env python3
import argparse
import sqlite3
import pandas as pd
from pathlib import Path

def read_csv_with_fallback(path: Path):
    for enc in ["utf-8", "utf-8-sig", "latin1", "cp1251"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    raise RuntimeError("Не вдалося прочитати CSV у поширених кодуваннях.")

def create_database(csv_path: Path, db_path: Path, table_name: str = "recipes") -> None:
    df = read_csv_with_fallback(csv_path)
    # нормалізуємо назви колонок
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    conn = sqlite3.connect(db_path)
    try:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        print("Таблиці в базі:", [t[0] for t in cursor.fetchall()])
    finally:
        conn.close()
    print(f"Базу даних створено! -> {db_path} (таблиця: {table_name})")

def run_queries(db_path: Path, table_name: str = "recipes") -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("\n=== Результати запитів ===")

    # 1. Перші 10 рецептів
    try:
        cursor.execute(f"SELECT recipe_name, cuisine FROM {table_name} LIMIT 10")
        print("\n1. Перші 10 рецептів:")
        for row in cursor.fetchall():
            print(f"- {row[0]} ({row[1]})")
    except sqlite3.OperationalError:
        print("\n1. Колонок 'recipe_name'/'cuisine' не знайдено — пропускаю.")

    # 2. Унікальні кухні
    try:
        cursor.execute(f"SELECT DISTINCT cuisine FROM {table_name}")
        print("\n2. Унікальні кухні:")
        print([row[0] for row in cursor.fetchall()])
    except sqlite3.OperationalError:
        print("\n2. Колонки 'cuisine' не знайдено — пропускаю.")

    # 3. Вегетаріанські рецепти
    try:
        cursor.execute(
            f"""
            SELECT recipe_name, dietary_restrictions
            FROM {table_name}
            WHERE dietary_restrictions LIKE '%vegetarian%'
            LIMIT 5
        """
        )
        print("\n3. Приклади вегетаріанських рецептів:")
        for row in cursor.fetchall():
            print(f"- {row[0]} ({row[1]})")
    except sqlite3.OperationalError:
        print("\n3. Колонки 'dietary_restrictions' не знайдено — пропускаю.")

    # 4. К-ть рецептів
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        print(f"\n4. Всього рецептів: {cursor.fetchone()[0]}")
    except sqlite3.OperationalError:
        print("\n4. Неможливо порахувати — таблиці/колонки немає.")

    # 5. Найдовші рецепти
    try:
        cursor.execute(
            f"""
            SELECT recipe_name, cooking_time_minutes
            FROM {table_name}
            WHERE cooking_time_minutes IS NOT NULL
            ORDER BY CAST(cooking_time_minutes AS REAL) DESC
            LIMIT 5
        """
        )
        print("\n5. Найдовші рецепти:")
        for row in cursor.fetchall():
            print(f"- {row[0]} ({row[1]} хв)")
    except sqlite3.OperationalError:
        print("\n5. Колонки 'cooking_time_minutes' не знайдено — пропускаю.")

    # 6. Групування
    try:
        cursor.execute(
            f"""
            SELECT cuisine, COUNT(*) as count
            FROM {table_name}
            GROUP BY cuisine
            HAVING count > 10
            ORDER BY count DESC
        """
        )
        print("\n6. Кількість рецептів по кухнях (>10):")
        for row in cursor.fetchall():
            print(f"- {row[0]}: {row[1]} рецептів")
    except sqlite3.OperationalError:
        print("\n6. Немає необхідних колонок — пропускаю.")

    # 7. Статистика калорій
    try:
        cursor.execute(
            f"""
            SELECT
                AVG(calories_per_serving) as avg_calories,
                MIN(calories_per_serving) as min_calories,
                MAX(calories_per_serving) as max_calories
            FROM {table_name}
            WHERE calories_per_serving > 0
        """
        )
        stats = cursor.fetchone()
        print("\n7. Статистика калорій:")
        if stats and stats[0] is not None:
            print(f"Середнє: {stats[0]:.1f}, Мінімум: {stats[1]}, Максимум: {stats[2]}")
        else:
            print("Даних недостатньо або немає колонки 'calories_per_serving'.")
    except sqlite3.OperationalError:
        print("\n7. Колонки 'calories_per_serving' не знайдено — пропускаю.")

    # 8. Топ-5 кухонь за калорійністю
    try:
        cursor.execute(
            f"""
            SELECT
                cuisine,
                AVG(calories_per_serving) as avg_calories
            FROM {table_name}
            WHERE calories_per_serving > 0
            GROUP BY cuisine
            ORDER BY avg_calories DESC
            LIMIT 5
        """
        )
        print("\n8. Топ-5 найкалорійніших кухонь:")
        for row in cursor.fetchall():
            print(f"- {row[0]}: {row[1]:.1f} калорій в середньому")
    except sqlite3.OperationalError:
        print("\n8. Не вистачає потрібних колонок — пропускаю.")

    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Імпорт CSV у SQLite та запуск прикладних запитів.")
    parser.add_argument("--csv", type=Path, default=Path("data/recipes.csv"))
    parser.add_argument("--db", type=Path, default=Path("outputs/recipes.db"))
    parser.add_argument("--table", type=str, default="recipes")
    parser.add_argument("--skip-queries", action="store_true")
    args = parser.parse_args()

    args.db.parent.mkdir(parents=True, exist_ok=True)
    create_database(args.csv, args.db, args.table)
    if not args.skip_queries:
        run_queries(args.db, args.table)

if __name__ == "__main__":
    main()
