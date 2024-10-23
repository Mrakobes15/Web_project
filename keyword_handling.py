import SQLTable as S
import concurrent.futures
import time
import mysql.connector


def update_or_insert_keyword_association(associations_table, keyword1_id, keyword2_id, retry_attempts=3, retry_delay=1):
    """
    Обновляет или вставляет новую запись в таблицу ассоциаций ключевых слов с обработкой дедлоков и переподключением.
    В случае дедлока или потери соединения выполняется повторная попытка до указанного числа попыток.

    :param associations_table: Объект таблицы ассоциаций.
    :param keyword1_id: ID первого ключевого слова.
    :param keyword2_id: ID второго ключевого слова.
    :param retry_attempts: Количество попыток при дедлоке или потере соединения.
    :param retry_delay: Задержка между попытками (в секундах).
    """
    # Преобразуем keyword_id в стандартные int, чтобы избежать ошибки с int64
    keyword1_id = int(keyword1_id)
    keyword2_id = int(keyword2_id)

    attempt = 0
    while attempt < retry_attempts:
        try:
            # Проверяем, активно ли соединение перед выполнением запроса
            if not associations_table.connection.is_connected():
                print("Connection lost. Reconnecting...")
                associations_table.connection.reconnect(attempts=3, delay=5)

            print(f"Attempting to update/insert association between {keyword1_id} and {keyword2_id}")

            # Используем буферизированный курсор для выполнения SELECT-запроса
            cursor = associations_table.connection.cursor(buffered=True)

            # Проверка, существует ли уже эта пара в таблице
            select_query = f"""
            SELECT association_strength FROM {associations_table.table_name}
            WHERE keyword_id_1 = %s AND keyword_id_2 = %s
            """
            cursor.execute(select_query, (keyword1_id, keyword2_id))
            result = cursor.fetchone()

            if result:
                current_strength = result[0]
                print(f"Current strength before update: {current_strength}")
                new_strength = current_strength + 1
                print(f"Updating strength to {new_strength}")

                # Выполняем обновление записи
                update_query = f"""
                UPDATE {associations_table.table_name}
                SET association_strength = %s
                WHERE keyword_id_1 = %s AND keyword_id_2 = %s
                """
                cursor.execute(update_query, (new_strength, keyword1_id, keyword2_id))
            else:
                print(f"Inserting new association between {keyword1_id} and {keyword2_id}")
                # Вставляем новую запись
                insert_query = f"""
                INSERT INTO {associations_table.table_name} (keyword_id_1, keyword_id_2, association_strength)
                VALUES (%s, %s, %s)
                """
                cursor.execute(insert_query, (keyword1_id, keyword2_id, 1))

            # Фиксация транзакции
            associations_table.connection.commit()
            print(f"Transaction committed successfully for association between {keyword1_id} and {keyword2_id}")

            # Закрываем курсор
            cursor.close()

            # Выход из цикла, если все прошло успешно
            break

        except mysql.connector.Error as err:
            # Если произошла блокировка или дедлок, выполняем повторную попытку
            if err.errno == 1213:  # Deadlock
                print(f"Deadlock detected, retrying... (Attempt {attempt + 1} of {retry_attempts})")
                attempt += 1
                associations_table.connection.rollback()
                time.sleep(retry_delay)
            elif err.errno in [2006, 2013, 10038, 10054, 10057]:
                print(f"Connection error detected: {err}. Retrying... (Attempt {attempt + 1} of {retry_attempts})")
                attempt += 1
                associations_table.connection.rollback()
                time.sleep(retry_delay)
            else:
                # Если ошибка другая, выводим её и прерываем процесс
                print(f"Failed to update or insert association: {err}")
                associations_table.connection.rollback()
                break

        finally:
            if attempt == retry_attempts:
                print(f"Failed to insert or update after {retry_attempts} attempts. Terminating the process.")
                break


def process_keywords_in_articles(db_config, articles_table_name, keywords_table_name, associations_table_name):
    """
    Последовательная обработка статей и ключевых слов, выявление их ассоциаций и обновление таблицы ассоциаций.
    """
    # Инициализация таблиц
    articles_table = S.SQLTable(db_config, articles_table_name)
    keywords_table = S.SQLTable(db_config, keywords_table_name)
    associations_table = S.SQLTable(db_config, associations_table_name)

    # Извлечение всех статей и ключевых слов
    articles_df = articles_table.fetch_all()
    keywords_df = keywords_table.fetch_all()

    # Перебор всех статей
    for _, article_row in articles_df.iterrows():
        article_text = article_row['abstract']  # Предполагается, что текст статьи находится в колонке 'text'
        print(f"Processing article ID {article_row['id']}")

        # Перебор всех пар ключевых слов
        for i in range(len(keywords_df)):
            for j in range(i + 1, len(keywords_df)):  # Проверка каждой уникальной пары (i, j)
                keyword1 = keywords_df.loc[i, 'keyword']
                keyword2 = keywords_df.loc[j, 'keyword']
                keyword1_id = keywords_df.loc[i, 'id']
                keyword2_id = keywords_df.loc[j, 'id']

                # Проверка наличия ключевых слов в статье
                if keyword1 in article_text and keyword2 in article_text:
                    print(f"Keywords '{keyword1}' and '{keyword2}' found in article ID {article_row['id']}")
                    # Обновление или вставка ассоциации
                    update_or_insert_keyword_association(associations_table, keyword1_id, keyword2_id)