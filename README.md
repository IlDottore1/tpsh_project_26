
# Бот для обработки статистики видео

Этот проект представляет собой Telegram-бота, который по текстовым запросам на русском возвращает одно число - ответ на поставленный вопрос, полученный на основе статистики по видео из JSON-данных, хранящихся в PostgreSQL.  

Проект использует **Docker Compose**, **asyncpg**, **aiogram** и **LLM (GigaChat)** для преобразования естественного языка в безопасные SQL-запросы.


---

## ✨ Локальный запуск

1. Клонировать репозиторий

```
git clone <repo-url>
cd project-root
````

2. Создать файл `.env` на основе `.env_example` и заполнить своими значениями

```
TELEGRAM_BOT_TOKEN=...
GIGACHAT_CLIENT_ID=...
GIGACHAT_CLIENT_SECRET=...
GIGACHAT_API_URL=https://gigachat.devices.sberbank.ru/api/v1/chat/completions
GIGACHAT_AUTH_URL=https://ngw.devices.sberbank.ru:9443/api/v2/oauth
```
GIGACHAT_API_URL И GIGACHAT_AUTH_URL заполнены для образца. Ниже указана подробная инструкция по получению токенов бота, client_id и client_secret

3. Запустить контейнеры Docker Compose

```
docker-compose up --build
```
Если на этом этапе возникла ошибка, попробуйте перезапустить терминал и ввести эту команду вновь. Иногда возникают проблемы со стороны Docker Desktop

4. Теперь бот готов к работе. Отправляйте сообщения в Telegram на русском языке, бот вернёт одно число

---

## ✨ Архитектура и подход

1. **Бот (app/main.py)**

   * Получает текстовый запрос
   * Отправляет запрос в **LLM GigaChat** с промптом `nl2json_prompt.txt`.
   * Получает JSON-инструкцию с полями:

     ```json
     {
       "intent": "<count|sum|sum_delta|count_distinct|other>",
       "target": "<videos|video_snapshots>",
       "column": "<колонка для агрегации>",
       "aggregate": "<count|sum>",
       "filters": { ... }
     }
     ```
   * На основе JSON формируется безопасный параметризованный SQL через функцию `sql_templates.build_query`.
   * Выполняется SQL через asyncpg, результат отправляется пользователю.


2. **База данных (PostgreSQL)**

   * Таблицы `videos` и `video_snapshots` с индексами для быстрого поиска.
   * JSON с видео загружается через `loader.py`.


3. **SQL генерация**

   * Функция `build_query` преобразует JSON в SQL с учётом:

     * intent (`count`, `sum`, `sum_delta`, `count_distinct`);
     * target таблицы (`videos` или `video_snapshots`);
     * фильтров по дате, creator_id, сравнениям и относительным периодам (`period_hours`).
   * Используются безопасные параметры (`$1, $2...`) для защиты от SQL-инъекций.

---

## ✨ LLM (GigaChat)

* **Промпт**: `app/nl2json_prompt.txt`

  * Содержит схему таблиц и правила преобразования вопросов на русском языке в JSON.
  * Обеспечивает:

    * валидный JSON без пояснений;
    * стандартизацию полей `intent`, `target`, `column`, `aggregate`, `filters`;
    * обработку дат и сравнений;
    * поддержку периодов (`period_hours`) для агрегирования изменений за первые N часов после публикации.

* **Пример промпта** (сокращённо):

```
Ты — помощник, который превращает вопрос о статистике видео в JSON.
...
Верни только JSON:
{
  "intent": "<count|sum|sum_delta|count_distinct|other>",
  "target": "<videos|video_snapshots>",
  "column": "<колонка>",
  "aggregate": "<count|sum>",
  "filters": {
     "creator_id": "<значение или null>",
     "date_from": "<YYYY-MM-DD или null>",
     "date_to": "<YYYY-MM-DD или null>",
     "date_field": "<video_created_at|created_at>",
     "comparison": "<опционально>",
     "period_hours": "<опционально>",
     "period_anchor": "<опционально>"
  }
}
```

* LLM гарантирует корректное сопоставление запроса к таблицам и колонкам базы. Промпт построен так, чтобы запрос обработался верно и не было конфликтов

---

## ✨ Получение всех данных для .env
1. Получение токена бота

- Перейдите в Telegram
- Вбейте в поиск @BotFather и отправьте ему сообщение /start
- Затем отправьте /newbot
- Вам будет предложенно выбрать имя бота и его username
- После этого бот отправит сообщение с токеном вашего бота. Его необходимо скопировать в поле TELEGRAM_BOT_TOKEN в файле .env

2. Получение данных из GigaChat
- Перейдите на сайт 
[https://developers.sber.ru/portal/products/gigachat-ap](https://developers.sber.ru/portal/products/gigachat-api)i
- В правом верхнем углу нажмите кнопку **Личный кабинет**
- Создайте новый аккаунт или войдите в существующий
- Нажмите **Создать проект** и выберите ```GigaChat API```
- Перейдите в **Настройки API** (на панели навигации слева, иконка ключа)
- Нажмите **Получить новый ключ** и кликните на выпадающее меню

- Скопируйте полученные данные и вставьте их в соответствующие поля
