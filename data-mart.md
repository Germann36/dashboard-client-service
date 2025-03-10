# Витрина данных для дашборда «Аналитика отдела клиентского сервиса за 2019 год»

**Задача**: создать витрину данных для дашборда, который отображает ключевые метрики отдела, такие как время ответа на заявки, количество обработанных заявок и другие показатели эффективности работы.

---

### Шаг 1: Создание временной таблицы с источниками клиентов

Для начала создадим временную таблицу, содержащую список источников, через которые клиенты обращаются в сервис.

```sql
CREATE TEMPORARY TABLE source_list (name_source TEXT);

INSERT INTO source_list (name_source) 
VALUES ('source 1'), ('source 2'), ('source 3'), ('source 4');
```

---

### Шаг 2: Создание представления для первого ответа клиенту

Создадим представление, которое фиксирует информацию о первом ответе менеджера клиенту. Это важно для расчета времени ответа.

```sql
CREATE OR REPLACE VIEW v_response_first AS
WITH all_response AS (
    SELECT 
        id_client,
        date_request,
        date_response,
        manager,
        ROW_NUMBER() OVER (PARTITION BY id_client ORDER BY date_response ASC) AS rn
    FROM all_raw
    WHERE 
        action_manager = 'response'
        AND source_client IN (SELECT name_source FROM source_list)
),
first_response AS (
    SELECT 
        *,
        EXTRACT(dow FROM date_request) AS dow,
        date_request::date AS date_req,
        date_request::time AS time_req
    FROM all_response
    WHERE rn = 1
),
```

---

### Шаг 3: Перенос заявки на рабочее время

Согласно бизнес-правилам, отсчет времени ответа (SLA) начинается с начала следующей рабочей смены, а не с момента поступления заявки.

```sql
shift_request AS (
    SELECT 
        fr.id_client,
        fr.manager,
        fr.date_request,
        fr.date_response,
        CASE
            -- Если день выходной или пятница после 20:00, переносим на первый рабочий день
            WHEN dc.is_working = 0
                 OR (dc.is_working = 1 AND fr.dow = 5 AND fr.time_req > '20:00:00')
            THEN (
                SELECT MIN(dc.date_calendar)
                FROM dict_calendar dc
                WHERE dc.date_calendar > fr.date_req 
                  AND dc.is_working = 1
            ) + INTERVAL '9 hours'
            
            -- Если рабочий день до 09:00, переносим на 9 утра этого дня
            WHEN dc.is_working = 1 AND fr.time_req < '09:00:00'
            THEN fr.date_req + INTERVAL '9 hours'
            
            -- Если рабочий день (кроме пятницы) после 20:00, переносим на 9 утра следующего дня
            WHEN dc.is_working = 1 AND fr.dow != 5 AND fr.time_req > '20:00:00'
            THEN fr.date_req + INTERVAL '1 days 9 hours'
            
            -- В остальных случаях оставляем как есть
            ELSE fr.date_request
        END AS date_registration
    FROM first_response fr
    LEFT JOIN dict_calendar dc ON fr.date_req = dc.date_calendar
)
```

---

### Шаг 4: Расчет времени обработки заявки

Рассчитываем время между ответом (`date_response`) и регистрацией заявки (`date_registration`). Результат переводим в часы.

```sql
SELECT 
    *,
    ROUND(EXTRACT(EPOCH FROM date_response - date_registration) / 3600, 2) AS interval_hours
FROM shift_request;
```

---

### Шаг 5: Создание финального представления для дашборда

Создаем финальное представление, которое будет использоваться в дашборде.

```sql
CREATE OR REPLACE VIEW dash_client_service_view AS
SELECT 
    dc.id_client,
    CASE
        WHEN vrf.date_response IS NULL THEN 'wait'
        ELSE 'done'
    END AS response_status,
    dc.date_request,
    vrf.date_registration,
    vrf.date_response,
    vrf.interval_hours,
    vrf.manager
FROM dict_candidate dc
LEFT JOIN v_response_first vrf ON vrf.id_client = dc."ID"
WHERE dc.source_client IN (SELECT name_source FROM source_list);
```

---

### Шаг 6: Создание физической таблицы для хранения данных

Для повышения производительности и удобства работы с данными создаем физическую таблицу, которая будет хранить данные из представления. Эта таблица создается один раз и будет использоваться для хранения актуальных данных.

```sql
CREATE TABLE dash_client_service_table AS
SELECT * 
FROM dash_client_service_view;
```

---

### Шаг 7: Блок автоматизации для обновления данных

Для поддержания актуальности данных в таблице создаем блок автоматизации, который будет очищать таблицу и вставлять в нее новые данные из представления.

```sql
-- Очистка таблицы перед вставкой новых данных
TRUNCATE TABLE dash_client_service_table;

-- Вставка актуальных данных из представления
INSERT INTO dash_client_service_table
SELECT *
FROM dash_client_service_view;
```
**Примечание:** в планах автоматизировать ежедневное выполнение скрипта через библиотеку **schedule** в Python.


---

## Заключение

В рамках задачи была создана витрина данных для дашборда отдела клиентского сервиса. В процессе использовались временные таблицы, представления и сложные SQL-запросы для учета бизнес-правил, таких как перенос заявок на рабочее время.

В планах перейти с чистого SQL на использование библиотек **pandas**, **SQLAlchemy** и **schedule** для более гибкой обработки данных. Начальные этапы разработки можно увидеть в [этом ноутбуке](contact-centr.ipynb).
