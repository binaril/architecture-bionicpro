## Запуск

### Требования

- Docker Desktop с поддержкой Compose v2
- Свободные порты: 3000, 8000, 8080, 8088, 8123, 9000, 389, 5433, 5436

### Запуск всех сервисов

```bash
docker compose up --build -d
```

Дождитесь готовности (30–60 с):

```bash
docker compose ps
```

Все сервисы должны иметь статус `running` / `healthy`.

---

### 1. PKCE (Задание 1, Задача 2)

**Проверка в браузере:**

1. Откройте http://localhost:3000 -> **Login**
2. Ввести email или имя пользователя и пароль любого пользователя из config.ldif
3. Можно проверить в DevTools → Network, запрос `/realms/reports-realm/protocol/openid-connect/auth` должен содержать параметры
   - `code_challenge_method=S256`
   - `code_challenge=<base64url-строка>`
   
---

### 2. Airflow DAG (Задание 2, Задача 2)

1. Откройте Airflow UI: http://localhost:8088 (admin / admin)
2. Убедитесь, что присутствуют два DAG:
   - `crm_to_olap` — расписание `0 * * * *` (каждый час)
   - `build_mart` — расписание `0 1 * * *` (ежедневно в 01:00)
3. Запустите `crm_to_olap` вручную 
4. Запустите `build_mart` вручную


### 3. UI: кнопка отчёта (Задание 2, Задача 5)

1. Откройте http://localhost:3000
2. Войдите как `john.doe` / `password`
3. Нажмите **Download Report**
4. Браузер скачает файл `report.json` с данными телеметрии
5. Нажмите **Logout** → кнопка скрывается, виден только **Login**
6. Без входа нажать Download Report невозможно (кнопка недоступна до аутентификации)

---

## Пользователи для тестирования

| Логин (LDAP uid) | Пароль | Email | Запись в CRM |
|------------------|--------|-------|--------------|
| `john.doe` | `password` | john@example.com | ✅ John Doe |
| `jane.smith` | `password` | jane@example.com | ✅ Jane Smith |
| `alex.johnson` | `password` | alex@example.com | ✅ Alex Johnson |


## Диаграммы

- `Task1/BionicPRO_C4_model.drawio.png` — C4 для управления учётными данными (LDAP + IdP + PKCE)
- `Task2/BionicPRO_C4_model_2.drawio.png` — C4 для сервиса отчётов (ETL + OLAP + API)


Проверил на другом ПК, запустилось все ок. Были только проблемы с openldap из-за скрытых символов в entrypoint.sh, после корректировки норм
Так же приложил скриншоты в папке screenshots

http://localhost:8000/reports ограничен скачиванием только отчета пользователя, который его запрашивает. Пользователь определяется по Sid из токена