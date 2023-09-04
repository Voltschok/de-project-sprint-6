# Проект 6-го спринта

### Описание
В проекте по работе с аналитическими базами данных (de-project-sprint-6) было спроектировано хранилище данных (схема Data Vault) на базе СУБД Vertica. 

### Структура репозитория
- `/src/dags`

### Как запустить контейнер
Запустите локально команду:
```
docker run \
-d \
-p 3000:3000 \
-p 3002:3002 \
-p 15432:5432 \
--mount src=airflow_sp5,target=/opt/airflow \
--mount src=lesson_sp5,target=/lessons \
--mount src=db_sp5,target=/var/lib/postgresql/data \
--name=de-sprint-5-server-local \
sindb/de-pg-cr-af:latest
```

После того как запустится контейнер, вам будут доступны:
- Airflow
	- `localhost:3000/airflow`
- БД
	- `jovyan:jovyan@localhost:15432/de`
