# Books ETL Project

This project runs an ETL process for processed book data using Docker and PostgreSQL.

## 1. Start Docker Services

Run the following command to start the required services in the background:

```bash
sudo docker compose -f docker/docker-compose.yaml up -d
```

---

## 2. Check if the PostgreSQL container is running correctly

Replace `<container_name_or_id>` with your actual PostgreSQL container name:

```bash
docker ps
docker exec -it <container_name_or_id> psql -U postgres_user -d postgres_db -c "SELECT 1;"
```

If you see output like `?column? | 1`, your database is working.

---

## 3. Load Environment Variables

Export all environment variables from `docker/.env`:

```bash
export $(grep -v '^#' docker/.env | xargs)
```

---

## 4. Run the ETL Process

Run the ETL Python script, passing the desired execution date as an argument:

```bash
python3 src/books_etl.py 2025-01-01
```

---

## Notes

* Ensure **Python 3** and all dependencies from `requirements.txt` are installed before running the ETL.
* You can stop the services with:

  ```bash
  sudo docker compose -f docker/docker-compose.yaml down
  ```
* To inspect processed data:

  ```bash
  docker exec -it <container_name_or_id> psql -U postgres_user -d postgres_db -c "SELECT * FROM books_processed LIMIT 10;"
  ```
