import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
load_dotenv()

def get_db_url() -> str:
    user = os.getenv("POSTGRES_USER", "yt_user")
    password = os.getenv("POSTGRES_PASSWORD", "yt_password")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT" , "5432")
    db = os.getenv("POSTGRES_DB", "youtube_analytics")
    return f"postgressql+psyconfig2://{user}:{password}@{host}:{port}/{db}"


def get_engine():
    return create_engine(get_db_url(), future=True)

def execute_sql_file(engine, file_path: str) -> None:
    with open(file_path, "r" , encoding="utf-8") as file:
        sql = file.read()

        with engine.begin() as conn:
            for statement in sql.split(";"):
                stmt = statement.strip()
                if stmt:
                    conn.execute(text(stmt))
