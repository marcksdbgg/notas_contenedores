import os
import time
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

# Leer DATABASE_URL y normalizar esquema
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:password@localhost:5432/postits")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

PORT = int(os.environ.get("PORT", "8000"))

app = FastAPI(title="Postits API (FastAPI)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ajusta si quieres restringir
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

pool: Optional[ConnectionPool] = None

def wait_for_db(conninfo: str, retries: int = 60, delay: float = 1.0) -> None:
    """Espera a que Postgres esté listo sin depender de healthchecks externos."""
    last_err = None
    for _ in range(retries):
        try:
            with psycopg.connect(conninfo, connect_timeout=3) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                return
        except Exception as e:
            last_err = e
            time.sleep(delay)
    raise RuntimeError(f"No se pudo conectar a la DB tras {retries} intentos: {last_err}")

def init_db_table() -> None:
    assert pool is not None
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS posts (
                    id SERIAL PRIMARY KEY,
                    text TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT now()
                );
            """)

@app.on_event("startup")
def on_startup():
    global pool
    # Espera activa a DB (evita depender de apt/netcat/binaries extra)
    wait_for_db(DATABASE_URL)
    # Pool con autocommit y filas como diccionarios
    pool = ConnectionPool(
        conninfo=DATABASE_URL,
        min_size=1,
        max_size=10,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    init_db_table()

class PostIn(BaseModel):
    text: str

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/posts")
def list_posts() -> List[dict]:
    assert pool is not None
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, text, created_at FROM posts ORDER BY created_at DESC;")
            return cur.fetchall()

@app.post("/posts", status_code=201)
def create_post(payload: PostIn):
    text = (payload.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="empty text")
    assert pool is not None
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO posts (text) VALUES (%s) RETURNING id, text, created_at;",
                (text,),
            )
            return cur.fetchone()

@app.delete("/posts/{post_id}")
def delete_post(post_id: int):
    assert pool is not None
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM posts WHERE id = %s RETURNING id;", (post_id,))
            row = cur.fetchone()
            if row:
                return {"deleted": post_id}
            raise HTTPException(status_code=404, detail="not found")
