import os
import uuid
import datetime as dt
from typing import List


from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.engine import create_engine
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv


# Load env
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://dfs:dfs@localhost:5432/dfs")
RING_VNODES = int(os.getenv("RING_VNODES", "128"))


# Lazy SQLAlchemy engine (no ORM to keep it explicit)
engine = create_engine(DATABASE_URL, poolclass=NullPool, future=True)

app = FastAPI(title="DFS Metadata Service", version="0.1")

class RegisterNodeIn(BaseModel):
    addr: str # host:port

class RegisterNodeOut(BaseModel):
    node_id: uuid.UUID

class HeartbeatIn(BaseModel):
    node_id: uuid.UUID

class NodeOut(BaseModel):
    node_id: uuid.UUID
    addr: str
    last_heartbeat: dt.datetime
    is_alive: bool

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/nodes/register", response_model=RegisterNodeOut)
def register_node(body: RegisterNodeIn):
    node_id = uuid.uuid4()
    with engine.begin() as conn:
        conn.execute(text(
            """
            insert into nodes(node_id, addr, last_heartbeat, is_alive)
            values(:node_id, :addr, now(), true)
            on conflict (node_id) do nothing
            """
        ), {"node_id": str(node_id), "addr": body.addr})
    return RegisterNodeOut(node_id=node_id)

@app.post("/nodes/heartbeat")
def heartbeat(body: HeartbeatIn):
    with engine.begin() as conn:
        res = conn.execute(text(
            """
            update nodes set last_heartbeat = now(), is_alive = true
            where node_id = :node_id
            """
        ), {"node_id": str(body.node_id)})
        if res.rowcount == 0:
            raise HTTPException(status_code=404, detail="node not found")
    return {"ok": True}


@app.get("/nodes", response_model=List[NodeOut])
def list_nodes():
    with engine.begin() as conn:
        rows = conn.execute(text(
            "select node_id, addr, last_heartbeat, is_alive from nodes order by addr"
        )).mappings().all()
        return [NodeOut(**{
            "node_id": uuid.UUID(str(r["node_id"])) if not isinstance(r["node_id"], uuid.UUID) else r["node_id"],
            "addr": r["addr"],
            "last_heartbeat": r["last_heartbeat"],
            "is_alive": r["is_alive"],
        }) for r in rows]
# TODO (Session 2): /files/plan-upload, /files/{file_id} mapping, etc.