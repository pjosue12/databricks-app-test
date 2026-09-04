from fastapi import FastAPI, APIRouter, Query, HTTPException
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import os

app = FastAPI(title="BCR Pagos API")
api_router = APIRouter(prefix="/api")

# SDK toma credenciales OAuth del ambiente automáticamente
# cuando corre dentro de un Databricks App
w = WorkspaceClient()

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
TABLE = "bcr_demo.pagos.transacciones"


def ejecutar_query(sql: str) -> list[dict]:
    response = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="30s",
    )
    if response.status.state != StatementState.SUCCEEDED:
        raise HTTPException(
            status_code=500,
            detail=f"Query falló: {response.status.error}"
        )
    columns = [col.name for col in response.manifest.schema.columns]
    rows = response.result.data_array or []
    return [dict(zip(columns, row)) for row in rows]


@api_router.get("/transacciones")
def listar_transacciones(
    estado: str | None = Query(None, description="aprobada | rechazada | pendiente | revertida"),
    moneda: str | None = Query(None, description="CRC | USD"),
    limite: int        = Query(50, ge=1, le=500),
):
    where_clauses = []
    if estado:
        where_clauses.append(f"estado = '{estado}'")
    if moneda:
        where_clauses.append(f"moneda = '{moneda}'")
    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    sql = f"""
        SELECT id_transaccion, comercio, monto, moneda,
               monto_crc, estado, categoria_monto, fecha_hora
        FROM {TABLE}
        {where}
        ORDER BY fecha_hora DESC
        LIMIT {limite}
    """
    return ejecutar_query(sql)


@api_router.get("/transacciones/resumen")
def resumen():
    sql = f"""
        SELECT estado, categoria_monto,
               COUNT(*)                 AS total,
               ROUND(SUM(monto_crc), 2) AS monto_total_crc,
               ROUND(AVG(monto_crc), 2) AS monto_promedio_crc
        FROM {TABLE}
        GROUP BY estado, categoria_monto
        ORDER BY estado, categoria_monto
    """
    return ejecutar_query(sql)


@api_router.get("/transacciones/{id_transaccion}")
def detalle_transaccion(id_transaccion: str):
    sql = f"""
        SELECT * FROM {TABLE}
        WHERE id_transaccion = '{id_transaccion}'
        LIMIT 1
    """
    rows = ejecutar_query(sql)
    if not rows:
        raise HTTPException(status_code=404, detail="Transacción no encontrada")
    return rows[0]


@app.get("/health")
def health():
    return {"status": "ok"}


app.include_router(api_router)