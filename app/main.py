# app/main.py
import os
import uuid
import zipfile
import shutil
import tempfile
import subprocess
import asyncio
from typing import Optional

from fastapi import FastAPI, File, UploadFile, HTTPException, Depends, Header, Query
from fastapi.responses import JSONResponse, FileResponse
import asyncpg

# Konfigurasi dari env
DB_DSN = os.getenv("DATABASE_DSN")  # contoh: postgresql://user:pass@host:5432/dbname
TARGET_TABLE = os.getenv("TARGET_TABLE", "public.tapak_proyek")
STAGING_TABLE = os.getenv("STAGING_TABLE", "public.staging_tapak_upload")
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(50*1024*1024)))  # default 50 MB
API_KEY = os.getenv("API_KEY")  # optional - jika diset, semua request butuh header x-api-key

app = FastAPI(title="GIS Uploader - Tapak Proyek")

# Simple API key dependency
def require_api_key(x_api_key: Optional[str] = Header(None)):
    if API_KEY:
        if not x_api_key or x_api_key != API_KEY:
            raise HTTPException(status_code=401, detail="Invalid or missing API key")

# Helper: run ogr2ogr import into staging
def ogr2ogr_import(shp_dir: str, staging_table: str):
    shp_file = None
    for f in os.listdir(shp_dir):
        if f.lower().endswith(".shp"):
            shp_file = os.path.join(shp_dir, f)
            break
    if not shp_file:
        raise RuntimeError("Tidak ditemukan file .shp di dalam zip.")

    # Build ogr2ogr command. Force geometry name 'geom' and overwrite staging table.
    cmd = [
        "ogr2ogr",
        "-f", "PostgreSQL",
        f"PG:{DB_DSN}",
        shp_file,
        "-nln", staging_table,
        "-overwrite",
        "-lco", "GEOMETRY_NAME=geom",
        "-lco", "ENCODING=UTF-8",
        "-t_srs", "EPSG:4326"
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"ogr2ogr gagal: {proc.stderr}")

# Helper: get columns list for table
async def get_table_columns(conn, table_fullname: str):
    schema, table = table_fullname.split(".", 1)
    rows = await conn.fetch("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position;
    """, schema, table)
    return [r["column_name"] for r in rows]

# Helper: append from staging to target with mapping by column name (case insensitive)
async def append_from_staging(conn, target_table: str, staging_table: str) -> int:
    target_cols = await get_table_columns(conn, target_table)
    staging_cols = await get_table_columns(conn, staging_table)

    # Normalize names to lower for matching
    staging_set = {c.lower(): c for c in staging_cols}

    insert_cols = []
    select_exprs = []
    for col in target_cols:
        if col == "id":
            continue
        insert_cols.append(col)
        # match by lower-case name
        if col.lower() in staging_set:
            select_exprs.append(f"{staging_set[col.lower()]} AS {col}")
        else:
            select_exprs.append(f"NULL AS {col}")

    insert_cols_sql = ", ".join(insert_cols)
    select_sql = ", ".join(select_exprs)
    sql = f"INSERT INTO {target_table} ({insert_cols_sql}) SELECT {select_sql} FROM {staging_table};"
    res = await conn.execute(sql)
    # asyncpg returns 'INSERT 0 X'
    try:
        inserted = int(res.split()[-1])
    except:
        inserted = 0
    return inserted

# Endpoint upload shapefile zip
@app.post("/upload", dependencies=[Depends(require_api_key)])
async def upload_shp(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Unggah file .zip yang berisi shapefile (.shp .dbf .shx .prj).")

    contents = await file.read()
    if len(contents) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="File terlalu besar.")

    tmp_dir = tempfile.mkdtemp(prefix="upload_")
    zip_path = os.path.join(tmp_dir, file.filename)
    with open(zip_path, "wb") as f:
        f.write(contents)

    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(tmp_dir)
    except Exception as e:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise HTTPException(status_code=400, detail=f"Gagal ekstrak ZIP: {e}")

    # Run ogr2ogr to import to staging table
    try:
        await asyncio.to_thread(ogr2ogr_import, tmp_dir, STAGING_TABLE)
    except Exception as e:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=f"Import shapefile gagal: {e}")

    # Connect to DB and append
    conn = await asyncpg.connect(DB_DSN)
    try:
        inserted = await append_from_staging(conn, TARGET_TABLE, STAGING_TABLE)
        # cleanup staging table rows (but keep table)
        await conn.execute(f"TRUNCATE TABLE {STAGING_TABLE};")
    finally:
        await conn.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return JSONResponse({"status": "ok", "inserted_rows": inserted})

# Helper export via ogr2ogr (blocking)
def _run_ogr2ogr_export(sql: str, out_dir: str, layer_name: str = "export_tapak"):
    out_path = os.path.join(out_dir, layer_name + ".shp")
    cmd = [
        "ogr2ogr",
        "-f", "ESRI Shapefile",
        out_path,
        f"PG:{DB_DSN}",
        "-sql", sql,
        "-nln", layer_name,
        "-lco", "ENCODING=UTF-8",
        "-t_srs", "EPSG:4326"
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"ogr2ogr export gagal: {proc.stderr}")

def _zip_shapefile_dir(src_dir: str, zip_out_path: str):
    base = zip_out_path.replace(".zip", "")
    shutil.make_archive(base_name=base, format='zip', root_dir=src_dir)
    return base + ".zip"

async def _export_sql_to_zip(sql: str, zip_name: str):
    tmpdir = tempfile.mkdtemp(prefix="export_")
    try:
        await asyncio.to_thread(_run_ogr2ogr_export, sql, tmpdir)
        zip_path = os.path.join(tempfile.gettempdir(), zip_name)
        if os.path.exists(zip_path):
            try:
                os.remove(zip_path)
            except:
                pass
        zip_created = await asyncio.to_thread(_zip_shapefile_dir, tmpdir, zip_path)
        return tmpdir, zip_created
    except Exception as e:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise

# Download all features
@app.get("/download/all", dependencies=[Depends(require_api_key)])
async def download_all():
    sql = f"SELECT * FROM {TARGET_TABLE}"
    tmpdir, zip_path = await _export_sql_to_zip(sql, zip_name="tapak_proyek_all.zip")
    if not os.path.exists(zip_path):
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(status_code=500, detail="Gagal membuat file export.")
    response = FileResponse(path=zip_path, filename="tapak_proyek_all.zip", media_type="application/zip")
    @response.background
    def _cleanup():
        try:
            os.remove(zip_path)
        except:
            pass
        shutil.rmtree(tmpdir, ignore_errors=True)
    return response

# Download by id
@app.get("/download/id/{feature_id}", dependencies=[Depends(require_api_key)])
async def download_by_id(feature_id: int):
    sql = f"SELECT * FROM {TARGET_TABLE} WHERE id = {feature_id}"
    tmpdir, zip_path = await _export_sql_to_zip(sql, zip_name=f"tapak_proyek_id_{feature_id}.zip")
    if not os.path.exists(zip_path):
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(status_code=404, detail="Fitur tidak ditemukan atau export gagal.")
    response = FileResponse(path=zip_path, filename=f"tapak_proyek_id_{feature_id}.zip", media_type="application/zip")
    @response.background
    def _cleanup():
        try:
            os.remove(zip_path)
        except:
            pass
        shutil.rmtree(tmpdir, ignore_errors=True)
    return response

# Download by comma-separated ids, e.g. ?ids=1,2,5
@app.get("/download/ids", dependencies=[Depends(require_api_key)])
async def download_by_ids(ids: str = Query(...)):
    try:
        id_list = [int(x) for x in ids.split(",") if x.strip() != ""]
    except ValueError:
        raise HTTPException(status_code=400, detail="Format ids salah.")
    if not id_list:
        raise HTTPException(status_code=400, detail="Daftar id kosong.")
    id_str = ",".join(str(i) for i in id_list)
    sql = f"SELECT * FROM {TARGET_TABLE} WHERE id IN ({id_str})"
    tmpdir, zip_path = await _export_sql_to_zip(sql, zip_name=f"tapak_proyek_ids_{id_str}.zip")
    if not os.path.exists(zip_path):
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(status_code=404, detail="Export gagal.")
    response = FileResponse(path=zip_path, filename=f"tapak_proyek_ids_{id_str}.zip", media_type="application/zip")
    @response.background
    def _cleanup():
        try:
            os.remove(zip_path)
        except:
            pass
        shutil.rmtree(tmpdir, ignore_errors=True)
    return response
