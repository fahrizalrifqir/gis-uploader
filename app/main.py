# app/main.py
import os
import uuid
import zipfile
import shutil
import tempfile
import subprocess
import asyncio
from typing import Optional, List

from fastapi import FastAPI, File, UploadFile, HTTPException, Depends, Header, Query
from fastapi.responses import JSONResponse, FileResponse
from starlette.background import BackgroundTask
import asyncpg
import logging

# -------------------------
# Konfigurasi dari ENV
# -------------------------
DB_DSN = os.getenv("DATABASE_DSN")
if not DB_DSN:
    raise RuntimeError("Environment variable DATABASE_DSN belum diset. Isi connection string Supabase Anda.")

TARGET_TABLE = os.getenv("TARGET_TABLE", "public.tapak_proyek")
STAGING_TABLE = os.getenv("STAGING_TABLE", "public.staging_tapak_upload")
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(50 * 1024 * 1024)))  # default 50MB
API_KEY = os.getenv("API_KEY")  # jika di-set, header x-api-key wajib

# -------------------------
# Setup aplikasi
# -------------------------
app = FastAPI(title="GIS Uploader - Tapak Proyek")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gis_uploader")

# -------------------------
# Dependency: API key sederhana
# -------------------------
def require_api_key(x_api_key: Optional[str] = Header(None)):
    if API_KEY:
        if not x_api_key or x_api_key != API_KEY:
            raise HTTPException(status_code=401, detail="Invalid or missing API key")

# -------------------------
# Helper: menjalankan ogr2ogr untuk import ke staging
# -------------------------
def ogr2ogr_import(shp_dir: str, staging_table: str):
    """
    Menemukan file .shp di shp_dir lalu menjalankan ogr2ogr untuk memasukkannya
    ke PostGIS sebagai staging_table (overwrite).
    Memaksa reprojeksi ke EPSG:4326 dan geometry name 'geom'.
    """
    shp_file = None
    for f in os.listdir(shp_dir):
        if f.lower().endswith(".shp"):
            shp_file = os.path.join(shp_dir, f)
            break
    if not shp_file:
        raise RuntimeError("Tidak ditemukan file .shp di dalam zip.")

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
    logger.info("Menjalankan ogr2ogr import: %s", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        logger.error("ogr2ogr error: %s", proc.stderr)
        raise RuntimeError(f"ogr2ogr gagal: {proc.stderr}")

# -------------------------
# Helper DB: kolom tabel
# -------------------------
async def get_table_columns(conn: asyncpg.Connection, table_fullname: str) -> List[str]:
    """
    Kembalikan list nama kolom untuk table_fullname dalam format 'schema.table'
    """
    if "." not in table_fullname:
        raise ValueError("Table name harus berformat schema.table")
    schema, table = table_fullname.split(".", 1)
    rows = await conn.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position;
        """,
        schema, table
    )
    return [r["column_name"] for r in rows]

# -------------------------
# Helper: append dari staging ke target
# -------------------------
async def append_from_staging(conn: asyncpg.Connection, target_table: str, staging_table: str) -> int:
    """
    Mapping kolom berdasarkan nama (case-insensitive).
    Untuk kolom target yang tidak ada di staging akan diisi NULL.
    Mengembalikan jumlah baris yang di-insert.
    """
    target_cols = await get_table_columns(conn, target_table)
    staging_cols = await get_table_columns(conn, staging_table)

    # create mapping staging lowercase -> original name
    staging_map = {c.lower(): c for c in staging_cols}

    insert_cols = []
    select_exprs = []
    for col in target_cols:
        if col == "id":  # skip primary serial
            continue
        insert_cols.append(col)
        if col.lower() in staging_map:
            # gunakan nama kolom staging asli, lalu alias ke nama target
            select_exprs.append(f"{staging_map[col.lower()]} AS {col}")
        else:
            select_exprs.append(f"NULL AS {col}")

    if not insert_cols:
        raise RuntimeError("Tidak ada kolom untuk di-insert ke tabel target.")

    insert_cols_sql = ", ".join(insert_cols)
    select_sql = ", ".join(select_exprs)
    sql = f"INSERT INTO {target_table} ({insert_cols_sql}) SELECT {select_sql} FROM {staging_table};"
    logger.info("Menjalankan append SQL ke target")
    res = await conn.execute(sql)
    # asyncpg execute mengembalikan 'INSERT 0 X' -> ambil angka terakhir
    try:
        inserted = int(res.split()[-1])
    except Exception:
        inserted = 0
    return inserted

# -------------------------
# Endpoint: upload shapefile zip
# -------------------------
@app.post("/upload", dependencies=[Depends(require_api_key)])
async def upload_shp(file: UploadFile = File(...)):
    """
    Menerima file ZIP yang berisi shapefile (.shp .dbf .shx .prj).
    Proses:
    - simpan sementara
    - extract
    - ogr2ogr import ke staging table
    - append ke target table
    - trunc staging
    - kembalikan jumlah fitur yang ditambahkan
    """
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
        raise HTTPException(status_code=400, detail=f"Gagal mengekstrak ZIP: {e}")

    # import to staging (blocking) via thread
    try:
        await asyncio.to_thread(ogr2ogr_import, tmp_dir, STAGING_TABLE)
    except Exception as e:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=f"Import shapefile gagal: {e}")

    conn = await asyncpg.connect(DB_DSN)
    try:
        inserted = await append_from_staging(conn, TARGET_TABLE, STAGING_TABLE)
        # kosongkan staging table (tetap biarkan strukturnya jika perlu)
        await conn.execute(f"TRUNCATE TABLE {STAGING_TABLE};")
    finally:
        await conn.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return JSONResponse({"status": "ok", "inserted_rows": inserted})

# -------------------------
# Helper export: ogr2ogr export dan zip
# -------------------------
def _run_ogr2ogr_export(sql: str, out_dir: str, layer_name: str = "export_tapak"):
    """
    Jalankan ogr2ogr untuk mengekspor hasil SQL dari PostGIS ke shapefile di out_dir.
    """
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
    logger.info("Menjalankan ogr2ogr export: %s", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        logger.error("ogr2ogr export failed: %s", proc.stderr)
        raise RuntimeError(f"ogr2ogr export gagal: {proc.stderr}")

def _zip_shapefile_dir(src_dir: str, zip_out_path: str) -> str:
    base = zip_out_path.replace(".zip", "")
    archive = shutil.make_archive(base_name=base, format='zip', root_dir=src_dir)
    return archive

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
        created = await asyncio.to_thread(_zip_shapefile_dir, tmpdir, zip_path)
        return tmpdir, created
    except Exception as e:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise

# -------------------------
# Endpoint: download all
# -------------------------
@app.get("/download/all", dependencies=[Depends(require_api_key)])
async def download_all():
    sql = f"SELECT * FROM {TARGET_TABLE}"
    tmpdir, zip_path = await _export_sql_to_zip(sql, zip_name="tapak_proyek_all.zip")
    if not os.path.exists(zip_path):
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(status_code=500, detail="Gagal membuat file export.")
    # gunakan BackgroundTask untuk cleanup setelah response selesai
    def _cleanup():
        try:
            os.remove(zip_path)
        except:
            pass
        shutil.rmtree(tmpdir, ignore_errors=True)
    return FileResponse(path=zip_path, filename="tapak_proyek_all.zip", media_type="application/zip",
                        background=BackgroundTask(_cleanup))

# -------------------------
# Endpoint: download by id
# -------------------------
@app.get("/download/id/{feature_id}", dependencies=[Depends(require_api_key)])
async def download_by_id(feature_id: int):
    sql = f"SELECT * FROM {TARGET_TABLE} WHERE id = {feature_id}"
    tmpdir, zip_path = await _export_sql_to_zip(sql, zip_name=f"tapak_proyek_id_{feature_id}.zip")
    if not os.path.exists(zip_path):
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(status_code=404, detail="Fitur tidak ditemukan atau export gagal.")
    def _cleanup():
        try:
            os.remove(zip_path)
        except:
            pass
        shutil.rmtree(tmpdir, ignore_errors=True)
    return FileResponse(path=zip_path, filename=f"tapak_proyek_id_{feature_id}.zip", media_type="application/zip",
                        background=BackgroundTask(_cleanup))

# -------------------------
# Endpoint: download by ids (comma separated)
# -------------------------
@app.get("/download/ids", dependencies=[Depends(require_api_key)])
async def download_by_ids(ids: str = Query(..., description="Contoh: ids=1,3,5")):
    try:
        id_list = [int(x) for x in ids.split(",") if x.strip() != ""]
    except ValueError:
        raise HTTPException(status_code=400, detail="Format ids salah. Gunakan angka dipisah koma.")
    if not id_list:
        raise HTTPException(status_code=400, detail="Daftar id kosong.")
    id_str = ",".join(str(i) for i in id_list)
    sql = f"SELECT * FROM {TARGET_TABLE} WHERE id IN ({id_str})"
    tmpdir, zip_path = await _export_sql_to_zip(sql, zip_name=f"tapak_proyek_ids_{id_str}.zip")
    if not os.path.exists(zip_path):
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(status_code=404, detail="Export gagal.")
    def _cleanup():
        try:
            os.remove(zip_path)
        except:
            pass
        shutil.rmtree(tmpdir, ignore_errors=True)
    return FileResponse(path=zip_path, filename=f"tapak_proyek_ids_{id_str}.zip", media_type="application/zip",
                        background=BackgroundTask(_cleanup))
