from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqllineage.runner import LineageRunner
from pathlib import Path
from argparse import Namespace
from pydantic import BaseModel
import os
import re
import sqllineage

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "*"],  # Replace with your frontend's URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Define the path to your "Scripts" folder
scripts_folder = "Scripts"

def list_queries(folder_path):
    query_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".sql"):
                query_files.append(os.path.join(root, file))
    return query_files

def extract_sql_from_args(args: Namespace) -> str:
    sql = ""
    if getattr(args, "f", None):
        try:
            with open(args.f) as f:
                sql = f.read()
        except IsADirectoryError:
            # logger.exception("%s is a directory", args.f)
            exit(1)
        except FileNotFoundError:
            # logger.exception("No such file: %s", args.f)
            exit(1)
        except PermissionError:
            # On Windows, open a directory as file throws PermissionError
            # logger.exception("Permission denied when reading file '%s'", args.f)
            exit(1)
    elif getattr(args, "e", None):
        sql = args.e
    return sql

class Item(BaseModel):
    f: str = None
    d: str = None

class LineageLevel:
    TABLE = "table"
    COLUMN = "column"

# Serve the static files from the React app's build directory
app.mount("/static", StaticFiles(directory="../fronend/build/static"), name="static")

@app.get("/")
def serve_frontend():
    # Serve the main index.html file from the build directory
    return FileResponse("../fronend/build/index.html")

@app.post("/list-queries")
async def get_query_list(payload: Item):
    if payload.f:
        root = Path(payload.f).parent
    elif payload.d:
        root = Path(payload.d)
    else:
        root = Path(scripts_folder)
    data = {
        "id": str(root),
        "name": root.name,
        "is_dir": True,
        "children": [
            {"id": str(p), "name": p.name, "is_dir": p.is_dir()}
            for p in sorted(root.iterdir(), key=lambda _: (not _.is_dir(), _.name))
        ],
    }
    return data

@app.post("/script")
def script(payload: Item):
    req_args = Namespace(**payload.__dict__)
    sql = extract_sql_from_args(req_args)
    return {"content": sql}

@app.post("/lineage")
def lineage(payload: Item):
    # this is to avoid circular import
    from sqllineage.runner import LineageRunner

    req_args = Namespace(**payload.__dict__)
    sql = extract_sql_from_args(req_args)
    
    # Extract tables used in the selected script
    used_tables = extract_tables_from_sql(sql)

    # Find source scripts for each table
    source_scripts = []
    for table in used_tables:
        source_scripts.append(find_source_scripts(table.split('.')[-1]))
    # Combine the main script and source scripts into one SQL query
    combined_sql = combine_scripts(sql, source_scripts)


    # Perform lineage analysis on the combined SQL query
    lr = LineageRunner(combined_sql, verbose=True)
    data = {
        "verbose": str(lr),
        "dag": lr.to_cytoscape(),
        "column": lr.to_cytoscape(LineageLevel.COLUMN),
    }
    
    return data

@app.get("/lineage/all")
async def get_all_lineage():
    all_lineage = {}
    queries = []
    
    for root, _, files in os.walk(scripts_folder):
        for file in files:
            if file.endswith(".sql"):
                query_path = os.path.join(root, file)
                with open(query_path, "r") as query_file:
                    sql_query = query_file.read()
                try:
                    lineage_runner = LineageRunner(sql_query)
                    lineage = lineage_runner.to_cytoscape()
                    query_name = os.path.relpath(query_path, scripts_folder)
                    all_lineage[query_name] = lineage
                    queries.append(sql_query)
                except Exception as e:
                    # Skip the script and continue with the next one if there's an error
                    print(f"Error analyzing {query_name}: {str(e)}")
                    continue

    # Exclude scripts with errors from the overall lineage analysis
    scripts_with_errors = set(all_lineage.keys())
    combined_script = ";\n".join(queries)
    
    overall_lineage = LineageRunner(combined_script).to_cytoscape()

    return {"lineage": overall_lineage, "scripts_with_errors": list(scripts_with_errors)}

@app.get("/lineage/{query_name}")
def get_lineage_with_sources(query_name: str):
    query_path = os.path.join(scripts_folder, query_name)
    if not os.path.exists(query_path):
        raise HTTPException(status_code=404, detail="Query not found")

    with open(query_path, "r") as query_file:
        sql_query = query_file.read()

    # Extract tables used in the selected script
    used_tables = extract_tables_from_sql(sql_query)

    # Find source scripts for each table
    source_scripts = []
    for table in used_tables:
        source_scripts.append(find_source_scripts(table.split('.')[-1]))
    # Combine the main script and source scripts into one SQL query
    combined_sql = combine_scripts(sql_query, source_scripts)


    # Perform lineage analysis on the combined SQL query
    lr = LineageRunner(combined_sql, verbose=True)
    data = {
        "verbose": str(lr),
        "dag": lr.to_cytoscape(),
        "column": lr.to_cytoscape(LineageLevel.COLUMN),
    }
    return data


def extract_tables_from_sql(sql_query):
    # Regular expression to match table names
    table_pattern = r'\b(?:FROM|JOIN|UPDATE|INTO)\s+([A-Za-z0-9_.]+)\b'
    table_names = set(re.findall(table_pattern, sql_query, flags=re.IGNORECASE))
    return table_names

def find_source_scripts(table_name):
    sql_query = ''
    
    for root, _, files in os.walk(scripts_folder):
        for file in files:
            if file.endswith(f"{table_name}.sql"):
                query_path = os.path.join(root, file)
                with open(query_path, "r") as query_file:
                    sql_query = query_file.read()

    return sql_query

def combine_scripts(main_script_sql, source_scripts):
    #Regex to standardize table names across the dbs and tables
    schema_table_pattern = re.compile(r'\[(.*?)\]\.\[(.*?)\]\.\[(.*?)\]')
    # Combine the SQL queries of the main script and its source scripts
    combined_sql = main_script_sql + '\n' + ';\n'.join(source_scripts)
    combined_sql = schema_table_pattern.sub(r'\1.\2.\3', combined_sql)
    return combined_sql