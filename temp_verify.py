import ast
from pathlib import Path

base = Path('/home/matt-christy/Documents/TechnicalProjects/sports-betting-odds-tracker')
for rel in [
    'app/query_betting_api.py',
    'app/config/config.py',
    'app/ddl/table_schemas.py',
    'app/gcs_to_bigquery.py',
    'streamlit/streamlit_app.py',
]:
    path = base / rel
    try:
        with path.open('r', encoding='utf-8') as f:
            ast.parse(f.read(), filename=str(path))
        print('OK', rel)
    except Exception as exc:
        print('FAIL', rel, type(exc).__name__, exc)
