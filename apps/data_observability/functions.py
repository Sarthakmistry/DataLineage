import pandas as pd
import json
import os
import uuid
import psycopg2
from dotenv import load_dotenv
from django.conf import settings

load_dotenv(os.path.join(settings.BASE_DIR, '.env'))


# ---------------------------------------------------------------------------
# Load lineage master data from PostgreSQL once at module level
# ---------------------------------------------------------------------------
def _load_lineage_data():
    conn = psycopg2.connect(
        host=os.environ['PGHOST'],
        port=os.environ.get('PGPORT', 5432),
        user=os.environ['PGUSER'],
        password=os.environ['PGPASSWORD'],
        dbname='data_observability',
        sslmode='require',
    )
    cur = conn.cursor()
    cur.execute('SELECT * FROM consumption.data_lineage_master_data')
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    df = pd.DataFrame(rows, columns=cols)

    df.columns = [c.lower() for c in df.columns]
    for col in ['source_metadata', 'target_metadata', 's_delete_metadata', 't_delete_metadata']:
        df[col] = df[col].apply(
            lambda x: x if isinstance(x, dict)
            else (json.loads(x) if isinstance(x, str) and x.strip() not in ('', '{}', 'NULL') else {})
        )
    return df


_lineage_df = _load_lineage_data()


# ---------------------------------------------------------------------------
# Helper: build a node dict from a row and its metadata
# ---------------------------------------------------------------------------
def _build_source_node(row, level):
    src_meta = row.source_metadata if isinstance(row.source_metadata, dict) else {}
    s_del_meta = row.s_delete_metadata if isinstance(row.s_delete_metadata, dict) else {}
    return {
        "id": str(uuid.uuid4()),
        "table_name": row.source_object_name,
        "label": row.source_table,
        "db": row.source_database,
        "schema": row.source_schema,
        "level": level,
        "TABLE_OWNER": src_meta.get('TABLE_OWNER', '-'),
        "TABLE_TYPE": src_meta.get('TABLE_TYPE', '-'),
        "ROW_COUNT": src_meta.get('ROW_COUNT', '-'),
        "BYTES": src_meta.get('BYTES', '-'),
        "CREATED": src_meta.get('CREATED', '-'),
        "LAST_ALTERED": src_meta.get('LAST_ALTERED', '-'),
        "LAST_DDL": src_meta.get('LAST_DDL', '-'),
        "LAST_DDL_BY": src_meta.get('LAST_DDL_BY', '-'),
        "IS_ICEBERG": src_meta.get('IS_ICEBERG', '-'),
        "IS_TRANSIENT": src_meta.get('IS_TRANSIENT', '-'),
        "USER": src_meta.get('USER', '-'),
        "IS_DROPPED": s_del_meta.get('SOURCE_DELETE_FLAG', 'NOT_DROPPED'),
        "DELETE_DATE": s_del_meta.get('DELETE_DATE', '-'),
        "DELETED_BY": s_del_meta.get('USER', '-'),
        "TYPE": row.s_table_type,
        "priv": row.s_user_privilege,
        "recon_score": '-',
        "LOAD_TYPE": src_meta.get('LOAD_TYPE', '-'),
        "PIPELINE_TYPE": src_meta.get('PIPELINE_TYPE', '-'),
        "SCHD_FREQUENCY": src_meta.get('SCHD_FREQUENCY', '-'),
        "SCHD_PLAN": src_meta.get('SCHD_PLAN', '-'),
        "API_URL": src_meta.get('API_URL', '-'),
        "REQ_METHOD": src_meta.get('REQ_METHOD', '-'),
        "STATUS": src_meta.get('STATUS', '-'),
        "S3_PATH": src_meta.get('S3_PATH', '-'),
        "FILE_NAME": src_meta.get('FILE_NAME', '-'),
        "FILE_PATH": src_meta.get('FILE_PATH', '-'),
        "LOAD_FREQUENCY": src_meta.get('LOAD_FREQUENCY', '-'),
        "props": [],
    }


def _build_target_node(row, level):
    tgt_meta = row.target_metadata if isinstance(row.target_metadata, dict) else {}
    t_del_meta = row.t_delete_metadata if isinstance(row.t_delete_metadata, dict) else {}
    return {
        "id": str(uuid.uuid4()),
        "table_name": row.target_object_name,
        "label": row.target_table,
        "db": row.target_database,
        "schema": row.target_schema,
        "level": level,
        "TABLE_OWNER": tgt_meta.get('TABLE_OWNER', '-'),
        "TABLE_TYPE": tgt_meta.get('TABLE_TYPE', '-'),
        "ROW_COUNT": tgt_meta.get('ROW_COUNT', '-'),
        "BYTES": tgt_meta.get('BYTES', '-'),
        "CREATED": tgt_meta.get('CREATED', '-'),
        "LAST_ALTERED": tgt_meta.get('LAST_ALTERED', '-'),
        "LAST_DDL": tgt_meta.get('LAST_DDL', '-'),
        "LAST_DDL_BY": tgt_meta.get('LAST_DDL_BY', '-'),
        "IS_ICEBERG": tgt_meta.get('IS_ICEBERG', '-'),
        "IS_TRANSIENT": tgt_meta.get('IS_TRANSIENT', '-'),
        "USER": tgt_meta.get('USER', '-'),
        "IS_DROPPED": t_del_meta.get('TARGET_DELETE_FLAG', 'NOT_DROPPED'),
        "DELETE_DATE": t_del_meta.get('DELETE_DATE', '-'),
        "DELETED_BY": t_del_meta.get('USER', '-'),
        "TYPE": row.t_table_type,
        "priv": row.t_user_privilege,
        "recon_score": '-',
        "LOAD_TYPE": '-',
        "PIPELINE_TYPE": '-',
        "SCHD_FREQUENCY": '-',
        "SCHD_PLAN": '-',
        "API_URL": '-',
        "REQ_METHOD": '-',
        "STATUS": '-',
        "S3_PATH": '-',
        "FILE_NAME": '-',
        "FILE_PATH": '-',
        "LOAD_FREQUENCY": '-',
        "props": [],
    }


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def get_metadata():
    df = _lineage_df

    src = df[['source_database', 'source_schema', 'source_table']].rename(
        columns={'source_database': 'db', 'source_schema': 'schema', 'source_table': 'table'})
    tgt = df[['target_database', 'target_schema', 'target_table']].rename(
        columns={'target_database': 'db', 'target_schema': 'schema', 'target_table': 'table'})
    combined = pd.concat([src, tgt]).dropna().drop_duplicates()

    meta = {}
    for _, row in combined.iterrows():
        db, schema, table = row['db'], row['schema'], row['table']
        if db not in meta:
            meta[db] = {'text': db, 'children': {}}
        if schema not in meta[db]['children']:
            meta[db]['children'][schema] = {'text': schema, 'children': []}
        meta[db]['children'][schema]['children'].append({'text': table, 'children': []})

    for db_node in meta.values():
        db_node['children'] = list(db_node['children'].values())
        for schema_node in db_node['children']:
            schema_node['children'] = list(schema_node['children'])

    return list(meta.values())


def getJson(df, root_name):
    children = []
    for child_name in df[df['SOURCE'] == root_name]['TARGET']:
        children.append({"text": child_name, "children": getJson(df, child_name)})
    return children


#################################################################### visjs data preparation functions ##############################


def generate_visjs_data(db, schema, table):
    table_name = f"{db}.{schema}.{table}"
    nodes = {}
    edges = []

    complete_dataset = _lineage_df[
        (_lineage_df.target_object_name == table_name) |
        (_lineage_df.source_object_name == table_name)
    ].copy()

    as_target = complete_dataset[
        complete_dataset.target_object_name == table_name
    ].dropna(subset=['source_object_name', 'target_object_name'])

    rows_to_use = as_target if not as_target.empty else complete_dataset[
        complete_dataset.source_object_name == table_name
    ].dropna(subset=['source_object_name', 'target_object_name'])

    for _, row in rows_to_use.iterrows():
        if row.source_object_name not in nodes:
            nodes[row.source_object_name] = _build_source_node(row, 0)
        if row.target_object_name not in nodes:
            src_level = nodes[row.source_object_name]["level"]
            nodes[row.target_object_name] = _build_target_node(row, src_level + 1)

        edges.append({
            "from": nodes[row.source_object_name]["id"],
            "to": nodes[row.target_object_name]["id"],
            "source_obj": nodes[row.source_object_name]["table_name"],
            "target_obj": nodes[row.target_object_name]["table_name"],
            "mapping": row.s_t_column_mapping,
        })

    return {"nodes": list(nodes.values()), "edges": edges}


def expand_visjs_data(table, node_id, level, connected_nodes):
    connected_nodes_lst = connected_nodes.split(',')
    nodes = {}
    edges = []

    complete_dataset = _lineage_df[_lineage_df.source_object_name == table].copy()

    rows = complete_dataset[
        (complete_dataset.source_object_name == table) &
        (complete_dataset.source_object_name != complete_dataset.target_object_name)
    ].dropna(subset=['source_object_name', 'target_object_name'])

    for _, row in rows.iterrows():
        if row.target_object_name not in nodes and row.target_object_name not in connected_nodes_lst:
            nodes[row.target_object_name] = _build_target_node(row, int(level) + 1)
            edges.append({
                "from": node_id,
                "to": nodes[row.target_object_name]["id"],
                "source_obj": table,
                "target_obj": nodes[row.target_object_name]["table_name"],
                "mapping": row.s_t_column_mapping,
            })

    return {"nodes": list(nodes.values()), "edges": edges}


def expand_visjs_data_back(table, node_id, level, connected_nodes):
    connected_nodes_lst = connected_nodes.split(',')
    nodes = {}
    edges = []

    complete_dataset = _lineage_df[_lineage_df.target_object_name == table].copy()

    rows = complete_dataset[
        (complete_dataset.target_object_name == table) &
        (complete_dataset.source_object_name != complete_dataset.target_object_name)
    ].dropna(subset=['source_object_name', 'target_object_name'])

    for _, row in rows.iterrows():
        if row.source_object_name not in nodes and row.source_object_name not in connected_nodes_lst:
            nodes[row.source_object_name] = _build_source_node(row, int(level) - 1)
            edges.append({
                "from": nodes[row.source_object_name]["id"],
                "to": node_id,
                "source_obj": nodes[row.source_object_name]["table_name"],
                "target_obj": table,
                "mapping": row.s_t_column_mapping,
            })

    return {"nodes": list(nodes.values()), "edges": edges}


def get_label(columns):
    final_title = ""
    try:
        unique_columns = set()
        for col in columns.split(','):
            col = col.strip()
            if col not in unique_columns:
                final_title += col + os.linesep
                unique_columns.add(col)
        return final_title
    except Exception:
        return final_title


def get_data_definition(SOURCE_OBJECT, TARGET_OBJECT, S_T_COLUMN):
    try:
        df_row = _lineage_df[
            (_lineage_df.source_object_name == SOURCE_OBJECT) &
            (_lineage_df.target_object_name == TARGET_OBJECT) &
            (_lineage_df.s_t_column_mapping == S_T_COLUMN)
        ].head(1)

        if df_row.empty:
            return pd.DataFrame(columns=['source_column', 'target_column', 'description', 'column_type', 'column_logic'])

        mapping_str = df_row.iloc[0]['s_t_column_mapping']
        target_db = df_row.iloc[0]['target_database']
        target_schema = df_row.iloc[0]['target_schema']
        target_table = df_row.iloc[0]['target_table']

        rows = []
        for pair in mapping_str.split(','):
            pair = pair.strip()
            if ' -> ' in pair:
                src_col, tgt_col = pair.split(' -> ', 1)
                rows.append({
                    'source_column': src_col.strip(),
                    'target_column': tgt_col.strip().upper(),
                    'description': 'NA',
                    'column_type': 'NA',
                    'column_logic': 'NA',
                })

        return pd.DataFrame(rows, columns=['source_column', 'target_column', 'description', 'column_type', 'column_logic'])
    except Exception:
        return pd.DataFrame(columns=['source_column', 'target_column', 'description', 'column_type', 'column_logic'])


def get_recon_data(TARGET_OBJECT):
    return '-'
