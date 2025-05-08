import pandas as pd
import numpy as np
import json
import re
import os
import uuid
from django.conf import settings
from datetime import datetime
from apps.home.functions import snowflake_sql, postgresql_query
from apps.source_repository.functions import get_source_credential_dict






app_source_name_pg = 'DPE_METADATA_SYSTEM_INFORMATION'
credentialsDict_pg, srcObj = get_source_credential_dict(None, app_source_name_pg)


app_source_name_pg_2 = 'DATA_CENTRAL_DATA_OBSERVABILITY_CONSUMPTION_DB'
credentialsDict_pg_2, srcObj = get_source_credential_dict(None, app_source_name_pg_2)




def get_metadata():
    query = """
    SELECT 'PROD' AS TARGET_DB, sm.id as schema_id, sm.schema_name as TARGET_SCHEMA, tm.id as table_id, tm.table_name as TARGET_TABLE
    FROM system_information.schema_master sm
    LEFT JOIN system_information.table_master tm on sm.id=tm.scm_id
    WHERE sm.schema_name in ('PUBLIC','DATA_GOVERNANCE')
    and not (tm.table_name ~ '_[0-9]*$' OR
    tm.table_name ~ '[0-9]+$' OR
    tm.table_name ILIKE '%_BACKUP%' OR
    tm.table_name ILIKE '%BKP%' OR
    tm.table_name ILIKE 'bckp%' OR
    tm.table_name ILIKE '%_TEMP%' OR
    tm.table_name ILIKE '%TEST%');
    """
    df = postgresql_query(query,credentialsDict_pg)[2]
   
   
    # Initialize an empty dictionary to store the output locations
    meta = {}


    # Group the DataFrame by 'TARGET_DB', 'TARGET_SCHEMA', and 'TARGET_TABLE'
    grouped = df.groupby(['target_db', 'target_schema', 'target_table'])


    # Iterate over each group
    for (db, schema, table), _ in grouped:
        if db not in meta:
            meta[db] = {'text': db, 'children': {}}


        if schema not in meta[db]['children']:
            meta[db]['children'][schema] = {'text': schema, 'children': []}


        # Add the table node as a child of the schema node
        meta[db]['children'][schema]['children'].append({'text': table, 'children': []})


    # Convert the nested dictionaries into lists
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


def generate_visjs_data(db,schema,table):
    table_name = f"{db}.{schema}.{table}"
    nodes = {}
    edges = []
    complete_data_qry = f"""SELECT * FROM consumption.data_lineage_master_data
                       where target_object_name = '{table_name}' or source_object_name = '{table_name}';"""
    complete_dataset = postgresql_query(complete_data_qry,credentialsDict_pg_2)[2]
    #print(complete_dataset)
    if (complete_dataset[(complete_dataset.target_object_name == table_name)].dropna(subset = ['source_object_name', 'target_object_name']).empty == False):
   
        for ind,row in complete_dataset[(complete_dataset.target_object_name == table_name)].dropna(subset = ['source_object_name', 'target_object_name']).iterrows():
            #type,source,target,cols = row
       
            src_meta = row.source_metadata
            print(src_meta)
            tgt_meta = row.target_metadata
            s_del_meta = row.s_delete_metadata
            t_del_meta = row.t_delete_metadata
            if row.source_object_name not in nodes:
                source_id = str(uuid.uuid4())
                source_lvl = 0
                nodes[row.source_object_name] = {"id": source_id if source_id !='' else '-',
                                                "table_name":row.source_object_name if row.source_object_name!='' else '-',
                                                "label": row.source_table if row.source_table!='' else '-',
                                                "db": row.source_database  if row.source_database!='' else '-',
                                                "schema": row.source_schema  if row.source_schema!='' else '-',
                                                "level": source_lvl,
                                                "TABLE_OWNER":src_meta['TABLE_OWNER'] if 'TABLE_OWNER' in src_meta.keys() else '-',
                                                "TABLE_TYPE":src_meta['TABLE_TYPE'] if 'TABLE_TYPE' in src_meta.keys() else '-',
                                                "ROW_COUNT":src_meta['ROW_COUNT'] if 'ROW_COUNT' in src_meta.keys() else '-',
                                                "BYTES":src_meta['BYTES'] if 'BYTES' in src_meta.keys() else '-',
                                                "CREATED":src_meta['CREATED'] if 'CREATED' in src_meta.keys() else '-',
                                                "LAST_ALTERED":src_meta['LAST_ALTERED'] if 'LAST_ALTERED' in src_meta.keys() else '-',
                                                "LAST_DDL":src_meta['LAST_DDL'] if 'LAST_DDL' in src_meta.keys() else '-',
                                                "LAST_DDL_BY":src_meta['LAST_DDL_BY'] if 'LAST_DDL_BY' in src_meta.keys() else '-',
                                                "IS_ICEBERG":src_meta['IS_ICEBERG'] if 'IS_ICEBERG' in src_meta.keys() else '-',
                                                "IS_TRANSIENT":src_meta['IS_TRANSIENT'] if 'IS_TRANSIENT' in src_meta.keys() else '-',
                                                "USER":src_meta['USER'] if 'USER' in src_meta.keys() else '-',
                                                "IS_DROPPED":s_del_meta['SOURCE_DELETE_FLAG'] if 'SOURCE_DELETE_FLAG' in s_del_meta.keys() else 'NOT_DROPPED',
                                                "DELETE_DATE":s_del_meta['DELETE_DATE'] if 'DELETE_DATE' in s_del_meta.keys() else '-',
                                                "DELETED_BY":s_del_meta['USER'] if 'USER' in s_del_meta.keys() else '-',
                                                "TYPE":row.s_table_type,
                                                "priv":row.s_user_privilege,
                                                "recon_score":'-',
                                                "LOAD_TYPE":src_meta['LOAD_TYPE'] if 'LOAD_TYPE' in src_meta.keys() else '-',
                                                "PIPELINE_TYPE": src_meta['PIPELINE_TYPE'] if 'PIPELINE_TYPE' in src_meta.keys() else '-',
                                                "SCHD_FREQUENCY": src_meta['SCHD_FREQUENCY'] if 'SCHD_FREQUENCY' in src_meta.keys() else '-',
                                                "SCHD_PLAN": src_meta['SCHD_PLAN'] if 'SCHD_PLAN' in src_meta.keys() else '-',
                                                "API_URL":src_meta['API_URL'] if 'API_URL' in src_meta.keys() else '-',
                                                "REQ_METHOD":src_meta['REQ_METHOD'] if 'REQ_METHOD' in src_meta.keys() else '-',
                                                "STATUS":src_meta['STATUS'] if 'STATUS' in src_meta.keys() else '-',
                                                "S3_PATH":src_meta['S3_PATH'] if 'S3_PATH' in src_meta.keys() else '-',
                                                "FILE_NAME":src_meta['FILE_NAME'] if 'FILE_NAME' in src_meta.keys() else '-',
                                                "FILE_PATH":src_meta['FILE_PATH'] if 'FILE_PATH' in src_meta.keys() else '-',
                                                "LOAD_FREQUENCY":src_meta['LOAD_FREQUENCY'] if 'LOAD_FREQUENCY' in src_meta.keys() else '-',
                                                "props": []}
            if row.target_object_name not in nodes:
                    # nodes[target] = {"name": target}
                target_id = str(uuid.uuid4())
                target_lvl = nodes[row.source_object_name]["level"] + 1  # random.choice([i for i in trgt_lvl if i > source_lvl])
                nodes[row.target_object_name] = {"id": target_id,
                                                "table_name": row.target_object_name,
                                                "label": row.target_table,
                                                "db": row.target_database,
                                                "schema": row.target_schema,
                                                "level": target_lvl,
                                                "TABLE_OWNER":tgt_meta['TABLE_OWNER'] if 'TABLE_OWNER' in tgt_meta.keys() else '-',
                                                "TABLE_TYPE":tgt_meta['TABLE_TYPE'] if 'TABLE_TYPE' in tgt_meta.keys() else '-',
                                                "ROW_COUNT":tgt_meta['ROW_COUNT'] if 'ROW_COUNT' in tgt_meta.keys() else '-',
                                                "BYTES":tgt_meta['BYTES'] if 'BYTES' in tgt_meta.keys() else '-',
                                                "CREATED":tgt_meta['CREATED'] if 'CREATED' in tgt_meta.keys() else '-',
                                                "LAST_ALTERED":tgt_meta['LAST_ALTERED'] if 'LAST_ALTERED' in tgt_meta.keys() else '-',
                                                "LAST_DDL":tgt_meta['LAST_DDL'] if 'LAST_DDL' in tgt_meta.keys() else '-',
                                                "LAST_DDL_BY":tgt_meta['LAST_DDL_BY'] if 'LAST_DDL_BY' in tgt_meta.keys() else '-',
                                                "IS_ICEBERG":tgt_meta['IS_ICEBERG'] if 'IS_ICEBERG' in tgt_meta.keys() else '-',
                                                "IS_TRANSIENT":tgt_meta['IS_TRANSIENT'] if 'IS_TRANSIENT' in tgt_meta.keys() else '-',
                                                "USER":tgt_meta['USER'] if 'USER' in tgt_meta.keys() else '-',
                                                "IS_DROPPED":t_del_meta['TARGET_DELETE_FLAG'] if 'TARGET_DELETE_FLAG' in t_del_meta.keys() else 'NOT_DROPPED',
                                                "DELETE_DATE":t_del_meta['DELETE_DATE'] if 'DELETE_DATE' in t_del_meta.keys() else '-',
                                                "DELETED_BY":t_del_meta['USER'] if 'USER' in t_del_meta.keys() else '-',
                                                "TYPE":row.t_table_type,
                                                "priv":row.t_user_privilege,
                                                "recon_score":'-',
                                                "LOAD_TYPE": '-',
                                                "PIPELINE_TYPE": '-',
                                                "SCHD_FREQUENCY":  '-',
                                                "SCHD_PLAN":  '-',
                                                "API_URL": '-',
                                                "REQ_METHOD": '-',
                                                "STATUS": '-',
                                                "S3_PATH":'-',
                                                "FILE_NAME": '-',
                                                "FILE_PATH": '-',
                                                "LOAD_FREQUENCY": '-',
                                                "props": []}
           
            edges.append({"from": nodes[row.source_object_name]["id"], "to": nodes[row.target_object_name]["id"],"source_obj":nodes[row.source_object_name]["table_name"],"target_obj":nodes[row.target_object_name]["table_name"],"mapping":row.s_t_column_mapping})
            # Create the JSON data
    else:
        for ind,row in complete_dataset[(complete_dataset.source_object_name == table_name)].dropna(subset = ['source_object_name', 'target_object_name']).iterrows():
            #type,source,target,cols = row
       
            src_meta = row.source_metadata
            tgt_meta = row.target_metadata
            s_del_meta = row.s_delete_metadata
            t_del_meta = row.t_delete_metadata
            if row.source_object_name not in nodes:
                source_id = str(uuid.uuid4())
                source_lvl = 0
                nodes[row.source_object_name] = {"id": source_id,
                                                "table_name":row.source_object_name,
                                                "label": row.source_table,
                                                "db": row.source_database,
                                                "schema": row.source_schema,
                                                "level": source_lvl,
                                                "TABLE_OWNER":src_meta['TABLE_OWNER'] if 'TABLE_OWNER' in src_meta.keys() else '-',
                                                "TABLE_TYPE":src_meta['TABLE_TYPE'] if 'TABLE_TYPE' in src_meta.keys() else '-',
                                                "ROW_COUNT":src_meta['ROW_COUNT'] if 'ROW_COUNT' in src_meta.keys() else '-',
                                                "BYTES":src_meta['BYTES'] if 'BYTES' in src_meta.keys() else '-',
                                                "CREATED":src_meta['CREATED'] if 'CREATED' in src_meta.keys() else '-',
                                                "LAST_ALTERED":src_meta['LAST_ALTERED'] if 'LAST_ALTERED' in src_meta.keys() else '-',
                                                "LAST_DDL":src_meta['LAST_DDL'] if 'LAST_DDL' in src_meta.keys() else '-',
                                                "LAST_DDL_BY":src_meta['LAST_DDL_BY'] if 'LAST_DDL_BY' in src_meta.keys() else '-',
                                                "IS_ICEBERG":src_meta['IS_ICEBERG'] if 'IS_ICEBERG' in src_meta.keys() else '-',
                                                "IS_TRANSIENT":src_meta['IS_TRANSIENT'] if 'IS_TRANSIENT' in src_meta.keys() else '-',
                                                "USER":src_meta['USER'] if 'USER' in src_meta.keys() else '-',
                                                "IS_DROPPED":s_del_meta['SOURCE_DELETE_FLAG'] if 'SOURCE_DELETE_FLAG' in s_del_meta.keys() else 'NOT_DROPPED',
                                                "DELETE_DATE":s_del_meta['DELETE_DATE'] if 'DELETE_DATE' in s_del_meta.keys() else '-',
                                                "DELETED_BY":s_del_meta['USER'] if 'USER' in s_del_meta.keys() else '-',
                                                "TYPE":row.s_table_type,
                                                "priv":row.s_user_privilege,
                                                "recon_score":'-',
                                                "LOAD_TYPE":src_meta['LOAD_TYPE'] if 'LOAD_TYPE' in src_meta.keys() else '-',
                                                "PIPELINE_TYPE": src_meta['PIPELINE_TYPE'] if 'PIPELINE_TYPE' in src_meta.keys() else '-',
                                                "SCHD_FREQUENCY": src_meta['SCHD_FREQUENCY'] if 'SCHD_FREQUENCY' in src_meta.keys() else '-',
                                                "SCHD_PLAN": src_meta['SCHD_PLAN'] if 'SCHD_PLAN' in src_meta.keys() else '-',
                                                "API_URL":src_meta['API_URL'] if 'API_URL' in src_meta.keys() else '-',
                                                "REQ_METHOD":src_meta['REQ_METHOD'] if 'REQ_METHOD' in src_meta.keys() else '-',
                                                "STATUS":src_meta['STATUS'] if 'STATUS' in src_meta.keys() else '-',
                                                "S3_PATH":src_meta['S3_PATH'] if 'S3_PATH' in src_meta.keys() else '-',
                                                "FILE_NAME":src_meta['FILE_NAME'] if 'FILE_NAME' in src_meta.keys() else '-',
                                                "FILE_PATH":src_meta['FILE_PATH'] if 'FILE_PATH' in src_meta.keys() else '-',
                                                "LOAD_FREQUENCY":src_meta['LOAD_FREQUENCY'] if 'LOAD_FREQUENCY' in src_meta.keys() else '-',
                                                "props": []}
            if row.target_object_name not in nodes:
                    # nodes[target] = {"name": target}
                target_id = str(uuid.uuid4())
                target_lvl = nodes[row.source_object_name]["level"] + 1  # random.choice([i for i in trgt_lvl if i > source_lvl])
                nodes[row.target_object_name] = {"id": target_id,
                                                "table_name": row.target_object_name,
                                                "label": row.target_table,
                                                "db": row.target_database,
                                                "schema": row.target_schema,
                                                "level": target_lvl,
                                                "TABLE_OWNER":tgt_meta['TABLE_OWNER'] if 'TABLE_OWNER' in tgt_meta.keys() else '-',
                                                "TABLE_TYPE":tgt_meta['TABLE_TYPE'] if 'TABLE_TYPE' in tgt_meta.keys() else '-',
                                                "ROW_COUNT":tgt_meta['ROW_COUNT'] if 'ROW_COUNT' in tgt_meta.keys() else '-',
                                                "BYTES":tgt_meta['BYTES'] if 'BYTES' in tgt_meta.keys() else '-',
                                                "CREATED":tgt_meta['CREATED'] if 'CREATED' in tgt_meta.keys() else '-',
                                                "LAST_ALTERED":tgt_meta['LAST_ALTERED'] if 'LAST_ALTERED' in tgt_meta.keys() else '-',
                                                "LAST_DDL":tgt_meta['LAST_DDL'] if 'LAST_DDL' in tgt_meta.keys() else '-',
                                                "LAST_DDL_BY":tgt_meta['LAST_DDL_BY'] if 'LAST_DDL_BY' in tgt_meta.keys() else '-',
                                                "IS_ICEBERG":tgt_meta['IS_ICEBERG'] if 'IS_ICEBERG' in tgt_meta.keys() else '-',
                                                "IS_TRANSIENT":tgt_meta['IS_TRANSIENT'] if 'IS_TRANSIENT' in tgt_meta.keys() else '-',
                                                "USER":tgt_meta['USER'] if 'USER' in tgt_meta.keys() else '-',
                                                "IS_DROPPED":t_del_meta['TARGET_DELETE_FLAG'] if 'TARGET_DELETE_FLAG' in t_del_meta.keys() else 'NOT_DROPPED',
                                                "DELETE_DATE":t_del_meta['DELETE_DATE'] if 'DELETE_DATE' in t_del_meta.keys() else '-',
                                                "DELETED_BY":t_del_meta['USER'] if 'USER' in t_del_meta.keys() else '-',
                                                "TYPE":row.t_table_type,
                                                "priv":row.t_user_privilege,
                                                "recon_score":'-',
                                                "LOAD_TYPE": '-',
                                                "PIPELINE_TYPE": '-',
                                                "SCHD_FREQUENCY":  '-',
                                                "SCHD_PLAN":  '-',
                                                "API_URL": '-',
                                                "REQ_METHOD": '-',
                                                "STATUS": '-',
                                                "S3_PATH":'-',
                                                "FILE_NAME": '-',
                                                "FILE_PATH": '-',
                                                "LOAD_FREQUENCY": '-',
                                                "props": []}
           
            edges.append({"from": nodes[row.source_object_name]["id"], "to": nodes[row.target_object_name]["id"],"source_obj":nodes[row.source_object_name]["table_name"],"target_obj":nodes[row.target_object_name]["table_name"],"mapping":row.s_t_column_mapping})
            # Create the JSON data


    graph_data = {"nodes": list(nodes.values()), "edges": edges}
   


    return graph_data






def expand_visjs_data(table,node_id,level,connected_nodes):
    connected_nodes_lst = connected_nodes.split(',')
    nodes = {}
    edges = []
    complete_data_qry = f"""SELECT * FROM consumption.data_lineage_master_data
                       where source_object_name = '{table}';"""
    complete_dataset = postgresql_query(complete_data_qry,credentialsDict_pg_2)[2]


   
   
    for ind,row in complete_dataset[(complete_dataset.source_object_name == table) & (complete_dataset.source_object_name != complete_dataset.target_object_name)].dropna(subset = ['source_object_name', 'target_object_name']).iterrows():
        #type,source,target,cols = row
        tgt_meta = row.target_metadata
        t_del_meta = row.t_delete_metadata
       
        if row.target_object_name not in nodes and row.target_object_name not in connected_nodes_lst:
                # nodes[target] = {"name": target}
            target_id = str(uuid.uuid4())
            target_lvl =  int(level) + 1  # random.choice([i for i in trgt_lvl if i > source_lvl])
            nodes[row.target_object_name] = {"id": target_id,
                                             "table_name": row.target_object_name,
                                             "label": row.target_table,
                                             "db": row.target_database,
                                             "schema": row.target_schema,
                                             "level": target_lvl,
                                             "TABLE_OWNER":tgt_meta['TABLE_OWNER'] if 'TABLE_OWNER' in tgt_meta.keys() else '-',
                                             "TABLE_TYPE":tgt_meta['TABLE_TYPE'] if 'TABLE_TYPE' in tgt_meta.keys() else '-',
                                             "ROW_COUNT":tgt_meta['ROW_COUNT'] if 'ROW_COUNT' in tgt_meta.keys() else '-',
                                             "BYTES":tgt_meta['BYTES'] if 'BYTES' in tgt_meta.keys() else '-',
                                             "CREATED":tgt_meta['CREATED'] if 'CREATED' in tgt_meta.keys() else '-',
                                             "LAST_ALTERED":tgt_meta['LAST_ALTERED'] if 'LAST_ALTERED' in tgt_meta.keys() else '-',
                                             "LAST_DDL":tgt_meta['LAST_DDL'] if 'LAST_DDL' in tgt_meta.keys() else '-',
                                             "LAST_DDL_BY":tgt_meta['LAST_DDL_BY'] if 'LAST_DDL_BY' in tgt_meta.keys() else '-',
                                             "IS_ICEBERG":tgt_meta['IS_ICEBERG'] if 'IS_ICEBERG' in tgt_meta.keys() else '-',
                                             "IS_TRANSIENT":tgt_meta['IS_TRANSIENT'] if 'IS_TRANSIENT' in tgt_meta.keys() else '-',
                                             "USER":tgt_meta['USER'] if 'USER' in tgt_meta.keys() else '-',
                                             "IS_DROPPED":t_del_meta['TARGET_DELETE_FLAG'] if 'TARGET_DELETE_FLAG' in t_del_meta.keys() else 'NOT_DROPPED',
                                             "DELETE_DATE":t_del_meta['DELETE_DATE'] if 'DELETE_DATE' in t_del_meta.keys() else '-',
                                             "DELETED_BY":t_del_meta['USER'] if 'USER' in t_del_meta.keys() else '-',
                                             "TYPE":row.t_table_type,
                                             "priv":row.t_user_privilege,
                                             "recon_score":'-',
                                             "LOAD_TYPE": '-',
                                                "PIPELINE_TYPE": '-',
                                                "SCHD_FREQUENCY":  '-',
                                                "SCHD_PLAN":  '-',
                                                "API_URL": '-',
                                                "REQ_METHOD": '-',
                                                "STATUS": '-',
                                                "S3_PATH":'-',
                                                "FILE_NAME": '-',
                                                "FILE_PATH": '-',
                                                "LOAD_FREQUENCY": '-',
                                               "props": []}
            edges.append({"from": node_id, "to": nodes[row.target_object_name]["id"],"source_obj":table,"target_obj":nodes[row.target_object_name]["table_name"],"mapping":row.s_t_column_mapping})
        # Create the JSON data
    graph_data = {"nodes": list(nodes.values()), "edges": edges}
    return graph_data
   
def expand_visjs_data_back(table,node_id,level,connected_nodes):
    connected_nodes_lst = connected_nodes.split(',')
    nodes = {}
    edges = []
    complete_data_qry = f"""SELECT * FROM consumption.data_lineage_master_data
                       where target_object_name = '{table}';"""
    complete_dataset = postgresql_query(complete_data_qry,credentialsDict_pg_2)[2]


   
    for ind,row in complete_dataset[(complete_dataset.target_object_name == table) & (complete_dataset.source_object_name != complete_dataset.target_object_name)].dropna(subset = ['source_object_name', 'target_object_name']).iterrows():
        #type,source,target,cols = row
        src_meta = row.source_metadata
        s_del_meta = row.s_delete_metadata
       
        if row.source_object_name not in nodes and row.source_object_name not in connected_nodes_lst:
            source_id = str(uuid.uuid4())
            source_lvl = int(level) -1
            nodes[row.source_object_name] = {"id": source_id,
                                             "table_name":row.source_object_name,
                                             "label": row.source_table,
                                             "db": row.source_database,
                                             "schema": row.source_schema,
                                             "level": source_lvl,
                                             "TABLE_OWNER":src_meta['TABLE_OWNER'] if 'TABLE_OWNER' in src_meta.keys() else '-',
                                             "TABLE_TYPE":src_meta['TABLE_TYPE'] if 'TABLE_TYPE' in src_meta.keys() else '-',
                                             "ROW_COUNT":src_meta['ROW_COUNT'] if 'ROW_COUNT' in src_meta.keys() else '-',
                                             "BYTES":src_meta['BYTES'] if 'BYTES' in src_meta.keys() else '-',
                                             "CREATED":src_meta['CREATED'] if 'CREATED' in src_meta.keys() else '-',
                                             "LAST_ALTERED":src_meta['LAST_ALTERED'] if 'LAST_ALTERED' in src_meta.keys() else '-',
                                             "LAST_DDL":src_meta['LAST_DDL'] if 'LAST_DDL' in src_meta.keys() else '-',
                                             "LAST_DDL_BY":src_meta['LAST_DDL_BY'] if 'LAST_DDL_BY' in src_meta.keys() else '-',
                                             "IS_ICEBERG":src_meta['IS_ICEBERG'] if 'IS_ICEBERG' in src_meta.keys() else '-',
                                             "IS_TRANSIENT":src_meta['IS_TRANSIENT'] if 'IS_TRANSIENT' in src_meta.keys() else '-',
                                             "USER":src_meta['USER'] if 'USER' in src_meta.keys() else '-',
                                             "IS_DROPPED":s_del_meta['SOURCE_DELETE_FLAG'] if 'SOURCE_DELETE_FLAG' in s_del_meta.keys() else 'NOT_DROPPED',
                                             "DELETE_DATE":s_del_meta['DELETE_DATE'] if 'DELETE_DATE' in s_del_meta.keys() else '-',
                                             "DELETED_BY":s_del_meta['USER'] if 'USER' in s_del_meta.keys() else '-',
                                             "TYPE":row.s_table_type,
                                             "priv":row.s_user_privilege,
                                             "recon_score":'-',
                                             "LOAD_TYPE":src_meta['LOAD_TYPE'] if 'LOAD_TYPE' in src_meta.keys() else '-',
                                                "PIPELINE_TYPE": src_meta['PIPELINE_TYPE'] if 'PIPELINE_TYPE' in src_meta.keys() else '-',
                                                "SCHD_FREQUENCY": src_meta['SCHD_FREQUENCY'] if 'SCHD_FREQUENCY' in src_meta.keys() else '-',
                                                "SCHD_PLAN": src_meta['SCHD_PLAN'] if 'SCHD_PLAN' in src_meta.keys() else '-',
                                                "API_URL":src_meta['API_URL'] if 'API_URL' in src_meta.keys() else '-',
                                                "REQ_METHOD":src_meta['REQ_METHOD'] if 'REQ_METHOD' in src_meta.keys() else '-',
                                                "STATUS":src_meta['STATUS'] if 'STATUS' in src_meta.keys() else '-',
                                                "S3_PATH":src_meta['S3_PATH'] if 'S3_PATH' in src_meta.keys() else '-',
                                                "FILE_NAME":src_meta['FILE_NAME'] if 'FILE_NAME' in src_meta.keys() else '-',
                                                "FILE_PATH":src_meta['FILE_PATH'] if 'FILE_PATH' in src_meta.keys() else '-',
                                                "LOAD_FREQUENCY":src_meta['LOAD_FREQUENCY'] if 'LOAD_FREQUENCY' in src_meta.keys() else '-',
                                               "props": []}
            edges.append({"from": nodes[row.source_object_name]["id"], "to": node_id,"source_obj":nodes[row.source_object_name]["table_name"],"target_obj":table,"mapping":row.s_t_column_mapping})
        # Create the JSON data
    graph_data = {"nodes": list(nodes.values()), "edges": edges}
    return graph_data


def get_label(columns):


    final_title = ""
    try:
        unique_columns = set()  # Initialize a set to store unique column names
        for col in columns.split(','):
            col = col.strip()  # Remove leading and trailing whitespaces
            if col not in unique_columns:
                final_title += col + os.linesep
                unique_columns.add(col)
        return final_title
    except:
        return final_title
   


def get_data_definition(SOURCE_OBJECT,TARGET_OBJECT,S_T_COLUMN):
    try:


    #Get DATA_LINEAGE_MASTER_DATA
        data_lineage_qry = f'''
                select TARGET_DATABASE,TARGET_SCHEMA,TARGET_TABLE,S_T_COLUMN_MAPPING
                from consumption.data_lineage_master_data
                where SOURCE_OBJECT_NAME = '{SOURCE_OBJECT}'
                and TARGET_OBJECT_NAME = '{TARGET_OBJECT}'
                and S_T_COLUMN_MAPPING = '{S_T_COLUMN}'
                limit 1
            '''
        df_data_lineage = postgresql_query(data_lineage_qry,credentialsDict_pg_2)[2]
       
       
        #Get SYSTEM_INFORMATION_DATA_DICTIONARY
        info_schema_qry = "SELECT 'PROD' AS DATABASE , sm.schema_name as SCHEMA, tm.table_name as TABLE, cm.column_name as column_name,cm.id as cm_id,dd.description as description , COALESCE(dd.classification,'NA') as classification FROM system_information.schema_master sm LEFT JOIN system_information.table_master tm on sm.id=tm.scm_id LEFT JOIN system_information.column_master cm on cm.tm_id = tm.id LEFT JOIN data_catalog.data_dictionary dd on dd.cm_id = cm.id"
       
        df_info_schema = postgresql_query(info_schema_qry,credentialsDict_pg)[2]


        bg_info_schema_qry = '''
                SELECT 'PROD' AS DATABASE, "SCHEMA_NAME" as SCHEMA, "TABLE_NAME" as TABLE,"COLUMN_NAME"  as column_name,
                        COALESCE("COLUMN_TYPE",'NA') as COLUMN_TYPE ,COALESCE("COLUMN_OWNER",'NA') as COLUMN_OWNER , COALESCE("COLUMN_DESC",'NA') as COLUMN_DESCRIPTION , COALESCE("COLUMN_LOGIC",'NA') as COLUMN_LOGIC
                FROM consumption."DATA_LINEAGE_BUSINESS_GLOSSARY";
            '''
        df_bg_info_schema = postgresql_query(bg_info_schema_qry,credentialsDict_pg_2)[2]


        # Split s_t_column_mapping into separate rows
        df_data_lineage = df_data_lineage.assign(s_t_column_mapping=df_data_lineage['s_t_column_mapping'].str.split(','))
        df_data_lineage = df_data_lineage.explode('s_t_column_mapping')


        # # Extract source and target columns
        split_columns = df_data_lineage['s_t_column_mapping'].str.split(' -> ', expand=True)
        df_data_lineage['source_column'] = split_columns[0]
        df_data_lineage['target_column'] = split_columns[1].str.strip().str.upper()  # Convert to uppercase and remove spaces


       
        df_merged = pd.merge(df_data_lineage, df_info_schema, how='left',
                        left_on=['target_database', 'target_schema', 'target_table', 'target_column'],
                        right_on=['database', 'schema', 'table', 'column_name'])
       


        df_merged_bg = pd.merge(df_merged, df_bg_info_schema, how='left',
                        left_on=['target_database', 'target_schema', 'target_table', 'target_column'],
                        right_on=['database', 'schema', 'table', 'column_name'])
       
        #df_merged_bg['description'].apply(lambda x: np.nan if x is None else x)
        df_merged_bg['description'] = df_merged_bg['description'].replace({None: np.nan}).fillna(df_merged_bg['column_description'])
       
        # drop unwanted columns
        df_merged.drop(['s_t_column_mapping','database', 'schema', 'table'],axis = 1,inplace=True)
        #df_merged['cm_id'] = df_merged['cm_id'].astype(int)
        #print(df_merged_bg[['source_column','target_column','description','column_type','column_logic']])
        print(df_merged_bg[['source_column','target_column','description','column_type','column_logic']])
       
       
        return df_merged_bg[['source_column','target_column','description','column_type','column_logic']]
    except:
        return pd.DataFrame(columns=['source_column', 'target_column', 'description', 'column_type', 'column_logic'])


def get_recon_data(TARGET_OBJECT):
    T_DATABASE, T_SCHEMA, T_TABLE = TARGET_OBJECT.split('.')
    recon_data_qry = f'''
            select * from (
            select  case when SOURCE_COUNT <> 0 and TARGET_COUNT <> 0
            then ROUND((case when SOURCE_COUNT < TARGET_COUNT then SOURCE_COUNT::decimal else TARGET_COUNT::decimal end
            / case when SOURCE_COUNT < TARGET_COUNT then TARGET_COUNT::decimal else SOURCE_COUNT::decimal end) * 100, 2) end recocile_score,
            row_number() over (partition by TARGET_TABLE order by report_time desc) R,
            SOURCE_COUNT,target_count ,*,
            TO_char(report_time::timestamp, 'DD-MM-YYYY')
            FROM public.comparator_report_table
            where
            TO_char(report_time::timestamp, 'DD-MM-YYYY') = '11-03-2024'
            order by report_time  desc)
            where R=1
            and target_schema = '{T_SCHEMA}'
            and target_table = '{T_TABLE}'
            and target = 'SNOWFLAKE'
        '''
    df_recon_data = postgresql_query(recon_data_qry,credentialsDict_pg_2)[2]
 
    try :
        recon_data = [df_recon_data['recocile_score'].values[0]]
        reconcile_score = str(recon_data[0])
    except:
        reconcile_score = '-'
 
    return reconcile_score