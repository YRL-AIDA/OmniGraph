import re
import pandas as pd
import json
import numpy as np

from sql_graph_translate.seq_to_graph import parse
from sql_graph_translate.nodes import create_nodes
from sql_graph_translate.metrics import target_values_map, flexible_denotation_accuracy, to_value_list
from sql_graph_translate.utils import find_first_edges

def deserializ_tapex_linear_table(linear_table):
    
    head_pattern = r" col :(.*) row 1 : "
    row_pattern = r" row \d+ : "
    coll_delimetr = " | "
    heads = [h.strip() for h in 
             re.search(head_pattern,linear_table)[0].replace(" col : ","",1).replace(" row 1 : ","",1).split(coll_delimetr)]
    rows = [[r.strip() for r in row.split(coll_delimetr)] for row in re.split(row_pattern,linear_table)[1:]]
    return pd.DataFrame(rows,columns=heads)

def serialize_table_to_tapex_format(df):
    head_pattern = " col : "
    row_pattern = " row {num} : "
    coll_delimetr = " | "
    
    lin_table = head_pattern+coll_delimetr.join(df.columns)
    for i,row in df.iterrows():
        #print(row_pattern.format(num=i+1))
        lin_table+=row_pattern.format(num=i+1)+coll_delimetr.join(str(r) for r in row.values)
    
    return lin_table

def translate_query_to_graph_form(query,answer=None,flatten_mode = 'preorder',
                                  Omega_include=["P","C","S","GB","H","OB","A","OP","L"],task='tapex'):
    
    pattern = ' col : '
    try:
        sql,_ = query.split(pattern)
    except Exception as e:
        print(e)
    sql  = sql.strip()
    df = deserializ_tapex_linear_table(" col : "+query.split(" col : ")[1])
    
    new_column_names = [f'"{"_".join(head.split(" "))}"' for head in df.columns]
    #print(new_column_names)
    for head in sorted(set(df.columns),key=len, reverse=True):
        sql = sql.replace(head,f'{"_".join(head.split(" "))}')
    #print(sql)
    for head in set(df.columns):  
        #print(escape_special_characters("_".join(head.split(" "))))
        sql = re.sub(f' {escape_special_characters("_".join(head.split(" ")))} '
                     ,f' "{"_".join(head.split(" "))}" ',sql) 
        #sql = sql.replace(head,f'"{"_".join(head.split(" "))}"') 
        #print(sql)
    df.columns = new_column_names
    df['agg'] = np.zeros(df.shape[0])
    edges = []
    condi_expressions = {}
    G0 = create_nodes(sql, df=df,condi_expressions=condi_expressions,edges=edges)
    if task == 'tapex':
        sort_nodes = sort_graphe_execute_nodes(edges)
        #result = to_value_list(G0.executed_last_node())
        sort_nodes = sort_graphe_execute_nodes(edges)
        transformed_query = ' NODE '+f' NODE '.join(sort_nodes)
        return transformed_query+serialize_table_to_tapex_format(df), answer

def escape_special_characters(string):
    special_symbols = '.^$*+?{}[]\|()'
    return ''.join([f'\\{w}' if w in special_symbols else w for w in string])

def find_neighboors(node_name, edges):
    return set(edge[1] for edge in list(filter(lambda t: t[0] == node_name, edges)))

def find_layer_neighboors(parent_node_names,edges):
    layer_neighboors = []
    for node in parent_node_names:
        layer_neighboors.extend(find_neighboors(node,edges))
    return set(layer_neighboors)

def sort_graphe_execute_nodes(edges):
    sorted_nodes = []
    sorted_nodes.extend(find_first_edges(edges))
    layer_neighboors = sorted_nodes
    
    while len(layer_neighboors) > 0:
        print(layer_neighboors)
        layer_neighboors = find_layer_neighboors(layer_neighboors,edges)
        sorted_nodes.extend(layer_neighboors)
    return sorted_nodes