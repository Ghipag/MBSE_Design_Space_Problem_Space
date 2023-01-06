import pandas as pd

def process_relationships(data,sourceType,targetKey,targetType,relationshipName,direction,graph):
    # adding relationships to available tools
    relation_data = data[['Name',targetKey]]
    relation_data =  relation_data.dropna()

    s = relation_data[targetKey].str.split(';').apply(pd.Series, 1).stack()
    s.name = targetKey
    del relation_data[targetKey]
    s = s.to_frame().reset_index()
    relation_data = pd.merge(relation_data, s, right_on='level_0', left_index = True)

    del relation_data["level_0"]
    del relation_data["level_1"]
    relation_data = list(relation_data.T.to_dict().values())

    if direction == "OUTGOING":
        query = """
            UNWIND $rows AS row
            MERGE (source:"""+ sourceType + """ {uid:row.Name})
            MERGE (target:"""+ targetType + """ {uid:row."""+targetKey+"""})
            CREATE (source)-[r:"""+ relationshipName+"""]->(target)
                    SET r.Number_of_related_issues = 0    
                    SET r.Issue_Cost = 0       
        """
    else:
        query = """
            UNWIND $rows AS row
            MERGE (source:"""+ sourceType + """ {uid:row.Name})
            MERGE (target:"""+ targetType + """ {uid:row."""+targetKey+"""})
            CREATE (target)-[r:"""+ relationshipName+"""]->(source)
                    SET r.Number_of_related_issues = 0
                    SET r.Issue_Cost = 0           
        """
    run_neo_query(relation_data,query,graph)
    
def generate_node_match_query(namelist):
    query = ""
    for name in namelist:
        query = query + "MATCH("+name.replace(" ","_")+"{uid:'"+name+"'}) "

    query = query + "RETURN "
    
    for name in namelist:
        query = query + name.replace(" ","_")+","

    print('path query:\n')
    print(query[:-1])
    print('\n')

    return query[:-1]

def get_batches(lst, batch_size=100):
    return [(i, lst[i:i + batch_size]) for i in range(0, len(lst), batch_size)]

def clear_database(graph):
    query = """
        MATCH(n) DETACH DELETE n
    """
    graph.run(query)

def run_neo_query(data, query,graph):
    batches = get_batches(data)

    for index, batch in batches:
        print('[Batch: %s] Will add %s node(s) to Graph' % (index, len(batch)))
        graph.run(query, rows=batch)