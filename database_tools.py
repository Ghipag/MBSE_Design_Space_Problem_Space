import pandas as pd

def process_relationships(data,sourceType,targetKey,targetType,relationshipName,direction,graph):
    """
    Create relationships between nodes in the Neo4j graph based on provided data.

    This function creates relationships between nodes of a given source type and nodes of a given target type
    based on a specific key in the provided data DataFrame. The direction of the relationship (OUTGOING or INCOMING)
    is determined by the 'direction' argument.

    Args:
        data (pandas.DataFrame): A DataFrame containing data with columns 'Name' and the specified 'targetKey'.
        sourceType (str): The label of the source node type.
        targetKey (str): The key in the data DataFrame used to determine relationships.
        targetType (str): The label of the target node type.
        relationshipName (str): The name of the relationship to be created.
        direction (str): The direction of the relationship, either 'OUTGOING' or 'INCOMING'.
        graph: The Neo4j graph database connection.

    Returns:
        None
    """
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
    """
    Generate a Cypher query to match nodes with specified names.

    This function generates a Cypher query to match nodes in a Neo4j graph database with the provided names.
    The names are assumed to be unique identifiers (uids) for the nodes.

    Args:
        namelist (list): A list of node names (uids) to be matched.

    Returns:
        str: A Cypher query string that matches the specified nodes.
    """
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
    """
    Divide a list into batches of a specified size.

    This function takes a list and divides it into smaller batches of a specified size.
    It returns a list of tuples, where each tuple contains the starting index and the batch of items.

    Args:
        lst (list): The input list to be divided into batches.
        batch_size (int, optional): The size of each batch. Defaults to 100.

    Returns:
        list: A list of tuples where each tuple represents a batch of items along with its starting index.
    """
    return [(i, lst[i:i + batch_size]) for i in range(0, len(lst), batch_size)]

def clear_database(graph):
    """
    Clear all nodes and relationships from the Neo4j database.

    This function sends a query to Neo4j to match and detach-delete all nodes and their relationships
    in the graph, effectively clearing the entire database.

    Args:
        graph: The Neo4j graph database object.
    """
    query = """
        MATCH(n) DETACH DELETE n
    """
    graph.run(query)

def run_neo_query(data, query,graph):
    """
    Run a Neo4j query with optional batching.

    This function runs a Neo4j query with optional batching of data to avoid memory issues
    when inserting a large number of records into the database.

    Args:
        data (list): The data to be used in the query.
        query (str): The Neo4j query string.
        graph: The Neo4j graph database object.

    Returns:
        None
    """
    batches = get_batches(data)

    for index, batch in batches:
        print('[Batch: %s] Will add %s node(s) to Graph' % (index, len(batch)))
        graph.run(query, rows=batch)