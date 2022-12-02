import pandas as pd
from py2neo import Graph


#####################################################################
# Graph database config
#####################################################################

# Set up a link to the local graph database.
# Ideally get password from ENV variable
# graph = Graph(getenv("NEO4J_URL"), auth=(getenv("NEO4J_UID"), getenv("NEO4J_PASSWORD")))
graph = Graph("bolt://127.0.0.1:7687", auth=('neo4j', 'test'))

# Add uniqueness constraints.
#graph.run("CREATE CONSTRAINT FOR (l:Language) REQUIRE l.uid IS UNIQUE;")
#graph.run("CREATE CONSTRAINT FOR (t:Tool) REQUIRE t.uid IS UNIQUE;")
#graph.run("CREATE CONSTRAINT FOR (m:MajorStream) REQUIRE m.name IS UNIQUE;")
#graph.run("CREATE CONSTRAINT FOR (w:WorkType) REQUIRE w.name IS UNIQUE;")



def read_data(name):
    data = pd.read_csv(

        f"./data/{name}_info.csv",
        low_memory=False)
    print("Column name of data : ", data.columns)
    return data


def process_language_data(data):
    user_data = data[['Name','Developer','Year_of_latest_release', 'Tool', 'Method', 'Variability_Modelling', 'Simulation_Links', 'Customisation']]
    user_data =  user_data.dropna()

    # Convert data frame to list of dictionaries
    # Neo4j UNWIND query expects a list of dictionaries
    # for bulk insertion
    user_data = list(user_data.T.to_dict().values())
    print(user_data)

    query = """
            UNWIND $rows AS row

            MERGE (language:Language {uid:row.Name})
            ON CREATE SET 
                language.Developer = row.Developer,
                language.Year_of_latest_release = row.Year_of_latest_release,
                language.Tool = row.Tool,
                language.Method = row.Method,
                language.Variability_Modelling = row.Variability_Modelling,
                language.Simulation_Links = row.Simulation_Links,
                language.Customisation = row.Customisation
            MERGE (tool:Tool {uid:row.Tool})
            CREATE (language)-[r:AVAILABLE_IN]->(tool)
            MERGE (method:Method {uid:row.Method})
            CREATE (tool)-[r1:CAN_FOLLOW]->(method)
        """

    run_neo_query(user_data,query)

def process_tool_data(data):
    user_data = data[['Name','Developer','Year_of_latest_release', 'Language', 'Method', 'Simulation', 'Customisation']]
    user_data =  user_data.dropna()

    # Convert data frame to list of dictionaries
    # Neo4j UNWIND query expects a list of dictionaries
    # for bulk insertion
    user_data = list(user_data.T.to_dict().values())
    print(user_data)

    query = """
            UNWIND $rows AS row

            MERGE (tool:Tool {uid:row.Name})
            ON CREATE SET 
                tool.Developer = row.Developer,
                tool.Year_of_latest_release = row.Year_of_latest_release,
                tool.Language = row.Language,
                tool.Method = row.Method,
                tool.Simulation_Links = row.Simulation_Links,
                tool.Customisation = row.Customisation
        """

    run_neo_query(user_data,query)
    query = """
            MERGE (language:Language {uid:row.Language})
            CREATE (tool)-[r:CAN_USE]->(language)
        """
        
    run_neo_query(user_data,query)
    query = """
            MERGE (method:Method {uid:row.Method})
            CREATE (language)-[r1:CAN_BE_IMPLEMENTED_IN]->(method)
        """

    run_neo_query(user_data,query)

def process_method_data(data):
    user_data = data[['Name','Developer','Year_of_latest_release', 'Language', 'Tool', 'Design_Space_Exploration']]
    user_data =  user_data.dropna()

    # Convert data frame to list of dictionaries
    # Neo4j UNWIND query expects a list of dictionaries
    # for bulk insertion
    user_data = list(user_data.T.to_dict().values())
    print(user_data)

    query = """
            UNWIND $rows AS row

            MERGE (method:Method {uid:row.Name})
            ON CREATE SET 
                method.Developer = row.Developer,
                method.Year_of_latest_release = row.Year_of_latest_release,
                method.Language = row.Language,
                method.Tool = row.Tool,
                method.Design_Space_Exploration = row.Design_Space_Exploration
            MERGE (language:Language {uid:row.Language})
            CREATE (method)-[r:CAN_BE_WRITTEN_IN]->(language)
            MERGE (tool:Tool {uid:row.Tool})
            CREATE (method)-[r2:CAN_BE_PERFORMED_IN]->(tool)
        """

    run_neo_query(user_data,query)


def run_neo_query(data, query):
    batches = get_batches(data)

    for index, batch in batches:
        print('[Batch: %s] Will add %s node to Graph' % (index, len(batch)))
        print(batch)
        graph.run(query, rows=batch)


def get_batches(lst, batch_size=100):
    return [(i, lst[i:i + batch_size]) for i in range(0, len(lst), batch_size)]


if __name__== "__main__":
    language_data = read_data('Languages')
    process_language_data(language_data)
    #tool_data = read_data('Tools')
    #process_tool_data(tool_data)
    #method_data = read_data('Methods')
    #process_method_data(method_data)