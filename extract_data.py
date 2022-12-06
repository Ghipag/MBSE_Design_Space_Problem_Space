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
#graph.run("CREATE CONSTRAINT FOR (m:Method) REQUIRE m.uid IS UNIQUE;")
#graph.run("CREATE CONSTRAINT FOR (w:WorkType) REQUIRE w.name IS UNIQUE;")



def read_data(name):
    data = pd.read_csv(

        f"./data/{name}_info.csv",
        low_memory=False)
    print("Column name of data : ", data.columns)
    return data


def process_language_data(data):
    language_data = data[['Name','Developer','Year_of_latest_release', 'Variability_Modelling', 'Simulation_Links', 'Customisation']]
    language_data =  language_data.dropna()

    # Convert data frame to list of dictionaries
    # Neo4j UNWIND query expects a list of dictionaries
    # for bulk insertion
    language_data = list(language_data.T.to_dict().values())
    print(language_data)

    query = """
            UNWIND $rows AS row

            MERGE (language:Language {uid:row.Name})
            SET 
                language.Developer = row.Developer,
                language.Year_of_latest_release = row.Year_of_latest_release,
                language.Variability_Modelling = row.Variability_Modelling,
                language.Simulation_Links = row.Simulation_Links,
                language.Customisation = row.Customisation
        """

    run_neo_query(language_data,query)

    # adding relationships to available tools
    tool_data = data[['Name','Tool']]
    tool_data =  tool_data.dropna()

    s = tool_data['Tool'].str.split(';').apply(pd.Series, 1).stack()
    s.name = "Tool"
    del tool_data["Tool"]
    s = s.to_frame().reset_index()
    tool_data = pd.merge(tool_data, s, right_on='level_0', left_index = True)

    del tool_data["level_0"]
    del tool_data["level_1"]
    tool_data = list(tool_data.T.to_dict().values())

    query = """
           UNWIND $rows AS row
           MERGE (language:Language {uid:row.Name})
           MERGE (tool:Tool {uid:row.Tool})
           CREATE (language)-[r:AVAILABLE_IN]->(tool)
           
       """

    run_neo_query(tool_data,query)

    # adding relationships to available methods
    method_data = data[['Name','Method']]
    method_data =  method_data.dropna()

    s = method_data['Method'].str.split(';').apply(pd.Series, 1).stack()
    s.name = "Method"
    del method_data["Method"]
    s = s.to_frame().reset_index()
    method_data = pd.merge(method_data, s, right_on='level_0', left_index = True)

    del method_data["level_0"]
    del method_data["level_1"]
    method_data = list(method_data.T.to_dict().values())

    query = """
           UNWIND $rows AS row
           MERGE (language:Language {uid:row.Name})
           MERGE (method:Method {uid:row.Method})
           CREATE (language)-[r:CAN_FOLLOW]->(method)
           
       """

    run_neo_query(method_data,query)

def process_tool_data(data):
    tool_data = data[['Name','Developer','Year_of_latest_release', 'Simulation', 'Customisation']]
    tool_data =  tool_data.dropna()

    # Convert data frame to list of dictionaries
    # Neo4j UNWIND query expects a list of dictionaries
    # for bulk insertion
    tool_data = list(tool_data.T.to_dict().values())
    print(tool_data)

    query = """
            UNWIND $rows AS row

            MERGE (tool:Tool {uid:row.Name})
            SET 
                tool.Developer = row.Developer,
                tool.Year_of_latest_release = row.Year_of_latest_release,
                tool.Simulation_Links = row.Simulation_Links,
                tool.Customisation = row.Customisation
        """

    run_neo_query(tool_data,query)

    # adding relationships to available languages
    language_data = data[['Name','Language']]
    language_data =  language_data.dropna()

    s = language_data['Language'].str.split(';').apply(pd.Series, 1).stack()
    s.name = "Language"
    del language_data["Language"]
    s = s.to_frame().reset_index()
    language_data = pd.merge(language_data, s, right_on='level_0', left_index = True)

    del language_data["level_0"]
    del language_data["level_1"]
    language_data = list(language_data.T.to_dict().values())

    query = """
           UNWIND $rows AS row
           MERGE (tool:Tool {uid:row.Name})
           MERGE (language:Language {uid:row.Language})
           CREATE (tool)-[r:CAN_USE]->(language)
           
       """

    run_neo_query(language_data,query)

    # adding relationships to available methods
    method_data = data[['Name','Method']]
    method_data =  method_data.dropna()

    s = method_data['Method'].str.split(';').apply(pd.Series, 1).stack()
    s.name = "Method"
    del method_data["Method"]
    s = s.to_frame().reset_index()
    method_data = pd.merge(method_data, s, right_on='level_0', left_index = True)

    del method_data["level_0"]
    del method_data["level_1"]
    method_data = list(method_data.T.to_dict().values())

    query = """
           UNWIND $rows AS row
           MERGE (tool:Tool {uid:row.Name})
           MERGE (method:Method {uid:row.Method})
           CREATE (tool)-[r:CAN_IMPLEMENT]->(method)
       """

    run_neo_query(method_data,query)

def process_method_data(data):
    method_data = data[['Name','Developer','Year_of_latest_release', 'Design_Space_Exploration']]
    method_data =  method_data.dropna()

    # Convert data frame to list of dictionaries
    # Neo4j UNWIND query expects a list of dictionaries
    # for bulk insertion
    method_data = list(method_data.T.to_dict().values())
    print(method_data)

    query = """
            UNWIND $rows AS row

            MERGE (method:Method {uid:row.Name})
            SET 
                method.Developer = row.Developer,
                method.Year_of_latest_release = row.Year_of_latest_release,
                method.Design_Space_Exploration = row.Design_Space_Exploration
        """

    run_neo_query(method_data,query)

    # adding relationships to available languages
    language_data = data[['Name','Language']]
    language_data =  language_data.dropna()

    s = language_data['Language'].str.split(';').apply(pd.Series, 1).stack()
    s.name = "Language"
    del language_data["Language"]
    s = s.to_frame().reset_index()
    language_data = pd.merge(language_data, s, right_on='level_0', left_index = True)

    del language_data["level_0"]
    del language_data["level_1"]
    language_data = list(language_data.T.to_dict().values())

    query = """
           UNWIND $rows AS row
           MERGE (method:Method {uid:row.Name})
           MERGE (language:Language {uid:row.Language})
           CREATE (method)-[r:CAN_BE_WRITTEN_IN]->(language)
           
       """

    run_neo_query(language_data,query)

    # adding relationships to available tools
    tool_data = data[['Name','Tool']]
    tool_data =  tool_data.dropna()

    s = tool_data['Tool'].str.split(';').apply(pd.Series, 1).stack()
    s.name = "Tool"
    del tool_data["Tool"]
    s = s.to_frame().reset_index()
    tool_data = pd.merge(tool_data, s, right_on='level_0', left_index = True)

    del tool_data["level_0"]
    del tool_data["level_1"]
    tool_data = list(tool_data.T.to_dict().values())

    query = """
           UNWIND $rows AS row
           MERGE (method:Method {uid:row.Name})
           MERGE (tool:Tool {uid:row.Tool})
           CREATE (method)-[r:AVAILABLE_IN]->(tool)
           
       """

    run_neo_query(tool_data,query)

    # adding relationships to generated artifacts
    artifacts_data = data[['Name','Artifacts']]
    artifacts_data =  artifacts_data.dropna()

    s = artifacts_data['Artifacts'].str.split(';').apply(pd.Series, 1).stack()
    s.name = "Artifacts"
    del artifacts_data["Artifacts"]
    s = s.to_frame().reset_index()
    artifacts_data = pd.merge(artifacts_data, s, right_on='level_0', left_index = True)

    del artifacts_data["level_0"]
    del artifacts_data["level_1"]
    artifacts_data = list(artifacts_data.T.to_dict().values())

    query = """
           UNWIND $rows AS row
           MERGE (method:Method {uid:row.Name})
           MERGE (artifact:Artifact {uid:row.Artifacts})
           CREATE (method)-[r:GENERATES]->(artifact)
           
       """

    run_neo_query(artifacts_data,query)

def process_issue_data(data):
    issue_data = data[['Name','Summary','Affected_Aspects', 'Severity', 'Workaround']]
    issue_data =  issue_data.dropna()

    # Convert data frame to list of dictionaries
    # Neo4j UNWIND query expects a list of dictionaries
    # for bulk insertion
    issue_data = list(issue_data.T.to_dict().values())
    print(issue_data)

    query = """
            UNWIND $rows AS row

            MERGE (issue:Issue {uid:row.Name})
            SET 
                issue.Summary = row.Summary,
                issue.Severity = row.Severity,
                issue.Workaround = row.Workaround
        """

    run_neo_query(issue_data,query)

    #adding relationships to affected aspects
    aspect_data = data[['Name','Affected_Aspects']]
    aspect_data = aspect_data.dropna()

    s = aspect_data['Affected_Aspects'].str.split(';').apply(pd.Series, 1).stack()
    s.name = "Affected_Aspects"
    del aspect_data["Affected_Aspects"]
    s = s.to_frame().reset_index()
    aspect_data = pd.merge(aspect_data, s, right_on='level_0', left_index = True)

    del aspect_data["level_0"]
    del aspect_data["level_1"]
    aspect_data = list(aspect_data.T.to_dict().values())

    query = """
           UNWIND $rows AS row
           MERGE (issue:Issue {uid:row.Name})
           MERGE (aspect {uid:row.Affected_Aspects})
           CREATE (issue)-[r:AVAILABLE_IN]->(aspect)
           
       """

    run_neo_query(aspect_data,query)

def process_technique_data(data):
    technique_data = data[['Name','Summary','Variability_Type', 'Adv', 'Disadv']]
    technique_data =  technique_data.dropna()

    # Convert data frame to list of dictionaries
    # Neo4j UNWIND query expects a list of dictionaries
    # for bulk insertion
    technique_data = list(technique_data.T.to_dict().values())
    print(technique_data)

    query = """
            UNWIND $rows AS row

            MERGE (technique:Technique {uid:row.Name})
            SET 
                technique.Summary = row.Summary,
                technique.Variability_Type = row.Variability_Type,
                technique.Adv = row.Adv,
                technique.Disadv = row.Disadv
        """

    run_neo_query(technique_data,query)

    #adding relationships to inputs
    input_data = data[['Name','Inputs']]
    input_data = input_data.dropna()

    s = input_data['Inputs'].str.split(';').apply(pd.Series, 1).stack()
    s.name = "Inputs"
    del input_data["Inputs"]
    s = s.to_frame().reset_index()
    input_data = pd.merge(input_data, s, right_on='level_0', left_index = True)

    del input_data["level_0"]
    del input_data["level_1"]
    input_data = list(input_data.T.to_dict().values())

    query = """
           UNWIND $rows AS row
           MERGE (technique:Technique {uid:row.Name})
           MERGE (artifact:Artifact {uid:row.Inputs})
           CREATE (technique)-[r:TAKES_INPUT]->(artifact)
           
       """

    run_neo_query(input_data,query)

    #adding relationships to outputs
    output_data = data[['Name','Outputs']]
    output_data = output_data.dropna()

    s = output_data['Outputs'].str.split(';').apply(pd.Series, 1).stack()
    s.name = "Outputs"
    del output_data["Outputs"]
    s = s.to_frame().reset_index()
    output_data = pd.merge(output_data, s, right_on='level_0', left_index = True)

    del output_data["level_0"]
    del output_data["level_1"]
    output_data = list(output_data.T.to_dict().values())

    query = """
           UNWIND $rows AS row
           MERGE (technique:Technique {uid:row.Name})
           MERGE (artifact:Artifact {uid:row.Outputs})
           CREATE (technique)-[r:CREATS_OUTPUT]->(artifact)
           
       """

    run_neo_query(output_data,query)

    #adding relationships to solved issues
    solves_data = data[['Name','Solves']]
    solves_data = solves_data.dropna()

    s = solves_data['Solves'].str.split(';').apply(pd.Series, 1).stack()
    s.name = "Solves"
    del solves_data["Solves"]
    s = s.to_frame().reset_index()
    solves_data = pd.merge(solves_data, s, right_on='level_0', left_index = True)

    del solves_data["level_0"]
    del solves_data["level_1"]
    solves_data = list(solves_data.T.to_dict().values())

    query = """
           UNWIND $rows AS row
           MERGE (technique:Technique {uid:row.Name})
           MERGE (issue:Issue {uid:row.Solves})
           CREATE (technique)-[r:SOLVES]->(issue)
           
       """

    run_neo_query(solves_data,query)

def run_neo_query(data, query):
    batches = get_batches(data)

    for index, batch in batches:
        print('[Batch: %s] Will add %s node to Graph' % (index, len(batch)))
        print(batch)
        graph.run(query, rows=batch)


def get_batches(lst, batch_size=100):
    return [(i, lst[i:i + batch_size]) for i in range(0, len(lst), batch_size)]

def main():
    if __name__== "__main__":
        language_data = read_data('Languages')
        process_language_data(language_data)
        tool_data = read_data('Tools')
        process_tool_data(tool_data)
        method_data = read_data('Methods')
        process_method_data(method_data)
        issue_data = read_data('Issues')
        process_issue_data(issue_data)
        technique_data = read_data('Techniques')
        process_technique_data(technique_data)

if __name__ == "__main__":
    main()