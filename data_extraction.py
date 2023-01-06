import pandas as pd
import database_tools

def read_data(name):
    data = pd.read_csv(

        f"./data/{name}_info.csv",
        low_memory=False)
    print("Column name of data : ", data.columns)
    return data

def process_language_data(data,graph):
    language_data = data[['Name','Developer','Year_of_latest_release', 'Variability_Modelling', 'Simulation_Links', 'Customisation']]
    language_data =  language_data.dropna()

    # Convert data frame to list of dictionaries
    # Neo4j UNWIND query expects a list of dictionaries
    # for bulk insertion
    language_data = list(language_data.T.to_dict().values())

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

    database_tools.run_neo_query(language_data,query,graph)

    # adding relationships to available tools
    database_tools.process_relationships(data,'Language','Tool','Tool','AVAILABLE_IN','OUTGOING',graph)
    
    # adding relationships to available methods
    database_tools.process_relationships(data,'Language','Method','Method','CAN_FOLLOW','OUTGOING',graph)

def process_tool_data(data,graph):
    tool_data = data[['Name','Developer','Year_of_latest_release', 'Simulation', 'Customisation']]
    tool_data =  tool_data.dropna()

    # Convert data frame to list of dictionaries
    # Neo4j UNWIND query expects a list of dictionaries
    # for bulk insertion
    tool_data = list(tool_data.T.to_dict().values())

    query = """
            UNWIND $rows AS row

            MERGE (tool:Tool {uid:row.Name})
            SET 
                tool.Developer = row.Developer,
                tool.Year_of_latest_release = row.Year_of_latest_release,
                tool.Simulation_Links = row.Simulation_Links,
                tool.Customisation = row.Customisation
        """

    database_tools.run_neo_query(tool_data,query,graph)

    # adding relationships to available languages
    database_tools.process_relationships(data,'Tool','Language','Language','CAN_USE','OUTGOING',graph)

    # adding relationships to available methods
    database_tools.process_relationships(data,'Tool','Method','Method','CAN_IMPLEMENT','OUTGOING',graph)

    #adding relationships to available simulation tools
    database_tools.process_relationships(data,'Tool','Simulation_Tool','Simulation_Tool','CAN_EXECUTE_MODEL_IN','OUTGOING',graph)

    
def process_method_data(data,graph):
    method_data = data[['Name','Developer','Year_of_latest_release', 'Design_Space_Exploration']]
    method_data =  method_data.dropna()

    # Convert data frame to list of dictionaries
    # Neo4j UNWIND query expects a list of dictionaries
    # for bulk insertion
    method_data = list(method_data.T.to_dict().values())

    query = """
            UNWIND $rows AS row

            MERGE (method:Method {uid:row.Name})
            SET 
                method.Developer = row.Developer,
                method.Year_of_latest_release = row.Year_of_latest_release,
                method.Design_Space_Exploration = row.Design_Space_Exploration
        """

    database_tools.run_neo_query(method_data,query,graph)

    # adding relationships to available languages
    database_tools.process_relationships(data,'Method','Language','Language','CAN_BE_WRITTEN_IN','OUTGOING',graph)

    # adding relationships to available tools
    database_tools.process_relationships(data,'Method','Tool','Tool','APPLICABLE_TO','OUTGOING',graph)

    # adding relationships to generated artifacts
    database_tools.process_relationships(data,'Method','Artifacts','Artifact','GENERATES','OUTGOING',graph)

def process_issue_data(data,graph):
    issue_data = data[['Name','Summary','Affected_Aspects', 'Severity', 'Workaround']]
    issue_data =  issue_data.dropna()

    # Convert data frame to list of dictionaries
    # Neo4j UNWIND query expects a list of dictionaries
    # for bulk insertion
    issue_data = list(issue_data.T.to_dict().values())

    query = """
            UNWIND $rows AS row

            MERGE (issue:Issue {uid:row.Name})
            SET 
                issue.Summary = row.Summary,
                issue.Severity = row.Severity,
                issue.Evaluated_Severity = row.Severity,
                issue.Workaround = row.Workaround
        """

    database_tools.run_neo_query(issue_data,query,graph)

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
           CREATE (issue)-[r:AFFECTS]->(aspect)
           
       """

    database_tools.run_neo_query(aspect_data,query,graph)

def process_technique_data(data,graph):
    technique_data = data[['Name','Summary','Variability_Type', 'Adv', 'Disadv']]
    technique_data =  technique_data.dropna()

    # Convert data frame to list of dictionaries
    # Neo4j UNWIND query expects a list of dictionaries
    # for bulk insertion
    technique_data = list(technique_data.T.to_dict().values())

    query = """
            UNWIND $rows AS row

            MERGE (technique:Technique {uid:row.Name})
            SET 
                technique.Summary = row.Summary,
                technique.Variability_Type = row.Variability_Type,
                technique.Adv = row.Adv,
                technique.Disadv = row.Disadv
        """

    database_tools.run_neo_query(technique_data,query,graph)

    #adding relationships to inputs
    database_tools.process_relationships(data,'Technique','Inputs','Artifact','TAKES_AS_INPUT','INCOMING',graph)
    
    #adding relationships to outputs
    database_tools.process_relationships(data,'Technique','Outputs','Artifact','GENERATES','OUTGOING',graph)

    #adding relationships to solved issues
    database_tools.process_relationships(data,"Technique","Solves","Issue","SOLVES",'OUTGOING',graph)

def process_simtool_data(data,simtool_data,graph):
    simtool_data = data[['Name','Developer','Year_of_latest_release','Language', 'Customisation']]
    simtool_data =  simtool_data.dropna()

    # Convert data frame to list of dictionaries
    # Neo4j UNWIND query expects a list of dictionaries
    # for bulk insertion
    simtool_data = list(simtool_data.T.to_dict().values())
    print(simtool_data)

    query = """
            UNWIND $rows AS row

            MERGE (simtool:Simulation_Tool {uid:row.Name})
            SET 
                simtool.Developer = row.Developer,
                simtool.Language = row.Language,
                simtool.Customisation = row.Customisation
        """

    database_tools.run_neo_query(simtool_data,query,graph)

    # adding available outputs
    database_tools.process_relationships(data,"Simulation_Tool","Outputs","Artifact","EXECUTES",'OUTGOING',graph)

    # adding links to related methods, for later shortest path estimation (need to force the path through a method)
    query = """
            MATCH (tool:Tool)-[r]->(simtool:Simulation_Tool)
            MATCH (tool:Tool)-[r2]->(method:Method)
            CREATE (method)-[r32:METHOD_RELATED_SIMTOOL]->(simtool)
                SET r.Number_of_related_issues = 0    
                SET r.Issue_Cost = 0 
        """
    #database_tools.run_neo_query(simtool_data,query) NOT NESSECARY ANY MORE
    
def apply_issue_cost(languagedata,tooldata,methoddata,graph):
     # counting issues related to languages
    language_data = languagedata[['Name']]
    language_data =  language_data.dropna()

    language_data = list(language_data.T.to_dict().values())
    for language in language_data:
        query = """
                MATCH 
                    (n:Language)<-[r:AFFECTS]-(issue) WHERE n.uid = '"""+language['Name']+"""'
                WITH n,COUNT(r) as numberofissues, sum(issue.Evaluated_Severity) as Evaluated_Severity
                SET n.Number_of_related_issues = numberofissues
                SET n.Issue_Cost = Evaluated_Severity
                RETURN Evaluated_Severity
            """

        issue_Cost = graph.run(query).evaluate()
        evaluated_severity = issue_Cost
        if evaluated_severity is None:
            evaluated_severity = 0

        query = """
                MATCH 
                    (n:Language)<-[r2]-() WHERE n.uid = '"""+language['Name']+"""'
                WITH n,r2
                SET r2.Issue_Cost = """+str(evaluated_severity)+"""
            """

        graph.run(query)
          
    # counting issues related to tools
    tool_data = tooldata[['Name']]
    tool_data =  tool_data.dropna()

    tool_data = list(tool_data.T.to_dict().values())
    for tool in tool_data:
        query = """
                MATCH 
                    (n:Tool)<-[r:AFFECTS]-(issue) WHERE n.uid = '"""+tool['Name']+"""'
                WITH n,COUNT(r) as numberofissues, sum(issue.Evaluated_Severity) as Evaluated_Severity
                SET n.Number_of_related_issues = numberofissues
                SET n.Issue_Cost = Evaluated_Severity
                RETURN Evaluated_Severity
            """

        issue_Cost = graph.run(query).evaluate()
        evaluated_severity = issue_Cost
        if evaluated_severity is None:
            evaluated_severity = 0
            
        query = """
                MATCH 
                    (n:Tool)<-[r2]-() WHERE n.uid = '"""+tool['Name']+"""'
                WITH n,r2
                SET r2.Issue_Cost = """+str(evaluated_severity)+"""
            """

        graph.run(query)

    # counting issues related to methods
    method_data = methoddata[['Name']]
    method_data =  method_data.dropna()

    method_data = list(method_data.T.to_dict().values())
    for method in method_data:
        query = """
                MATCH 
                    (n:Method)<-[r:AFFECTS]-(issue) WHERE n.uid = '"""+method['Name']+"""'
                WITH n,COUNT(r) as numberofissues, sum(issue.Evaluated_Severity) as Evaluated_Severity
                SET n.Number_of_related_issues = numberofissues
                SET n.Issue_Cost = Evaluated_Severity
                RETURN Evaluated_Severity
            """

        issue_Cost = graph.run(query).evaluate()
        evaluated_severity = issue_Cost
        if evaluated_severity is None:
            evaluated_severity = 0
            
        query = """
                MATCH 
                    (n:Method)<-[r2]-() WHERE n.uid = '"""+method['Name']+"""'
                WITH n,r2
                SET r2.Issue_Cost = """+str(evaluated_severity)+"""
            """

        graph.run(query)

