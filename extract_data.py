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

def process_relationships(data,sourceType,targetKey,targetType,relationshipName,direction):
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
        """
    else:
        query = """
            UNWIND $rows AS row
            MERGE (source:"""+ sourceType + """ {uid:row.Name})
            MERGE (target:"""+ targetType + """ {uid:row."""+targetKey+"""})
            CREATE (target)-[r:"""+ relationshipName+"""]->(source)
                    SET r.Number_of_related_issues = 0         
        """
    run_neo_query(relation_data,query)
    
def generate_node_match_query(namelist):
    query = ""
    for name in namelist:
        query = query + "MATCH("+name.replace(" ","_")+"{uid:'"+name+"'}) "

    query = query + "RETURN "
    
    for name in namelist:
        query = query + name.replace(" ","_")+","

    print('shortest path query:\n')
    print(query[:-1])
    print('\n')

    return query[:-1]

def process_language_data(data):
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

    run_neo_query(language_data,query)

    # adding relationships to available tools
    process_relationships(data,'Language','Tool','Tool','AVAILABLE_IN','OUTGOING')
    
    # adding relationships to available methods
    process_relationships(data,'Language','Method','Method','CAN_FOLLOW','OUTGOING')

def process_tool_data(data):
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

    run_neo_query(tool_data,query)

    # adding relationships to available languages
    process_relationships(data,'Tool','Language','Language','CAN_USE','OUTGOING')

    # adding relationships to available methods
    process_relationships(data,'Tool','Method','Method','CAN_IMPLEMENT','OUTGOING')

    #adding relationships to available simulation tools
    process_relationships(data,'Tool','Simulation_Tool','Simulation_Tool','CAN_EXECUTE_MODEL_IN','OUTGOING')

    
def process_method_data(data):
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

    run_neo_query(method_data,query)

    # adding relationships to available languages
    process_relationships(data,'Method','Language','Language','CAN_BE_WRITTEN_IN','OUTGOING')

    # adding relationships to available tools
    process_relationships(data,'Method','Tool','Tool','AVAILABLE_IN','OUTGOING')

    # adding relationships to generated artifacts
    process_relationships(data,'Method','Artifacts','Artifact','GENERATES','OUTGOING')

def process_issue_data(data):
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
           CREATE (issue)-[r:AFFECTS]->(aspect)
           
       """

    run_neo_query(aspect_data,query)

def process_technique_data(data):
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

    run_neo_query(technique_data,query)

    #adding relationships to inputs
    process_relationships(data,'Technique','Inputs','Artifact','TAKES_AS_INPUT','INCOMING')
    
    #adding relationships to outputs
    process_relationships(data,'Technique','Outputs','Artifact','GENERATES','OUTGOING')

    #adding relationships to solved issues
    process_relationships(data,"Technique","Solves","Issue","SOLVES",'OUTGOING')

def process_simtool_data(data,tool_data):
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

    run_neo_query(simtool_data,query)

    # adding available outputs
    process_relationships(data,"Simulation_Tool","Outputs","Artifact","EXECUTES",'OUTGOING')

    # adding links to related methods, for later shortest path estimation need to force the path through a method
    query = """
            MATCH (tool:Tool)-[r]->(simtool:Simulation_Tool)
            MATCH (tool:Tool)-[r2]->(method:Method)
            CREATE (method)-[r32:METHOD_RELATED_SIMTOOL]->(simtool)
        """
    run_neo_query(simtool_data,query)
    

def update_issue_cost(languagedata,tooldata,methoddata):
     # counting issues related to languages
    language_data = languagedata[['Name']]
    language_data =  language_data.dropna()

    language_data = list(language_data.T.to_dict().values())
    for language in language_data:
        query = """
                MATCH 
                    (n:Language)<-[r:AFFECTS]-() WHERE n.uid = '"""+language['Name']+"""'
                WITH n,COUNT(r) as numberofissues
                SET n.Number_of_related_issues = numberofissues
                RETURN numberofissues
            """

        numberofissues = graph.run(query).evaluate()
        Noissues = numberofissues
        if Noissues is None:
            Noissues = 0

        query = """
                MATCH 
                    (n:Language)<-[r2]-() WHERE n.uid = '"""+language['Name']+"""'
                WITH n,r2
                SET r2.Number_of_related_issues = """+str(Noissues)+"""
            """

        graph.run(query)
          
    # counting issues related to tools
    tool_data = tooldata[['Name']]
    tool_data =  tool_data.dropna()

    tool_data = list(tool_data.T.to_dict().values())
    for tool in tool_data:
        query = """
                MATCH 
                    (n:Tool)<-[r:AFFECTS]-() WHERE n.uid = '"""+tool['Name']+"""'
                WITH n,COUNT(r) as numberofissues
                SET n.Number_of_related_issues = numberofissues
                RETURN numberofissues
            """

        numberofissues = graph.run(query).evaluate()
        Noissues = numberofissues
        if Noissues is None:
            Noissues = 0
            
        query = """
                MATCH 
                    (n:Tool)<-[r2]-() WHERE n.uid = '"""+tool['Name']+"""'
                WITH n,r2
                SET r2.Number_of_related_issues = """+str(Noissues)+"""
            """

        graph.run(query)

    # counting issues related to methods
    method_data = methoddata[['Name']]
    method_data =  method_data.dropna()

    method_data = list(method_data.T.to_dict().values())
    for method in method_data:
        query = """
                MATCH 
                    (n:Method)<-[r:AFFECTS]-() WHERE n.uid = '"""+method['Name']+"""'
                WITH n,COUNT(r) as numberofissues
                SET n.Number_of_related_issues = numberofissues
                RETURN numberofissues
            """

        numberofissues = graph.run(query).evaluate()
        Noissues = numberofissues
        if Noissues is None:
            Noissues = 0
            
        query = """
                MATCH 
                    (n:Method)<-[r2]-() WHERE n.uid = '"""+method['Name']+"""'
                WITH n,r2
                SET r2.Number_of_related_issues = """+str(Noissues)+"""
            """

        graph.run(query)


def identify_exploration_solution(technique_data,method_data):
    # Query creates graph projection (like a sub set of the main graph with relevant data), then runs the A* shortest path algorithm and collects results

    #CALL gds.graph.drop('solutionGraph') 
    #YIELD graphName
    query = """
            CALL gds.graph.drop('solutionGraph') 
            YIELD graphName
            CALL gds.graph.project(
                'solutionGraph',
                ['Method', 'Tool','Simulation_Tool','Language','Artifact','Technique'],
                ['AVAILABLE_IN','CAN_IMPLEMENT','METHOD_RELATED_SIMTOOL','EXECUTES','GENERATES','TAKES_AS_INPUT'],
                {relationshipProperties: 'Number_of_related_issues'}
            )
            YIELD
                graphName AS graph, nodeProjection, nodeCount AS nodes, relationshipProjection, relationshipCount AS rels
            MATCH (source{uid:"SysML V1"}), (target{uid:"Globally Optimal Design Parameters"})
            CALL gds.shortestPath.dijkstra.stream('solutionGraph', {
                sourceNode: source,
                targetNode: target,
                relationshipWeightProperty: 'Number_of_related_issues'
            })
            YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
            RETURN
                index,
                gds.util.asNode(sourceNode).name AS sourceNodeName,
                gds.util.asNode(targetNode).name AS targetNodeName,
                totalCost,
                [nodeId IN nodeIds | gds.util.asNode(nodeId).uid] AS nodeNames,
                costs,
                nodes(path) as path
            ORDER BY index
        """

    initial_path = graph.run(query).to_data_frame()
    # this result will not necessarily privde a total solution as some design technique inputs (artifacts) may be missing and need to be considered

    # identifying chosen method, for use later in checking simulation tool selection
    for node in initial_path.nodeNames[0]:
        if node in method_data['Name'].values:
            chosen_method = node

    # firstly identify selected techniques
    technique_list = []
    for node in initial_path.nodeNames[0]:
        if node in technique_data['Name'].values:
            technique_list.append(node)

    # now find required artifacts for those techniques
    for technique in technique_list:
        query = """
            MATCH(technique:Technique{uid:'"""+technique+"""'})<-[r:TAKES_AS_INPUT]-(preReq)
            RETURN preReq
        """
        technique_prereqs = graph.run(query).to_data_frame()
        # now add any artifacts not included in the initial path
        for preReq in technique_prereqs.preReq:
            if preReq['uid'] not in initial_path.nodeNames[0]:
                initial_path.nodeNames[0].append(preReq['uid'])

                # need to check for required techniques to generate these artifacts and add them to the solution path
                query = """
                    MATCH(artifact:Artifact{uid:'"""+preReq['uid']+"""'})<-[r:GENERATES|EXECUTES]-(preReq:Technique|Simulation_Tool)
                    RETURN preReq
                """
                artifact_prereqs = graph.run(query).to_data_frame()

                # as Executable System Model is a special case of artifact that is generated specifally by simulation tools,
                # need to hanld this special case and only select the best simulation tool
                if preReq['uid'] == 'Executable System Model':
                    query = """
                        CALL gds.graph.drop('simtoolGraph') 
                        YIELD graphName
                        CALL gds.graph.project(
                            'simtoolGraph',
                            ['Method', 'Simulation_Tool','Artifact'],
                            ['METHOD_RELATED_SIMTOOL','EXECUTES'],
                            {relationshipProperties: 'Number_of_related_issues'}
                        )
                        YIELD
                            graphName AS graph, nodeProjection, nodeCount AS nodes, relationshipProjection, relationshipCount AS rels
                        MATCH (source{uid:'"""+chosen_method+"""'}), (target{uid:'Executable System Model'})
                        CALL gds.shortestPath.dijkstra.stream('simtoolGraph', {
                            sourceNode: source,
                            targetNode: target,
                            relationshipWeightProperty: 'Number_of_related_issues'
                        })
                        YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
                        RETURN
                            index,
                            gds.util.asNode(sourceNode).name AS sourceNodeName,
                            gds.util.asNode(targetNode).name AS targetNodeName,
                            totalCost,
                            [nodeId IN nodeIds | gds.util.asNode(nodeId).uid] AS nodeNames,
                            costs,
                            nodes(path) as path
                        ORDER BY index
                    """

                    usable_simtool = graph.run(query).to_data_frame().nodeNames[0][1]
                    artifact_prereqs = artifact_prereqs.drop(artifact_prereqs.index.values)
                    artifact_prereqs = artifact_prereqs.append({'preReq':{'uid':usable_simtool}},ignore_index=True)
                    print(usable_simtool)
                    print(artifact_prereqs)


                # now add any techniques/simulation tools not included in the initial path
                if not artifact_prereqs.empty:
                    for artifact_preReq in artifact_prereqs.preReq:
                        if artifact_preReq['uid'] not in initial_path.nodeNames[0]:
                            initial_path.nodeNames[0].append(artifact_preReq['uid'])

    return initial_path
    
def run_neo_query(data, query):
    batches = get_batches(data)

    for index, batch in batches:
        print('[Batch: %s] Will add %s node(s) to Graph' % (index, len(batch)))
        graph.run(query, rows=batch)


def get_batches(lst, batch_size=100):
    return [(i, lst[i:i + batch_size]) for i in range(0, len(lst), batch_size)]

def clear_database():
    query = """
        MATCH(n) DETACH DELETE n
    """
    graph.run(query)

def main():
    if __name__== "__main__":
        clear_database()
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
        simtool_data = read_data('SimTools')
        process_simtool_data(simtool_data,tool_data)
        update_issue_cost(language_data,tool_data,method_data)
        candidate_path = identify_exploration_solution(technique_data,method_data)
        print(f"candidate solution issue count: {candidate_path.totalCost[0]}")
        generate_node_match_query(candidate_path.nodeNames[0])
        generate_node_match_query(['SysML V1','Cameo','SEAM','Cameo Simulation Toolkit','Surrogate Modelling','Genetic Optimisation','Globally Optimal Design Parameters'])



if __name__ == "__main__":
    main()