import pandas as pd

def identify_exploration_solution(startnode,endnode,scenario_data,technique_data,techniques_list,suggest_techniques,tool_data,simtool_data,graph):
    """
    Identify a solution path in the exploration graph between a start node and an end node based on scenario and context data.

    This function identifies a solution path in an exploration graph from a specified start node to an end node, taking into account scenario and context data.
    It allows the option to suggest additional techniques for the solution.

    Args:
        startnode (str): The UID of the start node in the graph.
        endnode (str): The UID of the end node in the graph.
        scenario_data (dict): A dictionary containing scenario-specific data.
        technique_data (pandas.DataFrame): A DataFrame containing data about available techniques.
        techniques_list (list): A list of technique names.
        suggest_techniques (bool): If True, the function can suggest additional techniques for the solution.
        tool_data (pandas.DataFrame): A DataFrame containing data about available tools.
        simtool_data (pandas.DataFrame): A DataFrame containing data about simulation tools.
        graph: The Neo4j graph database object.

    Returns:
        pandas.DataFrame: A DataFrame representing the solution path with relevant nodes and their properties.
    """
    # convert context data dict to list
    scenario_data_list = []
    for key in scenario_data:
        scenario_data_list.append(scenario_data[key])
    # try to remove old graph (will raise error if does not exist)
    try:
        query = """
                CALL gds.graph.drop('solutionGraph') 
                YIELD graphName
                """
        graph.run(query)
    except:
        print("graph already exists")

    # first identify if allowed to suggest extra techniques or use only selected
    if suggest_techniques:
        technique_label = 'Technique'
    else:
        technique_label = 'Selected_Technique'

    # Query creates graph projection (like a sub set of the main graph with relevant data), then runs dijkstra's algorithm and collects results
    query = """
            CALL gds.graph.project(
                'solutionGraph',
                ['Method', 'Tool','Simulation_Tool','Language','Artifact','"""+technique_label+"""'],
                ['AVAILABLE_IN','CAN_IMPLEMENT','METHOD_RELATED_SIMTOOL','EXECUTES','GENERATES','FORMS_INPUT_FOR'],
                {relationshipProperties: 'Issue_Cost'}
            )
            YIELD
                graphName AS graph, nodeProjection, nodeCount AS nodes, relationshipProjection, relationshipCount AS rels
            MATCH (source{uid:'"""+startnode+"""'}), (target{uid:'"""+endnode+"""'})
            CALL gds.shortestPath.dijkstra.stream('solutionGraph', {
                sourceNode: source,
                targetNode: target,
                relationshipWeightProperty: 'Issue_Cost'
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
    # add scenario context nodes (except last one)
    if not initial_path.empty:
        for node in scenario_data_list:
            if node is not startnode:
                initial_path.nodeNames[0].append(node) 
    # this result will not necessarily provide a total solution as some design technique inputs (artifacts) may be missing and need to be considered
    original_initial_path = initial_path.copy(deep =True)
    solution_path,techniques_list = identify_solution_path_prereqs(initial_path,suggest_techniques,technique_data,techniques_list,tool_data,simtool_data,graph)

    # update issues costs based on potentially updated set of techniques
    for technique in techniques_list:
        if technique not in solution_path['nodeNames'][0]:
            query="""
            MATCH(technique:Technique{uid:'"""+technique+"""'})-[r:SOLVES]->(issue)
            WITH issue, technique
            SET
                issue.Evaluated_Severity = issue.Severity
                REMOVE technique:Selected_Technique
            """
            graph.run(query)
        else:
            select_techniques([technique],graph)

    # now re run dijkstra's to see if still shortest path
    # Query creates graph projection (like a sub set of the main graph with relevant data), then runs dijkstra's algorithm and collects results
    updated_initial_path = identify_inital_path(startnode,endnode,scenario_data_list,technique_label,'comparison_check',graph)
    updated_solution_path,updated_techniques_list = identify_solution_path_prereqs(updated_initial_path,suggest_techniques,technique_data,techniques_list,tool_data,simtool_data,graph)

    # now check if any new steps exist in updated path -> if they do, re-run process again with updated context, until converged on solution
    print(original_initial_path['nodeNames'][0])
    for step in updated_solution_path['nodeNames'][0]:
        print(f'step: {step}')
        if step not in original_initial_path['nodeNames'][0]:
            solution_path = identify_exploration_solution(startnode,endnode,scenario_data,technique_data,updated_techniques_list,suggest_techniques,tool_data,simtool_data,graph)

    return solution_path

def identify_inital_path(startnode,endnode,scenario_data_list,technique_label,subgraph_name,graph):
    # Query creates graph projection (like a sub set of the main graph with relevant data), then runs dijkstra's algorithm and collects results
    # try to remove old graph (will raise error if does not exist)
    try:
        query = """
                CALL gds.graph.drop('"""+subgraph_name+"""') 
                YIELD graphName
                """
        graph.run(query)
    except:
        print("graph already exists")
    query = """
            CALL gds.graph.project(
                '"""+subgraph_name+"""',
                ['Method', 'Tool','Simulation_Tool','Language','Artifact','"""+technique_label+"""'],
                ['AVAILABLE_IN','CAN_IMPLEMENT','METHOD_RELATED_SIMTOOL','EXECUTES','GENERATES','FORMS_INPUT_FOR'],
                {relationshipProperties: 'Issue_Cost'}
            )
            YIELD
                graphName AS graph, nodeProjection, nodeCount AS nodes, relationshipProjection, relationshipCount AS rels
            MATCH (source{uid:'"""+startnode+"""'}), (target{uid:'"""+endnode+"""'})
            CALL gds.shortestPath.dijkstra.stream('"""+subgraph_name+"""', {
                sourceNode: source,
                targetNode: target,
                relationshipWeightProperty: 'Issue_Cost'
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
    # add scenario context nodes (except last one)
    for node in scenario_data_list:
        if node is not startnode:
            initial_path.nodeNames[0].append(node) 
    return initial_path
    
def identify_solution_path_prereqs(initial_path,suggest_techniques,technique_data,techniques_list,tool_data,simtool_data,graph):
    """
    Identify a solution path by considering technique and artifact prerequisites.

    This function extends an initial solution path by identifying and adding the prerequisite nodes (techniques and artifacts)
    required to complete the solution path. It considers technique prerequisites and their respective artifacts.

    Args:
        initial_path (pandas.DataFrame): A DataFrame representing the initial solution path with relevant nodes.
        suggest_techniques (bool): If True, additional techniques can be suggested for the solution.
        technique_data (pandas.DataFrame): A DataFrame containing data about available techniques.
        techniques_list (list): A list of technique names.
        tool_data (pandas.DataFrame): A DataFrame containing data about available tools.
        simtool_data (pandas.DataFrame): A DataFrame containing data about simulation tools.
        graph: The Neo4j graph database object.

    Returns:
        pandas.DataFrame: A DataFrame representing the extended solution path with prerequisite nodes and their properties.
    """
    # firslty checking if input node list is a data frame or simple list of node names
    # if simple list, adding it to a dataframe with node names collum for ease of use
    if isinstance(initial_path,list):
        initial_path_dict = {'nodeNames':[initial_path]}
        initial_path = pd.DataFrame(initial_path_dict)

    # identifying chosen tool, for use later in checking simulation tool selection
    for node in initial_path.nodeNames[0]:
        if node in tool_data['Name'].values:
            chosen_tool = node

    # count number of simulation tools included, for use later in checking simulation tool selection
    simtool_chosen = False
    for node in initial_path.nodeNames[0]:
        if node in simtool_data['Name'].values:
            simtool_chosen = True  

    # firstly identify techniques in solution path
    technique_list = []
    for node in initial_path.nodeNames[0]:
        if node in technique_data['Name'].values:
            technique_list.append(node)
    
    # need to identify if allowed to suggest extra techniques or use only selected
    if suggest_techniques:
        technique_label = 'Technique'
    else:
        technique_label = 'Selected_Technique'
    
    # loop until all artifact and technique prerequsites are met
    loops = 0
    solution_incompelte = True
    while solution_incompelte and loops<100:
        added_new_node = False
        # now find required artifacts for those techniques
        for technique in technique_list:

            # need to allow for alternative technique not nesseacry to generate an artifact and not selected by the user
            if (technique not in techniques_list) and (not suggest_techniques):
                pass
            else:
                query = """
                    MATCH(technique:"""+technique_label+"""{uid:'"""+technique+"""'})<-[r:FORMS_INPUT_FOR]-(preReq)
                    RETURN preReq
                """
                technique_prereqs = graph.run(query).to_data_frame()
                # now add any artifacts not included in the initial path
                for preReq in technique_prereqs.preReq:
                    if preReq['uid'] not in initial_path.nodeNames[0]:
                        initial_path.nodeNames[0].append(preReq['uid'])
                        added_new_node = True

                        # firstly check if artifact is generated by method already in context
                        use_method = False
                        query = """
                            MATCH(artifact:Artifact{uid:'"""+preReq['uid']+"""'})<-[r:GENERATES]-(preReq:Method)
                            RETURN preReq
                        """
                        
                        artifact_prereq_methods = graph.run(query).to_data_frame()
                        if not artifact_prereq_methods.empty:
                            for artifact_preReq_method in artifact_prereq_methods.preReq:
                                if artifact_preReq_method['uid']  in initial_path.nodeNames[0]:
                                    use_method = True

                        # if cannot be generated by a method in the context, need to check for required techniques to generate these artifacts and add them to the solution path
                        if not use_method:
                            query = """
                                MATCH(artifact:Artifact{uid:'"""+preReq['uid']+"""'})<-[r:GENERATES|EXECUTES]-(preReq:Technique|Simulation_Tool)
                                RETURN preReq
                            """
                            
                            artifact_prereqs = graph.run(query).to_data_frame()
                            # artifacts generated by methods can be


                            # as Executable System Model is a special case of artifact that is generated specifally by simulation tools,
                            # need to handle this special case and only select the best simulation tool (if one has not already been selected)

                            if preReq['uid'] == 'Executable System Model':
                                if not simtool_chosen: # don't add simtool if already chosen
                                    try:
                                        query = """
                                            CALL gds.graph.drop('simtoolGraph') 
                                            YIELD graphName
                                        """
                                        graph.run(query)
                                    except:
                                        print("graph already exists")
                                    
                                    # find sim tool with shortest path through
                                    query = """
                                        CALL gds.graph.project(
                                            'simtoolGraph',
                                            ['Tool', 'Simulation_Tool','Artifact'],
                                            ['CAN_EXECUTE_MODEL_IN','EXECUTES'],
                                            {relationshipProperties: 'Issue_Cost'}
                                        )
                                        YIELD
                                            graphName AS graph, nodeProjection, nodeCount AS nodes, relationshipProjection, relationshipCount AS rels
                                        MATCH (source{uid:'"""+chosen_tool+"""'}), (target{uid:'Executable System Model'})
                                        CALL gds.shortestPath.dijkstra.stream('simtoolGraph', {
                                            sourceNode: source,
                                            targetNode: target,
                                            relationshipWeightProperty: 'Issue_Cost'
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

                            # now add any techniques not included in the initial path (but not simtools)
                            if preReq['uid'] != 'Executable System Model':
                                
                                if not artifact_prereqs.empty:
                                    for artifact_preReq in artifact_prereqs.preReq:
                                        if artifact_preReq['uid'] not in initial_path.nodeNames[0]:
                                            initial_path.nodeNames[0].append(artifact_preReq['uid'])
                                            technique_list.append(artifact_preReq['uid'])

                                            added_new_node = True

        # update selected technique list and artifact list
        technique_list = []
        for node in initial_path.nodeNames[0]:
            if node in technique_data['Name'].values:
                technique_list.append(node)

        # finally if no new nodes have been added, break the main search loop
        if added_new_node is False:
            solution_incompelte = False

        loops+=1
    return  initial_path,technique_list

def select_techniques(techniques_list,graph):
    """
    Select a list of techniques and mark them as 'Selected_Technique' in the graph.

    This function takes a list of technique names and marks each technique as 'Selected_Technique' in the Neo4j graph
    database. It also updates the 'Evaluated_Severity' property of any issues solved by the selected techniques to 0.

    Args:
        techniques_list (list): A list of technique names to be selected.
        graph: The Neo4j graph database object.

    Returns:
        None
    """
    for technique in techniques_list:
        query="""
            MATCH(technique:Technique{uid:'"""+technique+"""'})-[r:SOLVES]->(issue)
            WITH issue, technique
            SET
                issue.Evaluated_Severity = 0
                SET technique:Selected_Technique
        """
        graph.run(query)

def set_solution_start(Scenario_context,graph):
    """
    Determine the starting point for identifying a solution based on the provided Scenario_context.

    This function takes a dictionary representing the Scenario_context, which includes information about
    the current state of the system modeling process, and determines the starting point for identifying a solution
    within that context. The starting point is determined based on the completion status of various steps in the
    conceptual MBSE process model.

    Args:
        Scenario_context (dict): A dictionary representing the Scenario_context, e.g., {'language': 'Python', 'tool': 'SimTool', ...}.
        graph: The Neo4j graph database object.

    Returns:
        str: The name of the starting point based on the provided Scenario_context.
    """
    
    # dict defining order of steps in conceptual MBSE process model
    steps = {'language':1,
            'tool':2,
            'method':3,
            'simulation_tool':4,
            'artifacts':5,
            'technique':6}

    # now find latest step included in Scenario_context
    final_step = 0
    for key in Scenario_context:
        if steps[key]>final_step:
            final_step = steps[key]
            final_key = Scenario_context[key]

    return final_key

def identify_technique_outputs(technique,solution_path,graph):
    """
    Identify and add the outputs generated by a specified technique to the solution path.

    This function queries the graph database to identify the outputs generated by a given technique.
    It then adds these output artifacts to the existing solution path.

    Args:
        technique (str): The name of the technique for which outputs are to be identified.
        solution_path (pd.DataFrame): A DataFrame representing the current solution path containing node names.
        graph: The Neo4j graph database object.

    Returns:
        pd.DataFrame: The updated solution path DataFrame with technique outputs included.
    """
    query = """
        MATCH(technique:Technique{uid:'"""+technique+"""'})-[r:GENERATES]->(output)
        RETURN output
    """
    technique_outputs = graph.run(query).to_data_frame()

    for output in technique_outputs.output:
        solution_path.nodeNames[0].append(output['uid'])
    return solution_path