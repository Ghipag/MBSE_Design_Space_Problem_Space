import pandas as pd

def identify_exploration_solution(startnode,endnode,scenario_data,technique_data,suggest_techniques,tool_data,simtool_data,graph):
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

    # Query creates graph projection (like a sub set of the main graph with relevant data), then runs the A* shortest path algorithm and collects results
    query = """
            CALL gds.graph.project(
                'solutionGraph',
                ['Method', 'Tool','Simulation_Tool','Language','Artifact','"""+technique_label+"""'],
                ['AVAILABLE_IN','CAN_IMPLEMENT','METHOD_RELATED_SIMTOOL','EXECUTES','GENERATES','TAKES_AS_INPUT'],
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
    print(initial_path)
    # add scenario context nodes (except last one)
    for node in scenario_data_list:
        if node is not startnode:
            initial_path.nodeNames[0].append(node) 
    # this result will not necessarily privde a total solution as some design technique inputs (artifacts) may be missing and need to be considered
    solution_path = identify_solution_path_prereqs(initial_path,suggest_techniques,technique_data,tool_data,simtool_data,graph)
    
    return solution_path
    
def identify_solution_path_prereqs(initial_path,suggest_techniques,technique_data,tool_data,simtool_data,graph):
    # firslty checking if input node list is a data frame or simpe list of node names
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
            query = """
                MATCH(technique:"""+technique_label+"""{uid:'"""+technique+"""'})<-[r:TAKES_AS_INPUT]-(preReq)
                RETURN preReq
            """
            technique_prereqs = graph.run(query).to_data_frame()
            # now add any artifacts not included in the initial path
            for preReq in technique_prereqs.preReq:
                if preReq['uid'] not in initial_path.nodeNames[0]:
                    initial_path.nodeNames[0].append(preReq['uid'])
                    added_new_node = True

                    # need to check for required techniques to generate these artifacts and add them to the solution path
                    query = """
                        MATCH(artifact:Artifact{uid:'"""+preReq['uid']+"""'})<-[r:GENERATES|EXECUTES]-(preReq:Technique|Simulation_Tool)
                        RETURN preReq
                    """
                    artifact_prereqs = graph.run(query).to_data_frame()

                    # as Executable System Model is a special case of artifact that is generated specifally by simulation tools,
                    # need to hanld this special case and only select the best simulation tool (if one has not already been selected)

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

                    # now add any techniques/simulation tools not included in the initial path
                    if not artifact_prereqs.empty:
                        for artifact_preReq in artifact_prereqs.preReq:
                            if artifact_preReq['uid'] not in initial_path.nodeNames[0]:
                                initial_path.nodeNames[0].append(artifact_preReq['uid'])
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
    return initial_path

def select_techniques(techniques_list,graph):
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
    
    # dict defining order of steps in conceptual MBSE process model
    steps = {'language':1,
            'tool':2,
            'method':3,
            'simulation_tool':4,
            'artifacts':5,
            'technique':6}

    # now find lastest step included in Scenario_context
    final_step = 0
    for key in Scenario_context:
        if steps[key]>final_step:
            final_step = steps[key]
            final_key = Scenario_context[key]

    return final_key