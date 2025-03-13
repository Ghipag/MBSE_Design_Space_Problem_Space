from py2neo import Graph
import pandas as pd
from pycirclize import Circos
import data_extraction
import database_tools
import network_analysis
import numpy as np


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

def test_for_all_artifacts():

    # load all info initially
    database_tools.clear_database(graph)
    data_extraction.process_Ontology_data(graph)
    language_data = data_extraction.read_data('Languages')
    data_extraction.process_language_data(language_data,graph)
    tool_data = data_extraction.read_data('Tools')
    data_extraction.process_tool_data(tool_data,graph)
    method_data = data_extraction.read_data('Methods')
    data_extraction.process_method_data(method_data,graph)
    simtool_data = data_extraction.read_data('SimTools')
    data_extraction.process_simtool_data(simtool_data,tool_data,graph)
    issue_data = data_extraction.read_data('Issues')
    data_extraction.process_issue_data(issue_data,graph)
    technique_data = data_extraction.read_data('Techniques')
    data_extraction.process_technique_data(technique_data,graph)
    actor_data = data_extraction.read_data('Actors')
    data_extraction.process_actor_data(actor_data,graph)

    # get list of artifacts
    query = """
                    MATCH(artifact:Artifact)
                    RETURN artifact
                """
    artifacts = graph.run(query).to_data_frame()

    artifact_list = []

    for artifact in artifacts.artifact:
        artifact_list.append(artifact['uid'])

    # get list of available mathos
    query = """
                    MATCH(method:Method)
                    RETURN method
                """
    methods = graph.run(query).to_data_frame()

    method_list = []

    for method in methods.method:
        method_list.append(method['uid'])


    # for each, check can find a path
    method_list = ['SEAM']
    paths = []
    for method in method_list:
        for artifact in artifact_list:
            paths.append(identify_process(MBSE_environment = {'Language':'SysML_V1',
                                'Tool':'Cameo',
                                'Method':method,
                                'Simulation_Tool':'Cameo_Simulation_Toolkit'}, #Design_Constraints
                        solution_end = artifact,
                        techniques_list = [],
                        suggest_techniques = True,
                        varaibility_types = ['Parameter']))
            
    columns = ['setup_info', 'Path Issue Cost', 'Path Query']
    results_df = pd.DataFrame(paths, columns=columns)
        
    results_df.to_csv('suggested_process_paths.csv')

    # plotting on chord diagram   
    # need to post process data
    # first check number of valid paths
    no_valid_paths = 0
    for path in paths:       
        if path[1] is not 'DNF':
            no_valid_paths += 1


    row_names = []
    col_names = ['SEAM']
    matrix_data = []
    index = 0
    row_data = np.zeros(no_valid_paths)
    for path in paths:       
        if path[1] is not 'DNF':
            row_names.append(path[0].split("'}")[1].split("['")[0])
            # col_names.append()
            path_cost= path[1]
            row_data[index] = path_cost
            print(row_data)
            index += 1
            
    matrix_data.append(row_data)

    print(matrix_data)
    print(row_names)
    print(col_names)
    matrix_df = pd.DataFrame(matrix_data, index=col_names, columns=row_names)
    print(matrix_data)

    # Initialize from matrix (Can also directly load tsv matrix file)
    circos = Circos.initialize_from_matrix(
        matrix_df,
        space=3,
        r_lim=(93, 100),
        cmap="tab10",
        ticks_interval=500,
        label_kws=dict(r=94, size=12, color="white"),
    )

    circos.savefig("chord_diagram01.png")


def identify_process(MBSE_environment = {'Language':'SysML_V1',
                            'Tool':'Cameo',
                            'Method':'SEAM',
                            'Simulation_Tool':'Cameo_Simulation_Toolkit'}, #Design_Constraints
        solution_end = 'Globally_Optimal_Design_Parameters',
        #'Globally_Optimal_Design_Parameters',
        techniques_list = [],
        suggest_techniques = True,
        varaibility_types = ['Parameter']):
        #'Surrogate_Modelling','Constrained_Genetic_Optimisation'

    #####################################################################
    # Setup and loading of database data
    #####################################################################
    
    database_tools.clear_database(graph)
    data_extraction.process_Ontology_data(graph)
    language_data = data_extraction.read_data('Languages')
    data_extraction.process_language_data(language_data,graph)
    tool_data = data_extraction.read_data('Tools')
    data_extraction.process_tool_data(tool_data,graph)
    method_data = data_extraction.read_data('Methods')
    data_extraction.process_method_data(method_data,graph)
    simtool_data = data_extraction.read_data('SimTools')
    data_extraction.process_simtool_data(simtool_data,tool_data,graph)
    issue_data = data_extraction.read_data('Issues')
    data_extraction.process_issue_data(issue_data,graph)
    technique_data = data_extraction.read_data('Techniques')
    data_extraction.process_technique_data(technique_data,graph)
    actor_data = data_extraction.read_data('Actors')
    data_extraction.process_actor_data(actor_data,graph)

    
    #####################################################################
    # Defintion of Scenario
    # i.e. what aspects of the MBSE envirnoment are being used and what are
    # the available techniques and desired outputs
    #####################################################################


    # define scenario context, i.e. MBSE environment
    solution_start =  MBSE_environment['Language']#network_analysis.set_solution_start(MBSE_environment,graph)
    #MBSE_environment['Language']

    # select set of techniques
    network_analysis.select_techniques(techniques_list,graph)
    data_extraction.apply_issue_cost(language_data,tool_data,method_data,graph)

    # select given MBSE environment elements(language, tool and method) in data base
    network_analysis.select_environment_elements(MBSE_environment,graph)



    #####################################################################
    # Generating a candidate 'optimal' solution path from defined scenario
    #####################################################################
    # firstly delabel any techniques that dont match the required variability types
    if varaibility_types != 'all':
        network_analysis.deselect_irrelevant_techniques(varaibility_types,graph)


    candidate_path = network_analysis.identify_exploration_solution(solution_start,solution_end,MBSE_environment,technique_data,techniques_list,suggest_techniques,language_data,tool_data,method_data,simtool_data,graph)
    print(candidate_path)
    # print(candidate_path['path'][0])
    setup_description = str(MBSE_environment)+'_'+solution_end+'_'+str(varaibility_types)
    total_cost = 'DNF'
    process_query = ''
    if 'totalCost' in candidate_path.keys():
        total_cost = candidate_path.totalCost[0]
        print(f"candidate solution issue cost: {total_cost}")
        process_query = database_tools.generate_node_match_query(candidate_path.nodeNames[0])
    return setup_description,total_cost,process_query

    # demonstrating last years solution
    # solution_minimum_path = ['SysML V1','Cameo','SEAM','Cameo Simulation Toolkit','Surrogate Modelling','Genetic Optimisation','Globally Optimal Design Parameters']
    # solution_full_path = network_analysis.identify_solution_path_prereqs(solution_minimum_path,True,technique_data,techniques_list,tool_data,simtool_data,graph)
    # database_tools.generate_node_match_query(solution_full_path.nodeNames[0])

    # # demonstrating upcoming solution
    # solution_minimum_path = ['Capella Language','Capella','ARCADIA','Neural Network Assisted Language Modeling for Architecture Generation and Engineering']
    #solution_path = network_analysis.identify_solution_path_prereqs(solution_minimum_path,True,technique_data,techniques_list,tool_data,simtool_data,graph)
    #solution_full_path = network_analysis.identify_technique_outputs('Neural Network Assisted Language Modeling for Architecture Generation and Engineering',solution_path,graph)
    #database_tools.generate_node_match_query(solution_full_path.nodeNames[0])

    #https://neo4j.com/labs/neosemantics/4.0/inference/
    # inference queries:
    """
    CALL n10s.inference.nodesLabelled('MBSE_Environment_Element',  {
    catNameProp: "dbLabel",
    catLabel: "Type",
    subCatRel: "NARROWER_THAN"
    })
    YIELD node
    RETURN node.uid as uid, labels(node) as categories;
    """

    """
    MATCH (technique:Technique)<-[:FORMS_INPUT_FOR]-(artifact:Artifact)<-[:GENERATES]-(method:Method {uid:'SEAM'})
    RETURN technique
    """
    """
    MATCH (technique:Technique)<-[:FORMS_INPUT_FOR]-(artifact:Artifact)<-[:GENERATES]-(method:Method)<-[:CAN_IMPLEMENT]-(tool:Tool{uid:'Cameo'})
    RETURN technique
    """
    """
    MATCH (technique:Technique)<-[:FORMS_INPUT_FOR]-(artifact:Artifact)<-[:GENERATES]-(method:Method)<-[:CAN_IMPLEMENT]-(tool:Tool)<-[:AVAILABLE_IN]-(language:Language)
    RETURN technique
    """

    """
    MATCH (technique:Technique)-[:SOLVES]->(artifact:Issue)-[:AFFECTS]->(method:Method {uid:'SEAM'}) RETURN technique
    """
if __name__ == "__main__":
    test_for_all_artifacts()