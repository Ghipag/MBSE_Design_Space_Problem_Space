from py2neo import Graph
import data_extraction
import database_tools
import network_analysis


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

def main():
    if __name__== "__main__":
        #####################################################################
        # Setup and loading of database data
        #####################################################################
        
        database_tools.clear_database(graph)
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

        # define desired outcome
        solution_end = 'Globally Optimal Design Parameters'

        # define scenario context, i.e. MBSE environment
        MBSE_environment = {'language':'SysML V1',
                            'tool':'Cameo',
                            'method':'SEAM',
                            'simulation_tool':'Cameo Simulation Toolkit'}
        solution_start = network_analysis.set_solution_start(MBSE_environment,graph)

        # select set of techniques
        techniques_list = ['Objective SysML Stereotypes','Constrained Genetic Optimisation','Design constraint and dependant variable SysML Stereotypes','Parametric Variability SysML Stereotypes','Design of experiments','Surrogate Modelling']
        network_analysis.select_techniques(techniques_list,graph)
        data_extraction.apply_issue_cost(language_data,tool_data,method_data,graph)


        #####################################################################
        # Generating a candidate 'optimal' solution path from defined scenario
        #####################################################################
        candidate_path = network_analysis.identify_exploration_solution(solution_start,solution_end,MBSE_environment,technique_data,techniques_list,False,tool_data,simtool_data,graph)
        print(f"candidate solution issue cost: {candidate_path.totalCost[0]}")
        database_tools.generate_node_match_query(candidate_path.nodeNames[0])

        # demonstrating last years solution
        solution_minimum_path = ['SysML V1','Cameo','SEAM','Cameo Simulation Toolkit','Surrogate Modelling','Genetic Optimisation','Globally Optimal Design Parameters']
        solution_full_path = network_analysis.identify_solution_path_prereqs(solution_minimum_path,True,technique_data,techniques_list,tool_data,simtool_data,graph)
        database_tools.generate_node_match_query(solution_full_path.nodeNames[0])

        # demonstrating upcoming solution
        solution_minimum_path = ['Capella Language','Capella','ARCADIA','Neural Network Assisted Language Modeling for Architecture Generation and Engineering']
        solution_path = network_analysis.identify_solution_path_prereqs(solution_minimum_path,True,technique_data,techniques_list,tool_data,simtool_data,graph)
        solution_full_path = network_analysis.identify_technique_outputs('Neural Network Assisted Language Modeling for Architecture Generation and Engineering',solution_path,graph)
        database_tools.generate_node_match_query(solution_full_path.nodeNames[0])

        #https://neo4j.com/labs/neosemantics/4.0/inference/


if __name__ == "__main__":
    main()