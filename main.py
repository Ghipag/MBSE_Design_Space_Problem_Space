import pandas as pd
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
        # setup and loading of database data
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
        
        # select set of techniques
        techniques_list = ['Objective SysML Stereotypes','Genetic Optimisation','Surrogate Modelling']
        network_analysis.select_techniques(techniques_list,graph)
        data_extraction.apply_issue_cost(language_data,tool_data,method_data,graph)

        # generating a candidate 'optimal' solution path
        candidate_path = network_analysis.identify_exploration_solution('SysML V1','Globally Optimal Design Parameters',technique_data,tool_data,simtool_data,graph)
        print(f"candidate solution issue cost: {candidate_path.totalCost[0]}")
        database_tools.generate_node_match_query(candidate_path.nodeNames[0])

        # demonstrating last years solution
        solution_minimum_path = ['SysML V1','Cameo','SEAM','Cameo Simulation Toolkit','Surrogate Modelling','Genetic Optimisation','Globally Optimal Design Parameters']
        solution_full_path = network_analysis.identify_solution_path_prereqs(solution_minimum_path,technique_data,tool_data,simtool_data,graph)
        database_tools.generate_node_match_query(solution_full_path.nodeNames[0])



if __name__ == "__main__":
    main()