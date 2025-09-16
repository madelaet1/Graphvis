#********************************************************************************
#*  Script: BMP##.py
#*  Purpose: Create Business Process Maps by importing an excel spreadsheet and
#*              using graphvis to render the diagram
#*  Command line: --help
#*                --sheet: tab to read from the excel spreadsheet  
#*                --app: Appliction name to map, this is case sensitive 
#*                --legend: Include a legend and mechanism guide in the diagram.
#*  
#*  Verion: 01
#*  Author: Mike DeLaet
#*
#*  Version Notes:
#*          00 - 18-Feb 2025 | Initial development | M DeLaet
#*          01 - 9-May 2025 | Add command line arguments | M DeLaet
#*          02 - 16-Sept 2025 | Code cleanup and add --legend | M DeLaet
#*
#********************************************************************************
import pandas as pd
import argparse
import os
from graphviz import Digraph


def get_label(row):
    #Generate label for edges using the topic, or blank if NaN.
    return f"{row['Topic']}" if pd.notna(row['Topic']) else ""

'''
parser = argparse.ArgumentParser(description="Specify the sheet/tab for the mapping.")
parser.add_argument('--sheet', help="Name of the sheet/tab where the mapping is located.")

args = parser.parse_args()
'''
parser = argparse.ArgumentParser(description="Create Business Process Maps from Excel.")

parser.add_argument('--sheet', help="Name of the Excel sheet to read.", required=False)
parser.add_argument('--app', help="Application name(s) to map(case sensitive). Comma-separated for multiple or use 'all' or 'ecosystem'.", required=False)
parser.add_argument('--legend',help="Include a legend and mechanism guide in the diagram.",action='store_true')

args = parser.parse_args()


# Use the argument if provided and not just whitespace, else prompt
if args.sheet and args.sheet.strip():
    sheet = args.sheet.strip()
else:
    sheet = input("Enter the tab/sheet where the mapping is located: ").strip()

print(f"Using sheet: {sheet}")

# Load the spreadsheet
df = pd.read_excel("BMP-Data.xlsx", sheet_name=sheet)
print (f'{df}')

# List unique applications from combined App-1 and App-2 columns
#unique_apps = sorted(set(df['App-1'].dropna().str.strip().unique()) | set(df['App-2'].dropna().str.strip().unique()))
unique_apps = sorted(set(df['App-1'].dropna().str.strip().unique()))
print("Available applications to map:")
for app in unique_apps:
    print(f"- {app}")
print (f'- all: all relationships, \n- ecosystem: relationships between ecosystem applications.')

'''
# Prompt user for application to map
app_to_map = input("Enter the application name(s) to map (comma-separated for multiple): ")
app_to_map_list = [app.strip() for app in app_to_map.split(',')]
'''
# Determine the application(s) to map
if args.app and args.app.strip():
    app_to_map = args.app.strip()
else:
    app_to_map = input("Enter the application name(s) to map (comma-separated for multiple): ").strip()

app_to_map_list = [app.strip() for app in app_to_map.split(',')]



# Filter the data to include only relevant rows or "all" for all applications
if "all" in app_to_map_list:
    filtered_df = df
    graph_engine = "sfdp"
    beautify = "false"
    ranksep='10.0'    # Increases vertical separationRight
    nodesep='10.0'    # Increases horizontal separation
    app_to_map_list = unique_apps
    map_all = True
    map_ecosystem = False
elif "ecosystem" in app_to_map_list:
    beautify = "false"
    ranksep='10.0'    # Increases vertical separationRight
    nodesep='10.0'    # Increases horizontal separation
    map_all=False
    map_ecosystem = True
    graph_engine = "sfdp"
    app_to_map_list = unique_apps
    filtered_df = df[df['App-1'].fillna('').str.strip().isin(unique_apps) & df['App-2'].fillna('').str.strip().isin(unique_apps)]
else:
    filtered_df = df[df['App-1'].fillna('').str.strip().isin(unique_apps) | df['App-2'].fillna('').str.strip().isin(unique_apps)]
    map_all=False
    map_ecosystem = False
    
    if len(app_to_map_list) == 1:  
        graph_engine = "circo"
        beautify = "true"
        ranksep='5.0'    # Increases vertical separationRight
        nodesep='2.0'    # Increases horizontal separation
    else: 
        graph_engine = "sfdp"
        beautify = "false"
        ranksep='5.0'    # Increases vertical separationRight
        nodesep='5.0'    # Increases horizontal separation
        
print(f'{filtered_df}')    

# Initialize a directed graph with additional attributes
graph_format = 'svg' 
g = Digraph(format=graph_format, engine=graph_engine) # dot, neato, fdp, sfdp, circo, twopi, nop, nop2, osage, patchwork
g.attr(
    compound='true',  # Allows edges to connect between subgraphs properly
    ranksep=f'{ranksep}',    # Increases vertical separationRight
    nodesep= f'{nodesep}',    # Increases horizontal separation
    overlap='false',  # Prevents nodes from overlapping
    splines='true',    # Enables smoother edge routing
    K='.5', 
    repulsiveforce='1.25',
    overlap_scale='0',
    smoothing='avg_dist',
    beautify=beautify,
    bgcolor = 'lightyellow'
)

for i in range(len(app_to_map_list) - 1):
    g.edge(app_to_map_list[i], app_to_map_list[i + 1], label="", len="10", dir="none", style="invisible")



# Edge attributes to control edge length
g.edge_attr.update(len='4.0')  # Increases distance between connected nodes

# Filter the data to include only relevant rows or "all" for all applications
if map_all == True:
    app_shape = 'box3d'
    for app in app_to_map_list:
        g.node(app, label=app, shape=f'{app_shape}', color='deepskyblue', fillcolor='lightskyblue', style='filled')
else:
    app_shape = 'box3d'
    for app in app_to_map_list:
        g.node(app, label=app, shape=f'{app_shape}', color='deepskyblue', fillcolor='lightskyblue', style='filled')

if map_ecosystem == True:
    app_df = filtered_df[(filtered_df['App-1'].str.strip().isin(app_to_map_list))]
else:
    app_df = filtered_df[
        (filtered_df['App-1'].str.strip().isin(app_to_map_list)) | 
        (filtered_df['App-2'].str.strip().isin(app_to_map_list))
    ]

for index, row in app_df.iterrows():  # Fixed iterrow() to iterrows()
    app_name = row['App-1']  # Define the app name from the row

    if app_name in unique_apps and app_name not in app_to_map_list:  # Fixed .isin()
        g.node(app_name, label=app_name, shape='box', color='deepskyblue', fillcolor='lightskyblue', style='filled')

# Define colors for mechanisms
mechanism_colors = {
    "fileshare": "blue",
    "ftp": "red",
    "api": "green"
}

mechanism_arrow = {
    "fileshare":"odot",
    "ftp":"box",
    "api":"diamond"
}

print(f'Relationships to be mapped: {filtered_df.shape[0]}')

# Add edges within a subgraph for better organization
i=0
for application in app_to_map_list:
    #if not filtered_df[(filtered_df['App-1'].str.strip() == application)].empty:
    with g.subgraph(name=f"cluster{i}") as relationships: 
        app_df = filtered_df[(filtered_df['App-1'].str.strip() == application) | (filtered_df['App-2'].str.strip() == application)]
        if app_df.empty:
            continue

        relationships.attr(label=f"{application}", style="dashed")
        #relationships.node(application, label="", shape='box', color='deepskyblue', fillcolor='lightskyblue', style='filled')

        i += 1
        for index, row in app_df.iterrows():
            if (row['App-1'] == application) or (row['App-1'] != application and row['App-1'] not in app_to_map_list):
                try:
                    style = "solid"
                    arrowhead = "normal"
                    arrowtail = "normal"
                    direction = "forward"
                    

                    app_1 = row['App-1'].strip()
                    app_2 = row['App-2'].strip()

                    #print(f"{row['App-1']}, {row['App-2']}\n")
                    
                    direction_type = str(row['Direction']).strip().lower() if pd.notna(row['Direction']) else ""
                    mechanism_type = str(row['Mechanism']).strip().lower() if pd.notna(row['Mechanism']) else ""
                    
                    color = mechanism_colors.get(mechanism_type, "black")  # Default to black if no match
                    arrowhead = mechanism_arrow.get(mechanism_type, "normal")  # Default to black if no match

                    if app_to_map == "all":
                        label = None
                    else:
                        label = get_label(row)

                    if direction_type == "bi-directional":
                        style = "bold"
                        direction = "both"
                        arrowtail = arrowhead
                    elif direction_type == "depends on":
                        style = "dashed"

                    #print(f'Row attributes: {row['app-1']} ->{row['app-2']} {row['Direction']} {row['Mechanism']}\n')

                    if not row.empty:
                        relationships.edge(app_1, app_2, label=label, shape='box', style=style, arrowhead=arrowhead, arrowtail=arrowtail, dir=direction, color=color)
                except Exception as e:
                    print(f"Error processing row: {row}\nException: {e}\n")

    # Add a properly structured subgraph for Legend and Mechanism at the bottom

    if args.legend:
        with g.subgraph(name="cluster01") as legend:
            legend.attr(label="Legend", style="dashed")
            legend.node("Legend", shape="box")
            legend.edge("Legend", "Bi-Directional", label="Double Arrow", style="bold", dir="both")
            legend.edge("Legend", "Depends On", label="Dashed Line", style="dashed")
            legend.edge("Legend", "Normal Flow", label="Solid Line", style="solid")

        with g.subgraph(name="cluster02") as mechanism:
            mechanism.attr(label="Mechanism", style="dashed")
            mechanism.node("Mechanism", shape="box")
            mechanism.edge("Mechanism", "API", label="Green Diamond", arrowhead="diamond", color="green")
            mechanism.edge("Mechanism", "FTP", label="Red Box", arrowhead="box", color="red")
            mechanism.edge("Mechanism", "Fileshare", label="Blue Circle", arrowhead="odot", color="blue")
    
# Save and render the graph

# Create output directory if it doesn't exist
output_dir = "bmp_output"
os.makedirs(output_dir, exist_ok=True)

# Build output filename based on application(s)
if "all" in app_to_map_list:
    output_filename = "relationship_map_all"
elif "ecosystem" in app_to_map_list:
    output_filename = "relationship_map_ecosystem"
else:
    # Join app names with underscores and sanitize for file naming
    output_filename = "relationship_map_" + "_".join(app_to_map_list).replace(" ", "_").replace("/", "_")

if args.legend:
    output_filename += "_legend"

# Create full path with output directory
output_file = os.path.join(output_dir, output_filename)

g.render(output_file)


print(f"Graph generated as {output_file}.{graph_format}")


