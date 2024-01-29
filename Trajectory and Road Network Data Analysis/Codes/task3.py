import os
import csv
from fmm import FastMapMatch, Network, NetworkGraph, UBODTGenAlgorithm, UBODT, FastMapMatchConfig
from tqdm import tqdm
import pandas as pd

def fmm_map_matching(network_path, ubodt_path, input_csv, output_csv, k=6, radius=0.05, gps_error=0.0002, regenerate_ubodt=False, threshold=0.02):
    # Check if all files exist
    if not os.path.exists(network_path):
        print(f"Network file {network_path} does not exist.")
        return
    
    if not os.path.exists(input_csv):
        print(f"Input CSV file {input_csv} does not exist.")
        return
    
    if regenerate_ubodt and os.path.exists(ubodt_path):
        os.remove(ubodt_path)

    # Load the network
    network = Network(network_path, "fid", "u", "v")
    print(f"Nodes {network.get_node_count()} edges {network.get_edge_count()}")
    graph = NetworkGraph(network)

    # Generate UBODT if not exists or if regeneration is forced
    if not os.path.exists(ubodt_path) or regenerate_ubodt:
        ubodt_gen = UBODTGenAlgorithm(network, graph)
        status = ubodt_gen.generate_ubodt(ubodt_path, threshold, binary=False, use_omp=True)
        if not status:
            print("Error generating UBODT.")
            return
        print("UBODT generated successfully.")

    # Load UBODT
    ubodt = UBODT.read_ubodt_csv(ubodt_path)

    # Create FMM model
    model = FastMapMatch(network, graph, ubodt)
    fmm_config = FastMapMatchConfig(k, radius, gps_error)

    # Process the input CSV and write results to output CSV
    with open(input_csv, "r") as in_csv, open(output_csv, "w", newline='') as out_csv:
        reader = csv.reader(in_csv)
        writer = csv.writer(out_csv)
        
        # Skip header in input and write header in output
        next(reader)

        # TODO
        writer.writerow(["Index", "cpath", "mgeom", "opath", "offset", "length", "spdist"])

        for index, row in tqdm(enumerate(reader)):
            gps = eval(row[8])
            wkt = 'LINESTRING(' + ','.join([' '.join([str(j) for j in i]) for i in gps]) + ')'
            result = model.match_wkt(wkt, fmm_config)

            candidates = list(result.candidates)
            writer.writerow([
                index,
                str(list(result.cpath)),
                result.mgeom.export_wkt(),
                str(list(result.opath)),
                str([c.offset for c in candidates]),
                str([c.length for c in candidates]),
                str([c.spdist for c in candidates])
            ])
            
            print('done with row: ', index)
            
            
if __name__ == "__main__":
    fmm_map_matching(
        network_path="data/porto/edges.shp",
        ubodt_path="data/ubodt.txt",
        input_csv="data/train-1500.csv",
        output_csv="data/matched_routines.csv"
    )
    
    file_path = 'data/matched_routines.csv'
    df = pd.read_csv(file_path, nrows = 5)
    df.head(5)