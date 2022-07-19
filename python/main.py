from multiprocessing.sharedctypes import Value
import radix as r
import pandas as pd
import numpy as np
import ipaddress as ip
import time

class IPAM:
    def __init__(self) -> None:
        self.data = pd.read_csv("./Data/ip_data.csv", na_filter=False)
        
    def main(self) -> None:
        left_Tree = r.Radix()
        right_Tree = r.Radix()
        
        # Self Overlap
        for index, row in self.data.iterrows():
            if self.data["INFOBLOX_TABLE"][index]:
                try:
                    curr_network = str(ip.ip_network(self.data["INFOBLOX_TABLE"][index]))
                except ValueError:
                    print(f"The row : { self.data['INFOBLOX_TABLE'][index] } is not in the correct format")
                else:
                    if curr_network:
                        new_node = left_Tree.search_best(curr_network)
                        if not new_node:
                            curr_node = left_Tree.add(curr_network)
                            self.data.at[index, "SELF_OVERLAP_A"] = "NO_OVERLAP"
                            curr_node.data["INDEX"] = index
                        else:
                            self.data.at[index,"SELF_OVERLAP_A"] = new_node.data["INDEX"]
        
        for index, row in self.data.iterrows():
            if self.data["ROUTING_TABLE"][index]: 
                try:
                    curr_network = str(ip.ip_network(self.data["ROUTING_TABLE"][index]))
                except ValueError:
                    print(f"The row : { self.data['ROUTING_TABLE'][index] } is not in the correct format")
                else:
                    if curr_network:
                        new_node = right_Tree.search_best(curr_network)
                        if not new_node:
                            curr_node = right_Tree.add(curr_network)
                            self.data.at[index, "SELF_OVERLAP_B"] = "NO_OVERLAP"
                            curr_node.data["INDEX"] = index
                        else:
                            self.data.at[index, "SELF_OVERLAP_B"] = new_node.data["INDEX"]
                        
        
        # CROSS OVERLAP
        for index, row in self.data.iterrows():
            if self.data["ROUTING_TABLE"][index]:
                try:
                    curr_network = str(ip.ip_network(self.data["ROUTING_TABLE"][index]))
                except ValueError:
                    print(f"The row : { self.data['ROUTING_TABLE'][index] } is not in the correct format")
                else:
                    if curr_network:
                        new_node = left_Tree.search_best(curr_network)
                        if not new_node:
                            curr_node = left_Tree.add(curr_network)
                            self.data.at[index, "CROSS_OVERLAP"] = "NO_OVERLAP"
                            curr_node.data["INDEX"] = index
                        else:
                            self.data.at[index,"CROSS_OVERLAP"] = new_node.data["INDEX"]


        self.data.to_csv("./data/out/Output.csv")

if __name__ == '__main__':
    start = time.perf_counter()
    _ipam = IPAM()
    _ipam.main()
    end = time.perf_counter()
    print(f"The Whole script completed in {end-start:0.4f} seconds")
