import radix as r
import pandas as pd
import numpy as np

class IPAM:
    def __init__(self) -> None:
        self.data = pd.read_csv("./Data/ip_data.csv", na_filter=False)
        
    def main(self) -> None:
        left_Tree = r.Radix()
        right_Tree = r.Radix()
        
        # Self Overlap
        for index, row in self.data.iterrows():
            if self.data["INFOBLOX_TABLE"][index]:
                new_node = left_Tree.search_best(row["INFOBLOX_TABLE"])
                if not new_node:
                    curr_node = left_Tree.add(row["INFOBLOX_TABLE"])
                    self.data.at[index, "SELF_OVERLAP_A"] = "NO_OVERLAP"
                    curr_node.data["INDEX"] = index
                else: 
                    self.data.at[index, "SELF_OVERLAP_A"] = new_node.data["INDEX"]
        
        for index, row in self.data.iterrows():
            if self.data["ROUTING_TABLE"][index]:
                new_node = right_Tree.search_best(row["ROUTING_TABLE"])
                if not new_node:
                    curr_node = right_Tree.add(row["ROUTING_TABLE"])
                    self.data.at[index, "SELF_OVERLAP_B"] = "NO_OVERLAP"
                    curr_node.data["INDEX"] = index
                else:
                    self.data.at[index, "SELF_OVERLAP_B"] = new_node.data["INDEX"]
                    # self.data.loc[self.data["ROUTING_TABLE"] == new_node.prefix, "SELF_OVERLAP_B"] = new_node.prefix
                        
        
        # # CROSS OVERLAP
        for index, row in self.data.iterrows():
            if self.data["ROUTING_TABLE"][index]:
                new_node = left_Tree.search_best(row["ROUTING_TABLE"])
                if not new_node:
                    curr_node = left_Tree.add(row["ROUTING_TABLE"])
                    self.data.at[index, "CROSS_OVERLAP"] = "NO_OVERLAP"
                    curr_node.data["INDEX"] = index
                else:
                    self.data.at[index, "CROSS_OVERLAP"] = new_node.data["INDEX"]
                    # self.data.loc[self.data["ROUTING_TABLE"] ==
                    #               new_node.prefix, "CROSS_OVERLAP"] = new_node.prefix


        self.data.to_csv("./Data/Output.csv")
if __name__ == '__main__':
    ip = IPAM()
    ip.main()
