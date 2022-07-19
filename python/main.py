from turtle import left, right
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
                    self.data.at[index, "SELF_OVERLAP_A"] = "NO"
                else: 
                    self.data.at[index, "SELF_OVERLAP_A"] = "YES"
        
        for index, row in self.data.iterrows():
            if self.data["ROUTING_TABLE"][index]:
                # print(self.data["ROUTING_TABLE"][index]) 
                new_node = right_Tree.search_best(row["ROUTING_TABLE"])
                if not new_node:
                    curr_node = right_Tree.add(row["ROUTING_TABLE"])
                    self.data.at[index, "SELF_OVERLAP_B"] = "NO"
                else:
                    self.data.at[index, "SELF_OVERLAP_B"] = "YES"
        
        # # CROSS OVERLAP
        for index, row in self.data.iterrows():
            if self.data["ROUTING_TABLE"][index]:
                # print(self.data["ROUTING_TABLE"][index])
                new_node = left_Tree.search_best(row["ROUTING_TABLE"])
                if not new_node:
                    curr_node = left_Tree.add(row["ROUTING_TABLE"])
                    self.data.at[index, "CROSS_OVERLAP"] = "NO"
                else:
                    self.data.at[index, "CROSS_OVERLAP"] = "YES"

        self.data.to_csv("./Data/Output.csv", index=False)
if __name__ == '__main__':
    ip = IPAM()
    ip.main()
