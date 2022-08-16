from multiprocessing.sharedctypes import Value
import radix as r
import pandas as pd
import numpy as np
import ipaddress as ip
import time, datetime
import collections
from enum import Enum
from copy import copy

isRunning = True
class ColumnLabel:
    NO_OVERLAP = "NO"
    SELF_OVERLAP_A = "SELF_OVERLAP_A"
    SELF_OVERLAP_B = "SELF_OVERLAP_B"
    CROSS_OVERLAP_A_2_B = "CROSS_OVERLAP_A_2_B"
    CROSS_OVERLAP_B_2_A = "CROSS_OVERLAP_B_2_A"
    COLUMN_A = "INFOBLOX_TABLE"
    COLUMN_B = "ROUTING_TABLE"
         
class IPAM:
    def __init__(self) -> None:
        self.data = pd.read_csv("./Data/ip_data.csv", na_filter=False)
        self.left_Tree = r.Radix()
        self.right_Tree = r.Radix()
        self.overlap_map_a = collections.defaultdict(list)
        self.overlap_map_b = collections.defaultdict(list)

    
    def find_self_overlap(self, src_column, src_tree, self_overlap_column_name, hash_map):

        # Self Overlap
        for index, row in self.data.iterrows():
            if self.data[src_column][index]:
                try:
                    curr_network = str(ip.ip_network(self.data[src_column][index]))
                except ValueError:
                    print(f"The row : { self.data[src_column][index] } is not in the correct format")
                else:
                    if curr_network:
                        new_node = src_tree.search_best(curr_network)
                        if not new_node:
                            curr_node = src_tree.add(curr_network)
                            self.data.at[index, self_overlap_column_name] = ColumnLabel.NO_OVERLAP
                            curr_node.data["INDEX"] = index
                        else:
                            self.data.at[index, self_overlap_column_name] = new_node.data["INDEX"]

                            if src_column == ColumnLabel.COLUMN_A:
                                self.overlap_map_a[new_node.data["INDEX"]].append(index)
                            else:
                                self.overlap_map_b[new_node.data["INDEX"]].append(index)

    def find_cross_overlap(self, src_column, src_tree, cross_overlap_column_name):

        # CROSS OVERLAP
        for index, row in self.data.iterrows():
            if self.data[src_column][index]:
                try:
                    curr_network = str(ip.ip_network(self.data[src_column][index]))
                except ValueError:
                    print(f"The row : { self.data[src_column][index] } is not in the correct format")
                else:
                    if curr_network:
                        new_node = src_tree.search_best(curr_network)
                        if not new_node:
                            curr_node = src_tree.add(curr_network)
                            self.data.at[index, cross_overlap_column_name] = ColumnLabel.NO_OVERLAP
                            curr_node.data["INDEX"] = index
                        else:
                            self.data.at[index, cross_overlap_column_name] = new_node.data["INDEX"]

    def fill_backward_overlap(self, column_to_be_checked, hash_map):

        print(f"Checking the backward overlapping for the column : {column_to_be_checked}")
        memory_map = collections.defaultdict(lambda: False)
        data_copy = self.data.copy(deep=True)
        for index, row in data_copy.iterrows():
            # ** Checking whether the current row is empty or not and equal to "NO_OVERLAP"
            if hash_map.get(index, False) and data_copy[column_to_be_checked][index] and data_copy[column_to_be_checked][index] == ColumnLabel.NO_OVERLAP:
                self.data.at[index, column_to_be_checked] = hash_map[index]

    def main(self) -> None:
        # SELF OVERLAP
        self.find_self_overlap(src_column=ColumnLabel.COLUMN_A, src_tree=self.left_Tree, self_overlap_column_name=ColumnLabel.SELF_OVERLAP_A, hash_map=self.overlap_map_a)
        self.find_self_overlap(src_column=ColumnLabel.COLUMN_B, src_tree=self.right_Tree, self_overlap_column_name=ColumnLabel.SELF_OVERLAP_B, hash_map=self.overlap_map_b)

        # CROSS OVERLAP
        self.find_cross_overlap(src_column=ColumnLabel.COLUMN_A, src_tree=self.right_Tree, cross_overlap_column_name=ColumnLabel.CROSS_OVERLAP_A_2_B)
        self.find_cross_overlap(src_column=ColumnLabel.COLUMN_B, src_tree=self.left_Tree, cross_overlap_column_name=ColumnLabel.CROSS_OVERLAP_B_2_A)

        # self.fill_backward_overlap(column_to_be_checked=ColumnLabel.SELF_OVERLAP_A, hash_map=self.overlap_map_a)
        # self.fill_backward_overlap(column_to_be_checked=ColumnLabel.SELF_OVERLAP_B, hash_map=self.overlap_map_b)

        # Output file based on Time 
        now = datetime.datetime.now().strftime("%d%m%Y_%H_%M_%S")
        self.data.to_csv(f"./Data/out/Output_{now}.csv")

if __name__ == '__main__':
    start = time.perf_counter()
    _ipam = IPAM()
    _ipam.main()
    end = time.perf_counter()
    print(f"The Whole script completed in {end-start:0.4f} seconds")
