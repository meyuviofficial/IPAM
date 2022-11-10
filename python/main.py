import radix as r
import pandas as pd
import ipaddress as ip
import time
import datetime
import collections
from os import path
import argparse as args

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
    def __init__(self, _input, _output) -> None:
        self.data = pd.read_csv(_input, na_filter=False)
        self._output = _output
        self.left_Tree = r.Radix()
        self.right_Tree = r.Radix()
        self.overlap_map_a = collections.defaultdict(list)
        self.overlap_map_b = collections.defaultdict(list)
        self.cross_overlap_map_a = collections.defaultdict(list)
        self.cross_overlap_map_b = collections.defaultdict(list)

    def find_self_overlap(self, src_column, src_tree, self_overlap_column_name):

        # Self Overlap
        for index, row in self.data.iterrows():
            if self.data[src_column][index]:
                try:
                    curr_network = str(ip.ip_network(self.data[src_column][index]))
                except ValueError:
                    print(f"The row : {self.data[src_column][index]} is not in the correct format")
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
                    print(f"The row : {self.data[src_column][index]} is not in the correct format")
                else:
                    if curr_network:
                        new_node = src_tree.search_best(curr_network)
                        if not new_node:
                            curr_node = src_tree.add(curr_network)
                            self.data.at[index, cross_overlap_column_name] = ColumnLabel.NO_OVERLAP
                            curr_node.data["INDEX"] = index
                        else:
                            self.data.at[index, cross_overlap_column_name] = new_node.data["INDEX"]

                            if src_column == ColumnLabel.COLUMN_A:
                                self.cross_overlap_map_a[new_node.data["INDEX"]].append(index)
                            else:
                                self.cross_overlap_map_b[new_node.data["INDEX"]].append(index)

    def fill_backward_overlap(self, column_to_be_checked, hash_map):

        print(f"Checking the backward overlapping for the column : {column_to_be_checked}")
        # memory_map = collections.defaultdict(lambda: False)
        data_copy = self.data.copy(deep=True)
        for index, row in data_copy.iterrows():
            # ** Checking whether the current row is empty or not and equal to "NO_OVERLAP"
            if hash_map.get(index, False) and data_copy[column_to_be_checked][index] and \
                    data_copy[column_to_be_checked][index] == ColumnLabel.NO_OVERLAP:
                self.data.at[index, column_to_be_checked] = hash_map[index]

    def main(self) -> None:
        # SELF OVERLAP
        self.find_self_overlap(src_column=ColumnLabel.COLUMN_A, src_tree=self.left_Tree,
                               self_overlap_column_name=ColumnLabel.SELF_OVERLAP_A)
        self.find_self_overlap(src_column=ColumnLabel.COLUMN_B, src_tree=self.right_Tree,
                               self_overlap_column_name=ColumnLabel.SELF_OVERLAP_B)

        # Fill Backward Overlap
        self.fill_backward_overlap(column_to_be_checked=ColumnLabel.SELF_OVERLAP_A, hash_map=self.overlap_map_a)
        self.fill_backward_overlap(column_to_be_checked=ColumnLabel.SELF_OVERLAP_B, hash_map=self.overlap_map_b)

        # CROSS OVERLAP
        self.find_cross_overlap(src_column=ColumnLabel.COLUMN_A, src_tree=self.right_Tree, cross_overlap_column_name=ColumnLabel.CROSS_OVERLAP_A_2_B)
        self.find_cross_overlap(src_column=ColumnLabel.COLUMN_B, src_tree=self.left_Tree, cross_overlap_column_name=ColumnLabel.CROSS_OVERLAP_B_2_A)

        # Fill Backward Overlap
        self.fill_backward_overlap(column_to_be_checked=ColumnLabel.CROSS_OVERLAP_A_2_B, hash_map=self.cross_overlap_map_b)
        self.fill_backward_overlap(column_to_be_checked=ColumnLabel.CROSS_OVERLAP_B_2_A, hash_map=self.cross_overlap_map_a)

        # print(self.cross_overlap_map_a, self.cross_overlap_map_a)
        # Output file based on Time
        now = datetime.datetime.now().strftime("%d%m%Y_%H_%M_%S")
        self.data.to_csv(f"{self._output[:-4]}_{now}.csv")


if __name__ == '__main__':
    start = time.perf_counter()
    parser = args.ArgumentParser()
    parser.add_argument("-i", "--input", help="Path of the input file")
    parser.add_argument("-o", "--output", help="Path of the output file")
    parser.add_argument("-c1", "--column1", help="Column 1 of the Input file")
    parser.add_argument("-c2", "--column2", help="Column 2 of the Input file")
    args = parser.parse_args()

    _input = args.input
    _output = args.output

    if not path.exists(_input):
        print("OOPS !! The path that you've entered is not right. Please re-run the script with the right path!")
    else:
        _ipam = IPAM(_input, _output)
        _ipam.main()
    end = time.perf_counter()
    print(f"The Whole script completed in {end - start:0.4f} seconds")
