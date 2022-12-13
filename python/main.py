# TODO: Create a custom function based on customizable column values
# TODO: All the four functions should be individually callable
#  1. Self_overlap_A,
#  2. Self_Overlap_B,
#  3. Cross_Overlap_A_2_B and
#  4. Cross_overlap_B_2_A

import argparse as args
import pandas as pd
import radix as r
import ipaddress as ip
import datetime
import collections
import time

# Defining the root variables
left_tree = r.Radix()
right_tree = r.Radix()
self_overlap_map_a = collections.defaultdict(list)
self_overlap_map_b = collections.defaultdict(list)


class Input:
    def __init__(self, _input: str, _output: str, column1: str, column2: str):
        self._input = _input
        self._output = _output
        self.column1 = column1
        self.column2 = column2

    @property
    def input(self):
        return self._input

    @property
    def output(self):
        return self._output


class ColumnLabel:
    NO_OVERLAP = "NO"
    SELF_OVERLAP_A = "SELF_OVERLAP_A"
    SELF_OVERLAP_B = "SELF_OVERLAP_B"
    CROSS_OVERLAP_A_2_B = "CROSS_OVERLAP_A_2_B"
    CROSS_OVERLAP_B_2_A = "CROSS_OVERLAP_B_2_A"


def fill_the_tree(tree, data, column_name):
    for index, row in data.iterrows():
        if data[column_name][index]:
            print(data[column_name][index])


def self_overlap(data, tree, column_name, self_overlap_column_name, overlap_map):
    print("Starting the function self overlap for the column : ", column_name)
    # find all the ips present in the given column
    # then add all the ips one by one
    # if any ip is already present, then self overlap is present

    for index, row in data.iterrows():
        if data[column_name][index]:
            curr_network = ip.ip_network(data[column_name][index])
            for current_ip in curr_network:
                _ip = str(current_ip)
                new_node = tree.search_exact(_ip)
                if not new_node:
                    curr_node = tree.add(_ip)
                    curr_node.data["INDEX"] = index
                    data.at[index, self_overlap_column_name] = ColumnLabel.NO_OVERLAP
                else:
                    data.at[index, self_overlap_column_name] = new_node.data["INDEX"]
                    # overlap_map[new_node.data["INDEX"]].append(index)


def cross_overlap(data, column_name, tree, cross_overlap_column_name):
    print("Starting the function cross overlap", cross_overlap_column_name)

    for index, row in data.iterrows():
        if data[column_name][index]:
            curr_network = ip.ip_network(data[column_name][index])

            flag = True
            for curr_ip in curr_network:
                _ip = str(curr_ip)
                new_node = tree.search_exact(_ip)
                if new_node:
                    data.at[index, cross_overlap_column_name] = new_node.data["INDEX"]
                    flag = False
            if flag:
                data.at[index, cross_overlap_column_name] = ColumnLabel.NO_OVERLAP


def fill_backward_overlap(data, column_to_be_checked, hash_map):

    print(f"Checking the backward overlapping for the column : {column_to_be_checked}")

    data_copy = data.copy(deep=True)
    for index, row in data_copy.iterrows():
        # ** Checking whether the current row is empty or not and equal to "NO_OVERLAP"
        if hash_map.get(index, False) and data_copy[column_to_be_checked][index] and \
                data_copy[column_to_be_checked][index] == ColumnLabel.NO_OVERLAP:
            data.at[index, column_to_be_checked] = hash_map[index]


def main(_inputs: Input) -> None:
    data = pd.read_csv(_inputs.input, na_filter=False)

    self_overlap(data, left_tree, inputs.column1, ColumnLabel.SELF_OVERLAP_A, self_overlap_map_a)
    self_overlap(data, right_tree, inputs.column2, ColumnLabel.SELF_OVERLAP_B, self_overlap_map_b)

    # fill_backward_overlap(data, ColumnLabel.SELF_OVERLAP_A, self_overlap_map_a)
    # fill_backward_overlap(data, ColumnLabel.SELF_OVERLAP_B, self_overlap_map_b)

    print(left_tree)
    print("\n")
    print(right_tree)

    cross_overlap(data, inputs.column1, right_tree, ColumnLabel.CROSS_OVERLAP_A_2_B)
    cross_overlap(data, inputs.column2, left_tree, ColumnLabel.CROSS_OVERLAP_B_2_A)

    now = datetime.datetime.now().strftime("%d%m%Y_%H_%M_%S")
    data.to_csv(f"{_inputs.output[:-4]}_{now}.csv")


if __name__ == '__main__':
    start = time.perf_counter()
    # Getting the inputs from the command line
    parser = args.ArgumentParser(description="add the arguments required for running the comparison script")
    parser.add_argument('-i', '--input', help='provide the input file path')
    parser.add_argument('-o', '--output', help='provide the output file path')
    parser.add_argument('-c1', '--column1', help='Name of the column 1 to be compared')
    parser.add_argument('-c2', '--column2', help='Name of the column 2 to be compared')

    var = parser.parse_args()
    inputs = Input(var.input, var.output, var.column1, var.column2)
    print(f"Given Inputs are Input file path: {inputs.input} and output file path: {inputs.output}")
    print(f"Processing the comparison between the column : {inputs.column1} and column : {inputs.column2}")

    main(inputs)
    end = time.perf_counter()
    print(f"The Whole script completed in {end - start:0.4f} seconds")