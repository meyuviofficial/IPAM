# Introduction

A Python script to find the overlapping IP Subnets between two different sources using Radix Tree.

## Working

This file categorizes the overlapping subnets based on the following three categories

    1. No_Overlap - The current subnet doesn't overlaps with any record in the whole dataset.
    2. Self_Subnet_Overlap_XYZ - The current subnet overlaps with another subnet in the current dataset. The current entry is a subnet of another supernet in the same dataset.
    3. Self_Supernet_Overlap_ABC - The current subnet overlaps with another subnet in the current dataset. The current entry is a supernet that has another subnet already occupied.
    4. Cross_Subnet_Overlap_XYZ - The current subnet overlaps with another subnet in the current dataset. The current entry is a subnet of another supernet in the same dataset.
    5. Cross_Supernet_Overlap_ABC - The current subnet overlaps with another subnet in the current dataset. The current entry is a supernet that has another subnet already occupied.

