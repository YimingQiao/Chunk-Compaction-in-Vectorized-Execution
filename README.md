# Data Chunk Compaction in Vectorized Execution

The Supplementary Material of our paper includes three repositories:
1. [Problem formalization and simulation](https://github.com/YimingQiao/Chunk-Compaction-Formalization)
2. **Some Microbenchmarks to compare various compaction strategies (Current Repository)**
3. [Integrate the Leaning and Logical Compaction into the Duckdb, evaluating the End-to-end performance](https://github.com/YimingQiao/Chunk-Compaction-in-Duckdb)

**Updates: The implementation of Logical Compaction has been successfully [merged into DuckDB](https://github.com/duckdb/duckdb/pull/14956)!**

---

This repository contains code that we use in the microbenchmark section of the paper. 
It includes a vectorized execution engine that supports the hash join and the filter operators. 

It implements several compaction strategies, including
 - No Compaction
 - Full Compaction
 - Binary Compaction
 - Dynamic/Learning Compaction

The code for the Logical Compaction is provided in the other branch.

We provide a compile script that can generate the executable file using each of the strategies

    bash ./build_versions.sh

The generated executable files are placed in the folder `compaction`.

    Usage: [program_name] [options]
    Options:
        --join-num [value]        Number of joins
        --chunk-factor [value]    Chunk factor
        --lhs-size [value]        Size of LHS tuples
        --rhs-size [value]        Size of RHS tuples
        --load-factor [value]     Load factor
        --payload-length=[list]   Comma-separated list of payload lengths for RHS   
                                    Example: --payload-length=[0,1000,0,0]

## Example:

    (base) yiming@golf:~/projects/compaction-project$ ./compaction/exe_binary_compaction --join-num 4 --chunk-factor 5 --lhs-size 20000000 --rhs-size 2000000 --load-factor 0.5 --payload-length=[0,0,0,0]
    ------------------ Setting ------------------
    Strategy: binary_compaction
    Number of Joins: 4
    Number of LHS Tuple: 20000000
    Number of RHS Tuple: 2000000
    Chunk Factor: 5
    Load Factor: 0.5
    RHS Payload Lengths: [0,0,0,0]
    ------------------ Statistic ------------------
    [Total Time]: 8.34773s
    -------
    Total: 1.51951 s        Calls: 244150   Avg: 6.22369e-06 s      [Binary Compact] 0x94648048003936
    Total: 0.000126583 s    Calls: 5        Avg: 2.53166e-05 s      [Binary Compact] 0x94648497661408
    Total: 1.81846 s        Calls: 230830   Avg: 7.87793e-06 s      [Binary Compact] 0x94648947646704
    Total: 1.28118 s        Calls: 48830    Avg: 2.62375e-05 s      [Join - Next] 0x94643598378848
    Total: 0.806537 s       Calls: 244150   Avg: 3.30345e-06 s      [Join - Next] 0x94647887257360
    Total: 0.449214 s       Calls: 50935    Avg: 8.81935e-06 s      [Join - Next] 0x94648048750560
    Total: 0.851043 s       Calls: 253630   Avg: 3.35545e-06 s      [Join - Next] 0x94648498572000
    Total: 0.574417 s       Calls: 9766     Avg: 5.88181e-05 s      [Join - Probe] 0x94643598378848
    Total: 0.410242 s       Calls: 48831    Avg: 8.40126e-06 s      [Join - Probe] 0x94647887257360
    Total: 0.177317 s       Calls: 10187    Avg: 1.74062e-05 s      [Join - Probe] 0x94648048750560
    Total: 0.192842 s       Calls: 50931    Avg: 3.78634e-06 s      [Join - Probe] 0x94648498572000
    -------
