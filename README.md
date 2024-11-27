# Data Chunk Compaction in Vectorized Execution

The Supplementary Material of our paper includes three repositories:
1. [Problem formalization and simulation](https://github.com/YimingQiao/Chunk-Compaction-Formalization)
2. **Some Microbenchmarks to compare various compaction strategies (Current Repository)**
3. [Integrate the Leaning and Logical Compaction into the Duckdb, evaluating the End-to-end performance](https://github.com/YimingQiao/duckdb/tree/logical_cpt)

**Updates: The implementation of Logical Compaction has been successfully [merged into DuckDB](https://github.com/duckdb/duckdb/pull/14956)!**

---

This repository contains code that we use in the microbenchmark section of the paper. 
It includes a vectorized execution engine that supports the hash join and the filter operators. 

It implements several compaction strategies, including
 - Logical Compaction

We provide a compile script that can generate the executable file using the strategies

    bash ./build_versions.sh

You can find the code of other compaction strategies in the other branch. 

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

    (base) yiming@golf:~/projects/compaction-project$ ./compaction/exe_logical_compaction --join-num 4 --chunk-factor 5 --lhs-size 20000000 --rhs-size 2000000 --load-factor 0.5 --payload-length=[0,0,0,0]
    ------------------ Setting ------------------
    Strategy: logical_compaction
    Number of Joins: 4
    Number of LHS Tuple: 20000000
    Number of RHS Tuple: 2000000
    Chunk Factor: 5
    Load Factor: 0.5
    RHS Payload Lengths: [0,0,0,0]
    ------------------ Statistic ------------------
    [Total Time]: 4.07585s
    -------
    Total: 1.26864 s        Calls: 48830    Avg: 2.59807e-05 s      [Join - Next] 0x93854344831568
    Total: 0.615136 s       Calls: 72980    Avg: 8.42883e-06 s      [Join - Next] 0x93858637800000
    Total: 0.462437 s       Calls: 82860    Avg: 5.58094e-06 s      [Join - Next] 0x93858827360528
    Total: 0.464402 s       Calls: 90620    Avg: 5.12472e-06 s      [Join - Next] 0x93859254106048
    Total: 0.578376 s       Calls: 9766     Avg: 5.92234e-05 s      [Join - Probe] 0x93854344831568
    Total: 0.281568 s       Calls: 14596    Avg: 1.92908e-05 s      [Join - Probe] 0x93858637800000
    Total: 0.172811 s       Calls: 16572    Avg: 1.04279e-05 s      [Join - Probe] 0x93858827360528
    Total: 0.129722 s       Calls: 18709    Avg: 6.93368e-06 s      [Join - Probe] 0x93859254106048
    -------
    
