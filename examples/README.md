# Overview
This folder includes various MapReduce examples written by Mimir.

# WordCount
* wc - WordCount implementation without combiner
* wc_cb - WordCount implementation with combiner

# Breadth-first Search
* bfs - efficient BFS implementation combining MapReduce with MPI model
* bfs_join - BFS implementation with MapReduce idea (based on join idea)

# Join
* join - join two datasets
* join_split - join with superfrequent keys split for highly skewed
  datasets

# Octree Clustering
* oc - Basic Octree Clustering implementation
* oc_cb - Octree Clustering with combiner

# Mini Test Suites
* minitest_map - Map Test
* minitest_map_shuffle - Map + Shuffle Test
* minitest_map_shuffle_reduce - Map + Shuffle + Reduce Test
* minitest_map_shuffle_combiner - Map + Shuffle + Combiner Test
* minitest_map_shuffle_io - Map + Shuffle + I/O Test
