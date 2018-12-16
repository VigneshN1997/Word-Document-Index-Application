# Word-Document-Index-Application
Given a large collection of documents distributed in the secondary memories (i.e. hard disks) of multiple nodes in a cluster, designed a program that extracts words and their frequency of occurrence from each document and created a word-document index in each node ranked on the frequency.Each index is local to a node a word is associated with only those documents in that node.

Also designed a program to merge all indices into one large word-document index ranked by decreasing frequency to be stored in node.

Implemented this program using MPI in C.

Measured the time taken for different numbers and sizes of documents and independently varying the number of processors used. For each input (of a given number of documents of certain sizes), plotted a curve of time taken against number of processors used.

Compiling and running:

Compile to generate a.out file
mpic++ -std=c++11 driver.cpp

Run:
mpirun -np num_processes ./a.out path_to_directory

assumptions: 
in all nodes directory has same path
all nodes have file names as numbers and no two nodes have same file name.
keep the stopwords file in the nfs
