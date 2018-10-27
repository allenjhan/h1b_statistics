# Table of Contents
1. [Problem](README.md#problem)
2. [Approach](README.md#approach)
3. [Run](README.md#run)

# Problem
The problem is to do some data processing on some text files containing H-1B visas, from the US Department of Labor's Office of Foreign Labor Certification Performance Data. Some files are provided in the input directory that have been pre-processed from Excel format into CSV. The goal is to calculate the statistics for all the files contained in this directory. So if the input directory contains three files with visa data, then statistics are calculated on the combined data from all three files.

# Approach
The approach chosen in this submission is to use Scala, because of its concise, functionally-driven Collections syntax. The presence of the groupBy function saves us the trouble of having to implement it ourselves from scratch. Scala also provides parallel collections for better performance for operations that are parallelizable. The present submission tries to leverage Scala's parallel collections by using the immutable ParVector, for faster performance. Care has been taken to use the parallelizable reduce function, by making sure the operations called by it are associative.

Due to the memory limitations of the Java Virtual Machine, the present submission processes the supplied data in batches of 50,000 entries. 50,000 is chosen as a number that safely respects the JVM's memory limitations, without trying to increase the amount of available memory with option flags. By calculating the statistics for each batch separately and combining afterwards, we avoid the problem of running out of memory if we were to try to load the entire dataset into memory at once before trying to process it.

The present implementation takes into account some quirks of the prepared dataset. For example, instead of naively attempting to split each line of data on the semicolon delimiter, the data is first parsed so that fields are separated by caret. This is because some fields that are enclosed in double quotes contain embedded semicolons. These semicolons are part of the data and should not be used to split the data. To deal with this problem, a character not found in the dataset, the caret symbol, is replaced for every use of the semicolon as a field delimitter.

# Run
To run the code, simply type **source ./run.sh** in the project directory. It is important that you run this shellscript from the top-level directory of this project, as the Scala code uses the current directory of the project as an argument. The submission here therefore assumes that the user runs this shellscript from the directory it is contained in. If you are not in the top-level directory for this project when running run.sh, please be sure to cd into it before running it.
