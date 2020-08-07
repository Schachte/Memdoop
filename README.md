In-Memory Filesystem for Hadoop
========================

### Tests

[![Build Status](https://travis-ci.org/Schachte/Memdoop.svg?branch=master)](https://travis-ci.org/Schachte/Memdoop)

This is a memory based implementation of the `hadoop.fs.FileSystem`. It is 
designed to be used for testing code written for the Hadoop environment. 

### Use-Case

There are two primary use-cases that have inspired me to utilize this module. The first use-case
is centered around testing. When writing unit-tests that are applicable to files being written
to the hadoop file-system, it becomes hard to assert the creation and content of those files
when unit testing individual pieces of the code.

The second use-case comes into play when dealing with Apache `ORC` files. `ORC` is a highly performant
compressed file format used in big-data. When streaming data in and writing ORC files directly,
the ORC file writer references the Hadoop FS interface. This interface is not directly compatible 
with the standard Java FS object interface. To mitigate this issue, this repository intercepts
the Hadoop calls to keep everything stored as outputstreams in the system memory. When you want
to upload this information to disk/cloud storage then you can rip them out of the VFS. 


### Inspiration

Inspiration for this repo has been derived from the parent project:

https://github.com/rastest/hadoop-memory-filesystem
