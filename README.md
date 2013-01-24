What’s Next? Music Recommendation System
========================================
### Karl Bo Lopker, Stephanie Smith, Sarah Jones

Description
-----------
This project is a Hadoop/Mapreduce implementation of the K-nearest neighbor similarity algorithm. Our main contribution was parallelizing KNN's training method for use in MapReduce.

Write ups
--------
* [Proposal](https://docs.google.com/document/d/18yulhDmP1zktvbdwsnsth73NDUhM3rBdnQpP5M5wRDc/edit)
* [Final paper](https://github.com/blopker/KDD-Music-Recommender-MapReduce/blob/master/CS290NKNNMapReduceFinalPaper.pdf?raw=true)

Dependencies
------------
* [args4j 2.0.21](http://args4j.kohsuke.org/)
* [Hadoop 0.20.2](http://hadoop.apache.org/)

Contents
--------
* smalltest.txt - A small database to test with. Can be run through the entire process.
* chunkit.py - Python script to chunk up a database file to be consumed by MapReduce.

Usage
-----
k is the number of similarities per song to generate.
r is the minimum number of ratings a similarity should have to be valid.


Sequential neighborhood generator:

	KDD-Music-Recommender.jar -k N -r N database

MapReduce neighborhood generator:

	hadoop jar KDD-Music-Recommender.jar -p [-k N] dirContainingChunks output

Query the neighborhood file:

	KDD-Music-Recommender.jar -q -t D -n neighborhoodFile -u activeUserFile database
