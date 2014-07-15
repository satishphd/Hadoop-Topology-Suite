Hadoop-Topology-Suite
=====================
Supports Polygon Overlay and Geometric Set operations namely Polygon Intersection, Union, Difference and Spatial Indexing 
using MapReduce and Hadoop. There are 3 different implementations based on 1) Single Map and Reduce phase 2) Chain of MapReduce phases, and 3) Single Map Phase only using Distributed Cache. The implementation is based on the paper "MapReduce algorithms for GIS Polygonal Overlay Processing". For more details: http://cs.gsu.edu/~dimos/sites/default/files/MapReduce%20algorithms%20for%20GIS%20Polygonal%20Overlay%20Processing.pdf

Uses third party R-tree implementation for filtering potentially intersecting polygons and Java port of General Polygon Clipping library by Daniel Bridenbecker.   

Format of Input Base Layer:
(Layer identifier) (polygon-id) (4 co-ordinates of MBR) (list of vertex co-ordinates of polygon)

e.g.
b polygon-id mbr1x mbr1y mbr2x mbr2y v1x v1y v2x v2y ..... vnx,vny 

Format of Input Overlay Layer is same without the "b" in the beginning
