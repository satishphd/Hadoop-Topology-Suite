Hadoop-Topology-Suite (http://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=6650985)
=====================
Supports Polygon Overlay and Geometric Set operations namely Polygon Intersection, Union, Difference and Spatial Indexing 
using MapReduce and Hadoop. There are 3 different implementations based on 1) Single Map and Reduce phase 2) Chain of MapReduce phases, and 3) Single Map Phase only using Distributed Cache. The implementation is based on the paper "MapReduce algorithms for GIS Polygonal Overlay Processing". For more details: http://cs.gsu.edu/~dimos/sites/default/files/MapReduce%20algorithms%20for%20GIS%20Polygonal%20Overlay%20Processing.pdf

Uses third party R-tree implementation for filtering potentially intersecting polygons and Java port of General Polygon Clipping library by Daniel Bridenbecker.   

Format of Input Base Layer:
(Layer identifier) (polygon-id) (4 co-ordinates of MBR) (list of vertex co-ordinates of polygon)

b polygon-id mbr1x mbr1y mbr2x mbr2y v1x v1y v2x v2y ..... vnx,vny 

e.g.

b 1 31.42464 -84.17953  31.4404 -84.14215  31.437873840332 -84.1431427001953  31.437593460083 -84.143180847168  31.4372596740723 -84.1433715820313  31.4366703033447 -84.1434936523438  31.4360046386719 -84.1433181762695  31.434663772583 -84.1425704956055  31.4344615936279 -84.1424255371094  31.4341640472412 -84.1423492431641  31.4338264465332 -84.1423797607422  31.4335346221924 -84.1425552368164  31.4333992004395 -84.1427993774414

Format of Input Overlay Layer is same without the "b" in the beginning
