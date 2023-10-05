-- **********start with empty table/relation**********

truncate cptriangle.cptriangledata;
select rai.exec($raiDatabase, $raiEngine, 'def delete[:cpTriangleDataView] = cpTriangleDataView', {}, false);

create or replace table cpsftesting.cptriangle.cpTriangleData(x int, y int) as select * from values (1,2),(2,3),(3,1),(4,5),(5,6),(6,4),(7,8),(8,9),(9,7),(10,11),(11,12),(12,10),(13,14),(14,15),(15,13),(16,17),(17,18),(18,16),(19,20),(20,21),(21,19),(22,23),(23,24),(24,22),(25,26),(26,27),(27,25),(28,29),(29,30),(30,28),(31,32),(32,33),(33,31),(34,35),(35,36),(36,34),(37,38),(38,39),(39,37),(40,41),(41,42),(42,40),(43,44),(44,45),(45,43),(46,47),(47,48),(48,46),(49,50),(50,51),(51,49),(49,1),(1,40),(40,49);

create or replace view cpsftesting.cptriangle.cpTriangleDataView as select x, y from cpsftesting.cptriangle.cpTriangleData;

select count (x, y) from cpsftesting.cptriangle.cpTriangleDataView;

call rai.delete_data_stream('cpsftesting.cptriangle.cpTriangleDataView');

call rai.delete_graph('cpTriangleDataGraph');

call rai.create_data_stream('cpsftesting.cpTriangle.cpTriangleDataView');

call rai.get_data_stream_status('cpsftesting.cptriangle.cpTriangleDataView');

-- wait for data_stream to sync
CALL SYSTEM$WAIT(120);

call rai.get_data_stream_status('cpsftesting.cptriangle.cpTriangleDataView');

call rai.create_graph('cpTriangleDataGraph', 'cpsftesting.cptriangle.cpTriangleDataView');

// all graphlib functions with result_table

SELECT rai.num_nodes('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleNumNodes' });
SELECT rai.num_edges('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleNumEdges' });
SELECT * from table(rai.adamic_adar('cpTriangleDataGraph', {'node1': [50, 1] , 'result_table': 'cpsftesting.cptriangle.cpTriangleAdemicAdar' }));
SELECT * from table(rai.degree('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleDegree' }));
SELECT * from table(rai.degree_histogram('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleDegreeHistogram' }));
SELECT * from table(rai.common_neighbor('cpTriangleDataGraph', { 'node1': [40], 'result_table': 'cpsftesting.cptriangle.cpTriangleCommonNeighbor' }));
SELECT * from table(rai.cosine_similarity('cpTriangleDataGraph', { 'node1': [1, 40], 'result_table': 'cpsftesting.cptriangle.cpTriangleCosineSimilarity'}));
SELECT rai.average_degree('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleAverageDegree' });
SELECT * from table(rai.degree_centrality('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleDegreeCentrality' }));
SELECT rai.diameter_range('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleDiameterRange' });
SELECT * from table(rai.eigenvector_centrality('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleEigenvectorCentrality' }));
SELECT rai.is_connected('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleIsConnected' });
SELECT rai.is_graph_created('cpTriangleDataGraph');
SELECT * from table(rai.jaccard_similarity('cpTriangleDataGraph', { 'node1': [40, 50], 'result_table': 'cpsftesting.cptriangle.cpTriangleJaccardSimiliary' }));
SELECT * from table(rai.local_clustering_coefficient('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleLocalClusteringCoefficient' }));
SELECT rai.max_degree('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleMaxDegree' });
SELECT rai.min_degree('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleMinDegree' });
SELECT * from table(rai.neighbor('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleNeighbor' }));
SELECT * from table(rai.neighbor('cpTriangleDataGraph', {'tuples': [[40, 41]], 'result_table': 'cpsftesting.cptriangle.cpTriangleNeighbor2' }));
SELECT rai.node('cpTriangleDataGraph', [4, 7, 9, 2, 1]);
select * from table(rai.pagerank('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTrianglePagerank' }));
SELECT * from table(rai.preferential_attachment('cpTriangleDataGraph', {'node1': [40], 'result_table': 'cpsftesting.cptriangle.cpTrianglePreferentialAttachment' }));
SELECT * from table(rai.shortest_path_length('cpTriangleDataGraph', {'source': 40, 'result_table': 'cpsftesting.cptriangle.cpTriangleShortestPathLength' }));
SELECT * from table(rai.shortest_path_length('cpTriangleDataGraph', {'source': 1, 'target': 3, 'result_table': 'cpsftesting.cptriangle.cpTriangleShortestPathLength2' }));
SELECT * from table(rai.shortest_path_length('cpTriangleDataGraph', {'tuples': [[1], [10, 11], [2, 4]], 'result_table': 'cpsftesting.cptriangle.cpTriangleShortestPathLength3' }));
SELECT * from table(rai.transitive_closure('cpTriangleDataGraph', { 'source': 40, 'result_table': 'cpsftesting.cptriangle.cpTriangleTransitiveClosure' }));
SELECT * from table(rai.triangle_count('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleCount' }));
SELECT * from table(rai.unique_triangle('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleUniqueTriangle' }));
SELECT * from table(rai.weakly_connected_component('cpTriangleDataGraph', { 'node': 5, 'result_table': 'cpsftesting.cptriangle.cpTriangleWeaklyConnectedComponent' }));

select * from cpsftesting.cptriangle.cpTriangleNumNodes;
select * from cpsftesting.cptriangle.cpTriangleNumEdges;
select * from cpsftesting.cptriangle.cpTriangleAdemicAdar;
select * from cpsftesting.cptriangle.cpTriangleDegree;
select * from cpsftesting.cptriangle.cpTriangleCommonNeighbor;
select * from cpsftesting.cptriangle.CPTRIANGLECOSINESIMILARITY;
select * from cpsftesting.cptriangle.CPTRIANGLEAVERAGEDEGREE;
select * from cpsftesting.cptriangle.CPTRIANGLEDEGREEHISTOGRAM;
select * from cpsftesting.cptriangle.CPTRIANGLEDEGREECENTRALITY;
select * from cpsftesting.cptriangle.CPTRIANGLEDIAMETERRANGE;
select * from cpsftesting.cptriangle.CPTRIANGLEEIGENVECTORCENTRALITY;
select * from cpsftesting.cptriangle.cptriangleisconnected;
select * from cpsftesting.cptriangle.CPTRIANGLEJACCARDSIMILIARY;
select * from cpsftesting.cptriangle.CPTRIANGLELOCALCLUSTERINGCOEFFICIENT;
select * from cpsftesting.cptriangle.CPTRIANGLEMAXDEGREE;
select * from cpsftesting.cptriangle.CPTRIANGLEMINDEGREE;
select * from cpsftesting.cptriangle.CPTRIANGLENEIGHBOR;
select * from cpsftesting.cptriangle.CPTRIANGLENEIGHBOR2;
select * from cpsftesting.cptriangle.CPTRIANGLEPAGERANK;
select * from cpsftesting.cptriangle.CPTRIANGLEPREFERENTIALATTACHMENT;
select * from cpsftesting.cptriangle.CPTRIANGLESHORTESTPATHLENGTH;
select * from cpsftesting.cptriangle.CPTRIANGLESHORTESTPATHLENGTH2;
select * from cpsftesting.cptriangle.CPTRIANGLESHORTESTPATHLENGTH3;
select * from cpsftesting.cptriangle.CPTRIANGLETRANSITIVECLOSURE;
select * from cpsftesting.cptriangle.CPTRIANGLECOUNT;
select * from cpsftesting.cptriangle.CPTRIANGLEUNIQUETRIANGLE;
select * from cpsftesting.cptriangle.CPTRIANGLEWEAKLYCONNECTEDCOMPONENT;

-- **********wipe out and populate with smaller set of pairs**********

truncate cptriangle.cptriangledata;
select rai.exec($raiDatabase, $raiEngine, 'def delete[:cpTriangleDataView] = cpTriangleDataView', {}, false);
insert into cpsftesting.cptriangle.cpTriangleData values (1,2),(2,3),(3,1),(4,5),(5,6),(6,4),(7,8),(8,9),(9,7),(10,11),(11,12),(12,10);

-- wait for data_stream to sync
CALL SYSTEM$WAIT(120);

call rai.get_data_stream_status('cpsftesting.cptriangle.cpTriangleDataView');

-- run graphlib functions again
SELECT rai.num_nodes('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleNumNodes' });
SELECT rai.num_edges('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleNumEdges' });
SELECT * from table(rai.adamic_adar('cpTriangleDataGraph', {'node1': [50, 1] , 'result_table': 'cpsftesting.cptriangle.cpTriangleAdemicAdar' }));
SELECT * from table(rai.degree('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleDegree' }));
SELECT * from table(rai.degree_histogram('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleDegreeHistogram' }));
SELECT * from table(rai.common_neighbor('cpTriangleDataGraph', { 'node1': [40], 'result_table': 'cpsftesting.cptriangle.cpTriangleCommonNeighbor' }));
SELECT * from table(rai.cosine_similarity('cpTriangleDataGraph', { 'node1': [1, 40], 'result_table': 'cpsftesting.cptriangle.cpTriangleCosineSimilarity'}));
SELECT rai.average_degree('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleAverageDegree' });
SELECT * from table(rai.degree_centrality('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleDegreeCentrality' }));
SELECT rai.diameter_range('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleDiameterRange' });
SELECT * from table(rai.eigenvector_centrality('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleEigenvectorCentrality' }));
SELECT rai.is_connected('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleIsConnected' });
SELECT rai.is_graph_created('cpTriangleDataGraph');
SELECT * from table(rai.jaccard_similarity('cpTriangleDataGraph', { 'node1': [40, 50], 'result_table': 'cpsftesting.cptriangle.cpTriangleJaccardSimiliary' }));
SELECT * from table(rai.local_clustering_coefficient('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleLocalClusteringCoefficient' }));
SELECT rai.max_degree('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleMaxDegree' });
SELECT rai.min_degree('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleMinDegree' });
SELECT * from table(rai.neighbor('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleNeighbor' }));
SELECT * from table(rai.neighbor('cpTriangleDataGraph', {'tuples': [[40, 41]], 'result_table': 'cpsftesting.cptriangle.cpTriangleNeighbor2' }));
SELECT rai.node('cpTriangleDataGraph', [4, 7, 9, 2, 1]);
select * from table(rai.pagerank('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTrianglePagerank' }));
SELECT * from table(rai.preferential_attachment('cpTriangleDataGraph', {'node1': [40], 'result_table': 'cpsftesting.cptriangle.cpTrianglePreferentialAttachment' }));
SELECT * from table(rai.shortest_path_length('cpTriangleDataGraph', {'source': 40, 'result_table': 'cpsftesting.cptriangle.cpTriangleShortestPathLength' }));
SELECT * from table(rai.shortest_path_length('cpTriangleDataGraph', {'source': 1, 'target': 3, 'result_table': 'cpsftesting.cptriangle.cpTriangleShortestPathLength2' }));
SELECT * from table(rai.shortest_path_length('cpTriangleDataGraph', {'tuples': [[1], [10, 11], [2, 4]], 'result_table': 'cpsftesting.cptriangle.cpTriangleShortestPathLength3' }));
SELECT * from table(rai.transitive_closure('cpTriangleDataGraph', { 'source': 40, 'result_table': 'cpsftesting.cptriangle.cpTriangleTransitiveClosure' }));
SELECT * from table(rai.triangle_count('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleCount' }));
SELECT * from table(rai.unique_triangle('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleUniqueTriangle' }));
SELECT * from table(rai.weakly_connected_component('cpTriangleDataGraph', { 'node': 5, 'result_table': 'cpsftesting.cptriangle.cpTriangleWeaklyConnectedComponent' }));

select * from cpsftesting.cptriangle.cpTriangleNumNodes;
select * from cpsftesting.cptriangle.cpTriangleNumEdges;
select * from cpsftesting.cptriangle.cpTriangleAdemicAdar;
select * from cpsftesting.cptriangle.cpTriangleDegree;
select * from cpsftesting.cptriangle.cpTriangleCommonNeighbor;
select * from cpsftesting.cptriangle.CPTRIANGLECOSINESIMILARITY;
select * from cpsftesting.cptriangle.CPTRIANGLEAVERAGEDEGREE;
select * from cpsftesting.cptriangle.CPTRIANGLEDEGREEHISTOGRAM;
select * from cpsftesting.cptriangle.CPTRIANGLEDEGREECENTRALITY;
select * from cpsftesting.cptriangle.CPTRIANGLEDIAMETERRANGE;
select * from cpsftesting.cptriangle.CPTRIANGLEEIGENVECTORCENTRALITY;
select * from cpsftesting.cptriangle.cptriangleisconnected;
select * from cpsftesting.cptriangle.CPTRIANGLEJACCARDSIMILIARY;
select * from cpsftesting.cptriangle.CPTRIANGLELOCALCLUSTERINGCOEFFICIENT;
select * from cpsftesting.cptriangle.CPTRIANGLEMAXDEGREE;
select * from cpsftesting.cptriangle.CPTRIANGLEMINDEGREE;
select * from cpsftesting.cptriangle.CPTRIANGLENEIGHBOR;
select * from cpsftesting.cptriangle.CPTRIANGLENEIGHBOR2;
select * from cpsftesting.cptriangle.CPTRIANGLEPAGERANK;
select * from cpsftesting.cptriangle.CPTRIANGLEPREFERENTIALATTACHMENT;
select * from cpsftesting.cptriangle.CPTRIANGLESHORTESTPATHLENGTH;
select * from cpsftesting.cptriangle.CPTRIANGLESHORTESTPATHLENGTH2;
select * from cpsftesting.cptriangle.CPTRIANGLESHORTESTPATHLENGTH3;
select * from cpsftesting.cptriangle.CPTRIANGLETRANSITIVECLOSURE;
select * from cpsftesting.cptriangle.CPTRIANGLECOUNT;
select * from cpsftesting.cptriangle.CPTRIANGLEUNIQUETRIANGLE;
select * from cpsftesting.cptriangle.CPTRIANGLEWEAKLYCONNECTEDCOMPONENT;