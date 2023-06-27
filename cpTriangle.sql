use role svcintdev;
use database cpSFtesting;
//create schema cp_test_project;
use warehouse compute_wh;
use schema rai;
call rai.use_rai_engine('cpSFtest0622');
call rai.use_rai_database('cpSFtesting0615');
select get_database('cpSFtesting0615');

//select rai.create_engine('cpSFtest0615', 's');
//select rai.create_database('cpSFtesting0615');

create or replace table cptriangle.cpTriangleData(x int, y int) as select * from values (1,2),(2,3),(3,1),(4,5),(5,6),(6,4),(7,8),(8,9),(9,7),(10,11),(11,12),(12,10),(13,14),(14,15),(15,13),(16,17),(17,18),(18,16),(19,20),(20,21),(21,19),(22,23),(23,24),(24,22),(25,26),(26,27),(27,25),(28,29),(29,30),(30,28),(31,32),(32,33),(33,31),(34,35),(35,36),(36,34),(37,38),(38,39),(39,37),(40,41),(41,42),(42,40),(43,44),(44,45),(45,43),(46,47),(47,48),(48,46),(49,50),(50,51),(51,49),(49,1),(1,40),(40,49);

call rai.create_data_stream('largejson.cpTriangleData');
call rai.create_graph('cpTriangleDataGraph', 'cptriangle.cpTriangleData');

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
SELECT rai.max_degree('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleMaxDegree' });
SELECT rai.min_degree('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleMinDegree' });
SELECT * from table(rai.neighbor('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleNeighbor' }));
SELECT * from table(rai.neighbor('cpTriangleDataGraph', {'node1': [40, 50, 500001], 'result_table': 'cpsftesting.cptriangle.cpTriangleNeighbor2' }));
SELECT rai.node('cpTriangleDataGraph', [4, 7, 9, 2, 1]);
select * from table(rai.pagerank('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTrianglePagerank' }));
SELECT * from table(rai.preferential_attachment('cpTriangleDataGraph', {'node1': [49], 'result_table': 'cpsftesting.cptriangle.cpTrianglePreferentialAttachment' }));
SELECT * from table(rai.shortest_path_length('cpTriangleDataGraph', {'source': 40, 'result_table': 'cpsftesting.cptriangle.cpTriangleShortestPathLength' }));
SELECT * from table(rai.shortest_path_length('cpTriangleDataGraph', {'source': 40, 'target': 1, 'result_table': 'cpsftesting.cptriangle.cpTriangleShortestPathLength2' }));
SELECT * from table(rai.transitive_closure('cpTriangleDataGraph', { 'source': 49, 'result_table': 'cpsftesting.cptriangle.cpTriangleTransitiveClosure' }));
SELECT * from table(rai.triangle_count('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleCount' }));
SELECT * from table(rai.unique_triangle('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleUniqueTriangle' }));
SELECT * from table(rai.weakly_connected_component('cpTriangleDataGraph', { 'node': 5, 'result_table': 'cpsftesting.cptriangle.cpTriangleWeaklyConnectedComponent' }));

select * from cptriangle.cpTriangleNumNodes;
select * from cptriangle.cpTriangleNumEdges;
select * from cptriangle.cpTriangleAdemicAdar;
select * from cptriangle.cpTriangleDegree;
select * from cptriangle.cpTriangleCommonNeighbor;
select * from cptriangle.CPTRIANGLECOSINESIMILARITY;
select * from cptriangle.CPTRIANGLEAVERAGEDEGREE;
select * from cptriangle.CPTRIANGLEDEGREEHISTOGRAM;
select * from cptriangle.CPTRIANGLEDEGREECENTRALITY;
select * from cptriangle.CPTRIANGLEDIAMETERRANGE;
select * from cptriangle.CPTRIANGLEEIGENVECTORCENTRALITY;
select * from cptriangle.cptriangleisconnected;
select * from cptriangle.CPTRIANGLEJACCARDSIMILIARY;
select * from cptriangle.CPTRIANGLEMAXDEGREE;
select * from cptriangle.CPTRIANGLEMINDEGREE;
select * from cptriangle.CPTRIANGLENEIGHBOR;
select * from cptriangle.CPTRIANGLENEIGHBOR2;
select * from cptriangle.CPTRIANGLEPAGERANK;
select * from cptriangle.CPTRIANGLEPREFERENTIALATTACHMENT;
select * from cptriangle.CPTRIANGLESHORTESTPATHLENGTH;
select * from cptriangle.CPTRIANGLESHORTESTPATHLENGTH2;
select * from cptriangle.CPTRIANGLETRANSITIVECLOSURE;
select * from cptriangle.CPTRIANGLECOUNT;
select * from cptriangle.CPTRIANGLEUNIQUETRIANGLE;
select * from cptriangle.CPTRIANGLEWEAKLYCONNECTEDCOMPONENT;

select rai.get_data_stream('cpsftesting.largejson.cpTriangleData');
select rai.get_data_stream_status('cpsftesting.rai.cpTriangleData');

call rai.delete_data_stream('cpsftesting.cptriangle.cpTriangleData');

call rai.delete_graph('cptriangle.cpTriangleDataGraph');

select rai.ping();

drop table cptriangledata;