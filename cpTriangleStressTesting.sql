use role svcintdev;
use database cpSFtesting;
//create schema cp_test_project;
use warehouse compute_wh;
use schema rai;
//select rai.create_engine('cpSFtest0621', 's');
call rai.use_rai_engine('cpSFtest0622');
call rai.use_rai_database('cpSFtesting0615');
select get_database('cpSFtesting0615');

create or replace table cptriangle.cpTriangleData(x int, y int);
insert into cptriangle.cpTriangleData values (1,2),(2,3),(3,1),(4,5),(5,6),(6,4),(7,8),(8,9),(9,7),(10,11),(11,12),(12,10),(13,14),(14,15),(15,13),(16,17),(17,18),(18,16),(19,20),(20,21),(21,19),(22,23),(23,24),(24,22),(25,26),(26,27),(27,25),(28,29),(29,30),(30,28),(31,32),(32,33),(33,31),(34,35),(35,36),(36,34),(37,38),(38,39),(39,37),(40,41),(41,42),(42,40),(43,44),(44,45),(45,43),(46,47),(47,48),(48,46),(49,50),(50,51),(51,49),(49,1),(1,40),(40,49);

create or replace table cptriangle.cpTriangleData(x int, y int) as select * from values (1,2),(2,3),(3,1),(4,5),(5,6),(6,4),(7,8),(8,9),(9,7),(10,11),(11,12),(12,10),(13,14),(14,15),(15,13),(16,17),(17,18),(18,16),(19,20),(20,21),(21,19),(22,23),(23,24),(24,22),(25,26),(26,27),(27,25),(28,29),(29,30),(30,28),(31,32),(32,33),(33,31),(34,35),(35,36),(36,34),(37,38),(38,39),(39,37),(40,41),(41,42),(42,40),(43,44),(44,45),(45,43),(46,47),(47,48),(48,46),(49,50),(50,51),(51,49),(49,1),(1,40),(40,49);

create or replace stage cptriangle.cpSFpairs
    url = 'azure://raiproductanalysis.blob.core.windows.net/csvtestdata/' credentials = (azure_sas_token = '?sv=2022-11-02&ss=b&srt=co&sp=rltfx&se=2023-06-26T23:41:37Z&st=2023-06-26T15:41:37Z&spr=https&sig=KZuJYQwm1GuTF0AcjuRzFpUWThu6XTcaz9oq4qvRlts%3D');

create or replace file format cptriangle.cpSFpairs
    type = csv
    COMPRESSION = none
    skip_header = 1;

//create or replace table cpTriangleDataTest(x int, y int);
copy into cptriangle.cpTriangleData
    from @cptriangle.cpSFpairs
    pattern = 'sfIntPairs1.csv'    
    file_format = cptriangle.cpSFpairs;

copy into cptriangle.cpTriangleData
    from @cptriangle.cpSFpairs
    pattern = 'sfIntPairs2.csv'    
    file_format = cptriangle.cpSFpairs;

copy into cptriangle.cpTriangleData
    from @cptriangle.cpSFpairs
    pattern = 'sfIntPairs3.csv'    
    file_format = cptriangle.cpSFpairs;

copy into cptriangle.cpTriangleData
    from @cptriangle.cpSFpairs
    pattern = 'sfIntPairs4.csv'    
    file_format = cptriangle.cpSFpairs;

copy into cptriangle.cpTriangleData
    from @cptriangle.cpSFpairs
    pattern = 'sfIntPairs10mil.csv'    
    file_format = cptriangle.cpSFpairs;

select count (*) from cptriangle.cpTriangleData;
select * from cptriangle.cpTriangleData;

delete from cptriangle.CPTRIANGLEDATA where X > 51;
truncate cptriangle.cptriangledata;

SELECT rai.num_nodes('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleNumNodes' });
select * from cptriangle.cpTriangleNumNodes;

SELECT * from table(rai.unique_triangle('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTriangleUniqueTriangle' }));
select * from cptriangle.cptriangleuniquetriangle;

select * from table(rai.pagerank('cpTriangleDataGraph', { 'result_table': 'cpsftesting.cptriangle.cpTrianglePagerank' }));
select * from cptriangle.cpTrianglePagerank;

call rai.delete_graph('cpTriangleDataGraph');
call rai.create_graph('cpTriangleDataGraph', 'cptriangle.cpTriangleData');

call rai.delete_data_stream('cptriangle.cpTriangleData');
call rai.create_data_stream('cptriangle.cpTriangleData');
select rai.get_data_stream('cpsftesting.cptriangle.cpTriangleData');
select rai.get_data_stream_status('cpsftesting.cptriangle.cpTriangleData');

select rai.exec('cpSFtesting0615', 'cpSFtest0615', 'def delete[:cpTriangleData] = cpTriangleData', {}, false);

select * from information_schema.load_history;
delete from information_schema.load_history where file_name = 'azure://raiproductanalysis.blob.core.windows.net/csvtestdata/sfIntPairs*';
