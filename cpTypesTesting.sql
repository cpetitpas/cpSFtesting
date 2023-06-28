use role svcintdev;
use database cpSFtesting;
//create schema cp_test_project;
use warehouse compute_wh;
use schema rai;
call rai.use_rai_engine('cpSFtest0615');
call rai.use_rai_database('cpSFtesting0615');
select get_database('cpSFtesting0615');

create or replace table cpTriangleData(x int, y int) as select * from values (1,2),(2,3),(3,1),(4,5),(5,6),(6,4),(7,8),(8,9),(9,7),(10,11),(11,12),(12,10),(13,14),(14,15),(15,13),(16,17),(17,18),(18,16),(19,20),(20,21),(21,19),(22,23),(23,24),(24,22),(25,26),(26,27),(27,25),(28,29),(29,30),(30,28),(31,32),(32,33),(33,31),(34,35),(35,36),(36,34),(37,38),(38,39),(39,37),(40,41),(41,42),(42,40),(43,44),(44,45),(45,43),(46,47),(47,48),(48,46),(49,50),(50,51),(51,49),(49,1),(1,40),(40,49);

select x, y, typeof(x), typeof(y) from cpTriangleData;

create or replace table cpDecIntTriangleData(x decimal, y decimal) as select * from values (1,2),(2,3),(3,1),(4,5),(5,6),(6,4),(7,8),(8,9),(9,7),(10,11),(11,12),(12,10),(13,14),(14,15),(15,13),(16,17),(17,18),(18,16),(19,20),(20,21),(21,19),(22,23),(23,24),(24,22),(25,26),(26,27),(27,25),(28,29),(29,30),(30,28),(31,32),(32,33),(33,31),(34,35),(35,36),(36,34),(37,38),(38,39),(39,37),(40,41),(41,42),(42,40),(43,44),(44,45),(45,43),(46,47),(47,48),(48,46),(49,50),(50,51),(51,49),(49,1),(1,40),(40,49);

select x, y, typeof(x), typeof(y) from cpDecIntTriangleData;

create or replace table cpNumIntTriangleData(x number, y number) as select * from values (1,2),(2,3),(3,1),(4,5),(5,6),(6,4),(7,8),(8,9),(9,7),(10,11),(11,12),(12,10),(13,14),(14,15),(15,13),(16,17),(17,18),(18,16),(19,20),(20,21),(21,19),(22,23),(23,24),(24,22),(25,26),(26,27),(27,25),(28,29),(29,30),(30,28),(31,32),(32,33),(33,31),(34,35),(35,36),(36,34),(37,38),(38,39),(39,37),(40,41),(41,42),(42,40),(43,44),(44,45),(45,43),(46,47),(47,48),(48,46),(49,50),(50,51),(51,49),(49,1),(1,40),(40,49);

select x, y, typeof(x), typeof(y) from cpNumIntTriangleData;

create or replace table cpNumFltTriangleData(x number(38, 2), y number(38, 2)) as select * from values (1.5,2.5),(2.5,3.5),(3.5,1.5),(4.5,5.5),(5.5,6.5),(6.5,4.5),(7.5,8.5),(8.5,9.5),(9.5,7.5),(10.5,11.5),(11.5,12.5),(12.5,10.5),(13.5,14.5),(14.5,15.5),(15.5,13.5),(16.5,17.5),(17.5,18.5),(18.5,16.5),(19.5,20.5),(20.5,21.5),(21.5,19.5),(22.5,23.5),(23.5,24.5),(24.5,22.5),(25.5,26.5),(26.5,27.5),(27.5,25.5),(28.5,29.5),(29.5,30.5),(30.5,28.5),(31.5,32.5),(32.5,33.5),(33.5,31.5),(34.5,35.5),(35.5,36.5),(36.5,34.5),(37.5,38.5),(38.5,39.5),(39.5,37.5),(40.5,41.5),(41.5,42.5),(42.5,40.5),(43.5,44.5),(44.5,45.5),(45.5,43.5),(46.5,47.5),(47.5,48.5),(48.5,46.5),(49.5,50.5),(50.5,51.5),(51.5,49.5),(49.5,1.5),(1.5,40.5),(40.5,49.5);

select x, y, typeof(x), typeof(y) from cpNumFltTriangleData;

create or replace table cpDecFltTriangleData(x decimal(38, 2), y decimal(38, 2)) as select * from values (1.5,2.5),(2.5,3.5),(3.5,1.5),(4.5,5.5),(5.5,6.5),(6.5,4.5),(7.5,8.5),(8.5,9.5),(9.5,7.5),(10.5,11.5),(11.5,12.5),(12.5,10.5),(13.5,14.5),(14.5,15.5),(15.5,13.5),(16.5,17.5),(17.5,18.5),(18.5,16.5),(19.5,20.5),(20.5,21.5),(21.5,19.5),(22.5,23.5),(23.5,24.5),(24.5,22.5),(25.5,26.5),(26.5,27.5),(27.5,25.5),(28.5,29.5),(29.5,30.5),(30.5,28.5),(31.5,32.5),(32.5,33.5),(33.5,31.5),(34.5,35.5),(35.5,36.5),(36.5,34.5),(37.5,38.5),(38.5,39.5),(39.5,37.5),(40.5,41.5),(41.5,42.5),(42.5,40.5),(43.5,44.5),(44.5,45.5),(45.5,43.5),(46.5,47.5),(47.5,48.5),(48.5,46.5),(49.5,50.5),(50.5,51.5),(51.5,49.5),(49.5,1.5),(1.5,40.5),(40.5,49.5);

select x, y, typeof(x), typeof(y) from cpDecFltTriangleData;

create or replace table parentPetitpas(parent string, child string) as select * from values ('Ron','Chris'),('Ron','Lisa'),('Ron','David'),('Ron','Michael'),('Vivian','Chris'),('Vivian','Lisa'),('Vivian','David'),('Vivian','Michael'),('Chris','Alex'),('Chris','Jacob'),('Chris','Chloe'),('Angela','Alex'),('Angela','Jacob'),('Angela','Chloe'),('Lisa','Steph'),('Lisa','Amanda'),('Bill','Steph'),('Bill','Amanda'),('Michael','Jaysin'),('Michael','David Jr'),('Michael','Nathan');

select * from parentPetitpas;

create or replace table cpFltTriangleData(x float, y float) as select * from values (1.5,2.5),(2.5,3.5),(3.5,1.5),(4.5,5.5),(5.5,6.5),(6.5,4.5),(7.5,8.5),(8.5,9.5),(9.5,7.5),(10.5,11.5),(11.5,12.5),(12.5,10.5),(13.5,14.5),(14.5,15.5),(15.5,13.5),(16.5,17.5),(17.5,18.5),(18.5,16.5),(19.5,20.5),(20.5,21.5),(21.5,19.5),(22.5,23.5),(23.5,24.5),(24.5,22.5),(25.5,26.5),(26.5,27.5),(27.5,25.5),(28.5,29.5),(29.5,30.5),(30.5,28.5),(31.5,32.5),(32.5,33.5),(33.5,31.5),(34.5,35.5),(35.5,36.5),(36.5,34.5),(37.5,38.5),(38.5,39.5),(39.5,37.5),(40.5,41.5),(41.5,42.5),(42.5,40.5),(43.5,44.5),(44.5,45.5),(45.5,43.5),(46.5,47.5),(47.5,48.5),(48.5,46.5),(49.5,50.5),(50.5,51.5),(51.5,49.5),(49.5,1.5),(1.5,40.5),(40.5,49.5);

select x, y, typeof(x), typeof(y) from cpFltTriangleData;

select * from cpFltTriangleData;

create or replace table cpDateData(x date) as select * from values ('06/01/2023'),('06/02/2023'),('06/03/2023'),('06/04/2023'),('06/05/2023'),('06/06/2023'),('06/07/2023'),('06/08/2023'),('06/09/2023'),('06/10/2023'),('06/11/2023'),('06/12/2023'),('06/13/2023'),('06/14/2023'),('06/15/2023'),('06/16/2023'),('06/17/2023'),('06/18/2023'),('06/19/2023'),('06/20/2023'),('06/21/2023'),('06/22/2023'),('06/23/2023'),('06/24/2023'),('06/25/2023'),('06/26/2023'),('06/27/2023'),('06/28/2023'),('06/29/2023'),('06/30/2023'),('07/01/2023');

select * from cpDateData;

create or replace table cpTimeStampData(x timestamp);
insert into cpTimeStampData values ('06/01/2023 12:00:00'),('06/02/2023 12:00:00'),('06/03/2023 12:00:00'),('06/04/2023 12:00:00'),('06/05/2023 12:00:00'),('06/06/2023 12:00:00'),('06/07/2023 12:00:00'),('06/08/2023 12:00:00'),('06/09/2023 12:00:00'),('06/10/2023 12:00:00'),('06/11/2023 12:00:00'),('06/12/2023 12:00:00'),('06/13/2023 12:00:00'),('06/14/2023 12:00:00'),('06/15/2023 12:00:00'),('06/16/2023 12:00:00'),('06/17/2023 12:00:00'),('06/18/2023 12:00:00'),('06/19/2023 12:00:00'),('06/20/2023 12:00:00'),('06/21/2023 12:00:00'),('06/22/2023 12:00:00'),('06/23/2023 12:00:00'),('06/24/2023 12:00:00'),('06/25/2023 12:00:00'),('06/26/2023 12:00:00'),('06/27/2023 12:00:00'),('06/28/2023 12:00:00'),('06/29/2023 12:00:00'),('06/30/2023 12:00:00'),('07/01/2023 12:00:00');

select * from cpTimeStampData;

create or replace table cpMixedDataTypes(x timestamp, y date, z float, a decimal(38, 2), b number(38, 2), c int, d string, e boolean) as select * from values ('06/01/23 12:00:00','06/01/23',3.1415926,3.14,3.22,2,'blah1', true),('06/02/23 12:00:00','06/02/23',4.1415926,4.14,4.22,3,'blah2', TRUE),('06/03/23 12:00:00','06/03/23',5.1415926,5.14,5.22,4,'blah3', false),('06/04/23 12:00:00','06/04/23',6.1415926,6.14,6.22,5,'blah4', FALSE),('06/05/23 12:00:00','06/05/23',7.1415926,7.14,7.22,6,'blah5', true);

select * from cpMixedDataTypes;

select x, y, z, a, b, c, c, typeof(z), typeof(a), typeof(b), typeof(c) from cpMixedDataTypes;

create or replace table cpBoolean(x boolean, y string) as select * from values (true,'true'),(TRUE,'TRUE'),(false,'false'),(FALSE,'FALSE');

select * from cpBoolean;

create or replace table cpBinary(x string, y binary) as select * from values ('This is a test binary string', to_binary('This is a test binary string', 'utf-8'
));

select * from cpBinary;

select *
FROM Table(common.relation2table('cpSF-testing-0ebf', 'cpSF-S16false-0ebfee39359baed9a514', 'def output = cpTriangleData'));

select *
FROM Table(common.relation2table('cpSF-testing-0ebf', 'cpSF-S16false-0ebfee39359baed9a514', 'def output = parentPetitpas'));

select *
FROM Table(common.relation2table('cpSF-testing-0ebf', 'cpSF-S16false-0ebfee39359baed9a514', 'def output = cpFltTriangleData'));

select *
FROM Table(common.relation2table('cpSF-testing-0ebf', 'cpSF-S16false-0ebfee39359baed9a514', 'def output = cpDateData'));

select *
FROM Table(common.relation2table('cpSF-testing-0ebf', 'cpSF-S16false-0ebfee39359baed9a514', 'def output = cpTimeStampData'));

select * FROM Table(common.relgraphlib(
    'cpSF-S16false-0ebfee39359baed9a514', 'cpSF-testing-0ebf', 'cpTriangleData',
    'X', 'Y', 'unique_triangle'));

select * FROM Table(common.relgraphlib(
    'cpSF-S16false-0ebfee39359baed9a514', 'cpSF-testing-0ebf', 'parentPetitpas',
    'PARENT', 'CHILD', 'degree'));

select * FROM Table(common.relgraphlib(
    'cpSF-S16false-0ebfee39359baed9a514', 'cpSF-testing-0ebf', 'cpFltTriangleData',
    'X', 'Y', 'triangle_distribution'));


call common.create_data_stream('hackathon23.cp_test_project.cpTriangleData', 'table', 'cpSF-testing-0ebf', 'cpTriangleData');
call common.create_data_stream('hackathon23.cp_test_project.parentPetitpas', 'table', 'cpSF-testing-0ebf', 'parentPetitpas');
call common.create_data_stream('hackathon23.cp_test_project.cpNumIntTriangleData', 'table', 'cpSF-testing-0ebf', 'cpNumIntTriangleData');
call common.create_data_stream('hackathon23.cp_test_project.cpNumFltTriangleData', 'table', 'cpSF-testing-0ebf', 'cpNumFltTriangleData');
call common.create_data_stream('hackathon23.cp_test_project.cpDecIntTriangleData', 'table', 'cpSF-testing-0ebf', 'cpDecIntTriangleData');
call common.create_data_stream('hackathon23.cp_test_project.cpDecFltTriangleData', 'table', 'cpSF-testing-0ebf', 'cpDecFltTriangleData');
call common.create_data_stream('hackathon23.cp_test_project.cpFltTriangleData', 'table', 'cpSF-testing-0ebf', 'cpFltTriangleData');
call common.create_data_stream('hackathon23.cp_test_project.cpDateData', 'table', 'cpSF-testing-0ebf', 'cpDateData');
call common.create_data_stream('hackathon23.cp_test_project.cpTimeStampData', 'table', 'cpSF-testing-0ebf', 'cpTimeStampData');
call common.create_data_stream('hackathon23.cp_test_project.cpMixedDataTypes', 'table', 'cpSF-testing-0ebf', 'cpMixedDataTypes');
call common.create_data_stream('hackathon23.cp_test_project.cpBoolean', 'table', 'cpSF-testing-0ebf', 'cpBoolean');
call common.create_data_stream('hackathon23.cp_test_project.cpBinary', 'table', 'cpSF-testing-0ebf', 'cpBinary');

call common.delete_data_stream('hackathon23.cp_test_project.cpTriangleData');
call common.delete_data_stream('hackathon23.cp_test_project.parentPetitpas');
call common.delete_data_stream('hackathon23.cp_test_project.cpNumIntTriangleData');
call common.delete_data_stream('hackathon23.cp_test_project.cpNumFltTriangleData');
call common.delete_data_stream('hackathon23.cp_test_project.cpDecIntTriangleData');
call common.delete_data_stream('hackathon23.cp_test_project.cpDecFltTriangleData');
call common.delete_data_stream('hackathon23.cp_test_project.cpFltTriangleData');
call common.delete_data_stream('hackathon23.cp_test_project.cpDateData');
call common.delete_data_stream('hackathon23.cp_test_project.cpTimeStampData');
call common.delete_data_stream('hackathon23.cp_test_project.cpMixedDataTypes');
call common.delete_data_stream('hackathon23.cp_test_project.cpBoolean');
call common.delete_data_stream('hackathon23.cp_test_project.cpBinary');





