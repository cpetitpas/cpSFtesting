use role svcintdev;
use database cpSFtesting;
//create schema cp_test_project;
use warehouse compute_wh;
use schema rai;
call rai.use_rai_engine('cpSFtest0615');
call rai.use_rai_database('cpSFtesting0615');
select get_database('cpSFtesting0615');

//select rai.create_engine ('cpSFtest0606', 's');

// type = variant
create or replace table cpVariantTest(x variant, y variant) as select * from values (1,2),(2,3),(3,1),(4,5),(5,6),(6,4),(7,8),(8,9),(9,7),(10,11),(11,12),(12,10),(13,14),(14,15),(15,13),(16,17),(17,18),(18,16),(19,20),(20,21),(21,19),(22,23),(23,24),(24,22),(25,26),(26,27),(27,25),(28,29),(29,30),(30,28),(31,32),(32,33),(33,31),(34,35),(35,36),(36,34),(37,38),(38,39),(39,37),(40,41),(41,42),(42,40),(43,44),(44,45),(45,43),(46,47),(47,48),(48,46),(49,50),(50,51),(51,49),(49,1),(1,40),(40,49);

select * from cpvarianttest;
select x, y, typeof(x), typeof(y) from cpvarianttest;

call rai.create_data_stream('cpVariantTest');
call rai.delete_data_stream('cpVariantTest');

select rai.get_data_stream('cpsftesting.rai.cpVariantTest');

// alter variant to int
alter table cpVariantTest2 alter x set data type number;

// type = variant
create or replace table cpVariantTest2(x int, y int) as select * from values (1,2),(2,3),(3,1),(4,5),(5,6),(6,4),(7,8),(8,9),(9,7),(10,11),(11,12),(12,10),(13,14),(14,15),(15,13),(16,17),(17,18),(18,16),(19,20),(20,21),(21,19),(22,23),(23,24),(24,22),(25,26),(26,27),(27,25),(28,29),(29,30),(30,28),(31,32),(32,33),(33,31),(34,35),(35,36),(36,34),(37,38),(38,39),(39,37),(40,41),(41,42),(42,40),(43,44),(44,45),(45,43),(46,47),(47,48),(48,46),(49,50),(50,51),(51,49),(49,1),(1,40),(40,49);

select * from cpvarianttest2;
select x, y, typeof(x), typeof(y) from cpvarianttest2;

call rai.create_data_stream('cpVariantTest2');
call rai.delete_data_stream('cpVariantTest2');

select rai.get_data_stream('cpsftesting.rai.cpVariantTest2');

// type = object
create or replace table person(sirname object, givenname object);
insert into person SELECT { 'sirname': 'Petitpas' } ,{ 'givenname': 'Ron' };
insert into person select { 'sirname': 'Petitpas' } ,{ 'givenname': 'Vivian' };
insert into person select { 'sirname': 'Petitpas' } ,{ 'givenname': 'David' };
insert into person select { 'sirname': 'Petitpas' } ,{ 'givenname': 'Michael' };
insert into person select { 'sirname': 'Petitpas' } ,{ 'givenname': 'Lisa' };
insert into person select { 'sirname': 'Petitpas' } ,{ 'givenname': 'Christopher' };

select sirname, givenname, typeof(sirname), typeof(givenname) from person;
//select * from person;

call rai.create_data_stream('person', 'cpsftesting0606');

call rai.delete_data_stream('rai.person');

select rai.get_data_stream_status('rai.person');

// type = array
create or replace table personArray(sirname array, givenname array);
insert into personArray SELECT [ 'Petitpas' ], [ 'Ron' ];
insert into personArray SELECT [ 'Petitpas' ], [ 'Vivian' ];
insert into personArray SELECT [ 'Petitpas' ], [ 'David' ];
insert into personArray SELECT [ 'Petitpas' ], [ 'Michael' ];
insert into personArray SELECT [ 'Petitpas' ], [ 'Lisa' ];
insert into personArray SELECT [ 'Petitpas' ], [ 'Chris' ];

select sirname, givenname, typeof(sirname), typeof(givenname) from personarray;
//select * from personarray;

call rai.create_data_stream('personArray');

call rai.delete_data_stream('personArray');

select rai.get_data_stream('cpsftesting.rai.personArray');

// type = geography
create or replace table montanaLocations(location geography);
insert into montanaLocations SELECT ('point(-116.015896 48.473413)');
insert into montanaLocations SELECT ('point(-116.015896 48.473414)');
insert into montanaLocations SELECT ('point(-116.015896 48.473415)');
insert into montanaLocations SELECT ('point(-116.015896 48.473416)');
insert into montanaLocations SELECT ('point(-116.015896 48.473417)');
insert into montanaLocations SELECT ('point(-116.015896 48.473418)');
insert into montanaLocations SELECT ('point(-116.015896 48.473419)');
insert into montanaLocations SELECT ('point(-116.015896 48.473420)');
insert into montanaLocations SELECT ('point(-116.015896 48.473421)');
insert into montanaLocations SELECT ('point(-116.015896 48.473422)');

select * from montanalocations;

call rai.create_data_stream('montanaLocations');

call rai.delete_data_stream('montanaLocations');

select rai.get_data_stream('cpsftesting.rai.montanaLocations');

// type = geometry
create or replace table cpGeometryTest(point geometry);
insert into cpGeometryTest SELECT ('point(1820.12 890.50)');
insert into cpGeometryTest SELECT ('point(1820.12 890.60)');
insert into cpGeometryTest SELECT ('point(1820.12 890.70)');
insert into cpGeometryTest SELECT ('point(1820.12 890.80)');
insert into cpGeometryTest SELECT ('point(1820.12 890.90)');
insert into cpGeometryTest SELECT ('point(1820.12 891.00)');
insert into cpGeometryTest SELECT ('point(1820.12 891.10)');
insert into cpGeometryTest SELECT ('point(1820.12 891.20)');
insert into cpGeometryTest SELECT ('point(1820.12 891.30)');
insert into cpGeometryTest SELECT ('point(1820.12 891.40)');

select * from cpGeometryTest;

call rai.create_data_stream('cpGeometryTest');

call rai.delete_data_stream('cpGeometryTest');

select rai.get_data_stream('cpsftesting.rai.cpGeometryTest');


// stream testing

create or replace table cpStreamTest(x int, y int) as select * from values (1,2),(2,3);

call rai.create_data_stream('cpStreamTest', 'cpsftesting0606');

call rai.delete_data_stream('rai.cpstreamtest');


select rai.get_data_stream_status('rai.cpvarianttest');


select list_data_streams();


// type change testing

// string to varchar
create or replace table parentPetitpas(parent string, child string) as select * from values ('Ron','Chris'),('Ron','Lisa'),('Ron','David'),('Ron','Michael'),('Vivian','Chris'),('Vivian','Lisa'),('Vivian','David'),('Vivian','Michael'),('Chris','Alex'),('Chris','Jacob'),('Chris','Chloe'),('Angela','Alex'),('Angela','Jacob'),('Angela','Chloe'),('Lisa','Steph'),('Lisa','Amanda'),('Bill','Steph'),('Bill','Amanda'),('Michael','Jaysin'),('Michael','David Jr'),('Michael','Nathan');

call rai.create_data_stream('parentPetitpas');

call rai.delete_data_stream('parentPetitpas');

select rai.get_data_stream('cpsftesting.rai.parentPetitpas');

alter table parentpetitpas alter parent set data type varchar;
insert into parentpetitpas (parent, child) values ('Chris', 'Joe');

select * from parentpetitpas;

// number to float
create or replace table cpNumFltTriangleData(x number(38, 2), y number(38, 2)) as select * from values (1.5,2.5),(2.5,3.5),(3.5,1.5),(4.5,5.5),(5.5,6.5),(6.5,4.5),(7.5,8.5),(8.5,9.5),(9.5,7.5),(10.5,11.5),(11.5,12.5),(12.5,10.5),(13.5,14.5),(14.5,15.5),(15.5,13.5),(16.5,17.5),(17.5,18.5),(18.5,16.5),(19.5,20.5),(20.5,21.5),(21.5,19.5),(22.5,23.5),(23.5,24.5),(24.5,22.5),(25.5,26.5),(26.5,27.5),(27.5,25.5),(28.5,29.5),(29.5,30.5),(30.5,28.5),(31.5,32.5),(32.5,33.5),(33.5,31.5),(34.5,35.5),(35.5,36.5),(36.5,34.5),(37.5,38.5),(38.5,39.5),(39.5,37.5),(40.5,41.5),(41.5,42.5),(42.5,40.5),(43.5,44.5),(44.5,45.5),(45.5,43.5),(46.5,47.5),(47.5,48.5),(48.5,46.5),(49.5,50.5),(50.5,51.5),(51.5,49.5),(49.5,1.5),(1.5,40.5),(40.5,49.5);

select x, y, typeof(x), typeof(y) from cpNumFltTriangleData;

call rai.create_data_stream('cpNumFltTriangleData');

call rai.delete_data_stream('cpNumFltTriangleData');

select rai.get_data_stream('cpsftesting.rai.cpNumFltTriangleData');

alter table cpNumFltTriangleData alter x set data type decimal(38, 2);
alter table cpNumFltTriangleData alter y set data type decimal(38, 2);
insert into cpNumFltTriangleData (x, y) values (100.5, 101.5);
delete from cpNumFltTriangleData where x = 100.5;
insert into cpNumFltTriangleData (x, y) values (200.5, 201.5);
