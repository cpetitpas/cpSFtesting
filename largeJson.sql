use role svcintdev;
use database cpSFtesting;
//create schema cp_test_project;
use warehouse compute_wh;
use schema rai;
//select rai.create_engine('cpSFtest0622', 'S');
call rai.use_rai_engine('cpSFtest0622');
call rai.use_rai_database('cpSFtesting0615');
select get_database('cpSFtesting0615');

//select rai.create_engine ('cpSFtest0606', 's');

//create or replace schema test;

create or replace stage largejson.cplargejson
    url = 'azure://raiproductanalysis.blob.core.windows.net/jsondata/' credentials = (azure_sas_token = '?sv=2022-11-02&ss=b&srt=co&sp=rltfx&se=2023-06-26T23:41:37Z&st=2023-06-26T15:41:37Z&spr=https&sig=KZuJYQwm1GuTF0AcjuRzFpUWThu6XTcaz9oq4qvRlts%3D');

create or replace file format largejson.cplargejson
    type = json
    COMPRESSION = none
    STRIP_OUTER_ARRAY = true
    ALLOW_DUPLICATE = TRUE;

create or replace table largejson.largejson(x variant);
copy into largejson.largejson
    from @largejson.cplargejson
    pattern = 'large-file.json'    
    file_format = largejson.cplargejson;

select * from largejson;

create or replace table largejson.loginCreatedAt(login varchar, createdAt varchar) as
select x:actor:login, x:created_at from largejson.largejson;
select * from largejson.loginCreatedAt;

insert into largejson.logincreatedat values ('cpetitpas', '2015-01-01T15:00:23Z');
delete from largejson.logincreatedat where login = 'cpetitpas';


call create_entity('loginName', ['id']);
call create_entity('createdAtTime', ['id']);
SELECT * FROM TABLE(RAI.LIST_ENTITIES());

call create_lookup('loginName', 'largejson.loginCreatedAt', 'login');
call create_lookup('createdAtTime', 'largejson.loginCreatedAt', 'createdAt');
SELECT * FROM TABLE(RAI.LIST_LOOKUPS());

CREATE or replace table largejson.loginCreatedAtEdges(source, target) AS (
    SELECT cpsftesting.RAI.node('loginName', [login]), cpsftesting.RAI.node('createdAtTime', [createdAt])
    FROM largejson.loginCreatedAt
);
insert into largejson.logincreatedatEdges(source, target) 
    SELECT cpsftesting.RAI.node('loginName', [login]), cpsftesting.RAI.node('createdAtTime', [createdAt])
    FROM largejson.loginCreatedAt;

//drop view loginCreatedAtEdges;

select * from largejson.loginCreatedAtEdges;
select * from largejson.loginCreatedAtEdges where SOURCE = -7602151129925313869;
select * from largejson.loginCreatedAtEdges where SOURCE = 2481003140224510582;

SELECT obj FROM RAI.LOOKUP_TABLE WHERE node_id = 882582449541769078;
SELECT cpsftesting.rai.LOOKUP(882582449541769078);

select source, target, typeof(source), typeof(target) from loginCreatedAtEdges;

select * from cpsftesting.rai.LOOKUP_TABLE limit 10;
select * from cpsftesting.rai.LOOKUP_TABLE where OBJ = {'id': 'cpetitpas', 'type': 'loginName'};
select * from cpsftesting.rai.LOOKUP_TABLE where OBJ = {'id': 'guybedford', 'type': 'loginName'};


call rebuild_lookup_table();

call drop_all_entities();
call drop_all_lookups();


call rai.create_data_stream('largejson.loginCreatedAtEdges');
call rai.delete_data_stream('largejson.loginCreatedAtEdges');
select rai.get_data_stream('cpsftesting.largejson.loginCreatedAtEdges');

//call rai.create_graph('auntUncleGraph', 'auntUncle');

call rai.create_graph('loginCreatedAtEdgesGraph', 'largejson.loginCreatedAtEdges');
call rai.delete_graph('loginCreatedAtEdgesGraph');

select rai.num_edges('loginCreatedAtEdgesGraph', { 'result_table': 'cpsftesting.largejson.loginCreatedAtEdgesNumEdges' });
select * from largejson.loginCreatedAtEdgesNumEdges;

select rai.num_nodes('loginCreatedAtEdgesGraph', { 'result_table': 'cpsftesting.largejson.loginCreatedAtEdgesNumNodes' });
select * from largejson.loginCreatedAtEdgesNumNodes;

SELECT * from table(rai.degree('loginCreatedAtEdgesGraph', { 'result_table': 'cpsftesting.largejson.loginCreatedAtEdgesDegree' }));
SELECT LOOKUP(COL1):id as name, COL2 FROM largejson.loginCreatedAtEdgesDegree where name = 'guybedford'
    order by COL2 DESC;
SELECT LOOKUP(COL1):id as name, COL2 FROM largejson.loginCreatedAtEdgesDegree where name = 'cpetitpas'
    order by COL2 DESC;
