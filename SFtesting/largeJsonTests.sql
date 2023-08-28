-- largeJson tests. Stage and file format not created here

create or replace table largejson.largejson(x variant);
copy into largejson.largejson
    from @largejson.cplargejson
    pattern = 'large-file.json'    
    file_format = largejson.cplargejson;

create or replace table largejson.loginCreatedAt(login varchar, createdAt timestamp) as
select x:actor:login, x:created_at from largejson.largejson;

call create_entity('loginName', 'id');
call create_entity('createdAtTime', 'id');
SELECT * FROM TABLE(RAI.LIST_ENTITIES());

call create_lookup('loginName', 'largejson.loginCreatedAt', 'login');
call create_lookup('createdAtTime', 'largejson.loginCreatedAt', 'createdAt');
SELECT * FROM TABLE(RAI.LIST_LOOKUPS());

CREATE or replace view largejson.loginCreatedAtEdgesView(src, dst) AS (
    SELECT cpsftesting.RAI.node('loginName', login::variant) as src, cpsftesting.RAI.node('createdAtTime', createdAt::variant) as dst
    FROM largejson.loginCreatedAt
);

call rai.delete_data_stream('largejson.loginCreatedAtEdgesView');
call rai.delete_graph('loginCreatedAtEdgesGraph');

call rai.create_data_stream('largejson.loginCreatedAtEdgesView');

call rai.get_data_stream_status('cpsftesting.largejson.loginCreatedAtEdgesView');

CALL SYSTEM$WAIT(120);

call rai.get_data_stream_status('cpsftesting.largejson.loginCreatedAtEdgesView');

call rai.create_graph('loginCreatedAtEdgesGraph', 'largejson.loginCreatedAtEdgesView');

select rai.num_nodes('loginCreatedAtEdgesGraph', { 'result_table': 'cpsftesting.largejson.loginCreatedAtEdgesNumNodes' });
select * from largejson.loginCreatedAtEdgesNumNodes;

SELECT * from table(rai.degree('loginCreatedAtEdgesGraph', { 'result_table': 'cpsftesting.largejson.loginCreatedAtEdgesDegree' }));

SELECT LOOKUP(NODE):id as name, DEGREE FROM largejson.loginCreatedAtEdgesDegree// where name = 'cpetitpas'
    order by DEGREE DESC;