-- **********largeJson tests. Stage and file format not created here**********
-- **********start with empty table/relation**********

truncate largejson.logincreatedat;
--call unset_rai_exit_on_error();
select rai.exec($raiDatabase, $raiEngine, 'def delete[:loginCreatedAtEdgesView] = loginCreatedAtEdgesView', {}, false);

create or replace table largejson.largejson(x variant);
copy into largejson.largejson
    from @largejson.cplargejson
    pattern = 'large-file.json'    
    file_format = largejson.cplargejson;

call drop_all_lookups();
call drop_all_entities();

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

-- wait for data_stream to catch up
CALL SYSTEM$WAIT(120);
call rai.get_data_stream_status('cpsftesting.largejson.loginCreatedAtEdgesView');

call rai.create_graph('loginCreatedAtEdgesGraph', 'largejson.loginCreatedAtEdgesView');

select rai.num_nodes('loginCreatedAtEdgesGraph', { 'result_table': 'cpsftesting.largejson.loginCreatedAtEdgesNumNodes' });
select * from largejson.loginCreatedAtEdgesNumNodes;

DROP table IF EXISTS largejson.loginCreatedAtEdgesDegree();
SELECT * from table(rai.degree('loginCreatedAtEdgesGraph', { 'result_table': 'cpsftesting.largejson.loginCreatedAtEdgesDegree' }));

SELECT LOOKUP(NODE):id as name, DEGREE FROM largejson.loginCreatedAtEdgesDegree
    order by DEGREE DESC
    limit 50;

DROP table IF EXISTS largejson.loginCreatedAtEdgesPagerank();
SELECT * from table(rai.pagerank('loginCreatedAtEdgesGraph', { 'result_table': 'cpsftesting.largejson.loginCreatedAtEdgesPagerank' }));

SELECT LOOKUP(NODE):id as name, VALUE FROM largejson.loginCreatedAtEdgesPagerank
    order by VALUE DESC
    limit 50;

-- **********add to base table, run graphlib functions again**********

copy into largejson.largejson
    from @largejson.cplargejson
    pattern = 'large-file2.json'    
    file_format = largejson.cplargejson;

-- insert into post-processed table that is behind the data_stream view

insert into largejson.logincreatedat
    select x:actor:login, x:created_at from largejson.largejson;

-- wait for data_stream to catch up

CALL SYSTEM$WAIT(120);
call rai.get_data_stream_status('cpsftesting.largejson.loginCreatedAtEdgesView');

DROP table IF EXISTS largejson.loginCreatedAtEdgesDegree();
SELECT * from table(rai.degree('loginCreatedAtEdgesGraph', { 'result_table': 'cpsftesting.largejson.loginCreatedAtEdgesDegree' }));

SELECT LOOKUP(NODE):id as name, DEGREE FROM largejson.loginCreatedAtEdgesDegree
    order by DEGREE DESC
    limit 50;

DROP table IF EXISTS largejson.loginCreatedAtEdgesPagerank();
SELECT * from table(rai.pagerank('loginCreatedAtEdgesGraph', { 'result_table': 'cpsftesting.largejson.loginCreatedAtEdgesPagerank' }));

SELECT LOOKUP(NODE):id as name, VALUE FROM largejson.loginCreatedAtEdgesPagerank
    order by VALUE DESC
    limit 50;

