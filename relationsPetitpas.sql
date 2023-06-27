use role svcintdev;
use database cpSFtesting;
//create schema cp_test_project;
use warehouse compute_wh;
use schema rai;
call rai.use_rai_engine('cpSFtest0622');
call rai.use_rai_database('cpSFtesting0615');
select get_database('cpSFtesting0615');

// base EDB data
create or replace table relationsPetitpas.parentPetitpas(parent string, child string) as select * from values ('Ron','Chris'),('Ron','Lisa'),('Ron','David'),('Ron','Michael'),('Vivian','Chris'),('Vivian','Lisa'),('Vivian','David'),('Vivian','Michael'),('Chris','Alex'),('Chris','Jacob'),('Chris','Chloe'),('Angela','Alex'),('Angela','Jacob'),('Angela','Chloe'),('Lisa','Steph'),('Lisa','Amanda'),('Bill','Steph'),('Bill','Amanda'),('Michael','Jaysin'),('Michael','David Jr'),('Michael','Nathan');

// grandparent
create or replace table relationsPetitpas.grandparent as
SELECT GrandData.Parent as Grandparent, ParentData.GrandChild as GrandChild

From relationsPetitpas.parentpetitpas as GrandData

Inner JOIN 
(
    SELECT parent as parent, child as GrandChild
    FROM relationsPetitpas.parentpetitpas
    WHERE parent IN (Select child from relationsPetitpas.parentpetitpas)
) As ParentData

On GrandData.Child=ParentData.Parent

Order by GrandData.Parent;
select * from relationsPetitpas.grandparent;

// sibling
create or replace table relationsPetitpas.sibling as
SELECT ParentData1.child as Sibling1, ParentData2.Sibling2 as Sibling2

From relationsPetitpas.parentpetitpas as ParentData1

Inner JOIN 
(
    select Distinct child as sibling2, parent as parent
    FROM relationsPetitpas.parentpetitpas
    WHERE parent IN (Select parent from relationsPetitpas.parentpetitpas)
) As ParentData2

On ParentData1.parent = ParentData2.parent and ParentData1.child <> ParentData2.sibling2
Group By ParentData1.child, ParentData2.Sibling2
Order by ParentData1.child;
select * from relationsPetitpas.sibling;

// auntUncle
create or replace table relationsPetitpas.auntUncle as
SELECT ParentData.nieceNephew as nieceNephew, SiblingData.sibling1 as auntUncle1

From relationsPetitpas.sibling as SiblingData

INNER JOIN 
(
    select child as nieceNephew, parent as parent
    FROM relationsPetitpas.parentpetitpas
    WHERE parent IN (Select sibling1 from relationsPetitpas.sibling) and child IN (Select sibling1 from relationsPetitpas.sibling)
) As ParentData

On SiblingData.sibling2 = ParentData.parent
Group by ParentData.nieceNephew, SiblingData.sibling1
Order by ParentData.nieceNephew;
select * from relationsPetitpas.auntUncle;

// cousin
create or replace table relationsPetitpas.cousin as
SELECT ParentData1.child as cousin1, ParentData2.child as cousin2

From relationsPetitpas.parentpetitpas as ParentData1
     inner join relationsPetitpas.parentpetitpas as ParentData2
     on ParentData1.parent IN (SELECT sibling1 from relationsPetitpas.sibling where sibling2 = ParentData2.parent);

select * from relationsPetitpas.cousin;

// entities and lookups for auntUncle

call create_entity('nieceNephew', ['id']);
call create_entity('auntUncle', ['id']);
SELECT * FROM TABLE(RAI.LIST_ENTITIES());

call create_lookup('nieceNephew', 'relationsPetitpas.auntUncle', 'nieceNephew');
call create_lookup('auntUncle', 'relationsPetitpas.auntUncle', 'auntUncle1');
SELECT * FROM TABLE(RAI.LIST_LOOKUPS());

CREATE or replace table relationsPetitpas.auntUncleEdges(source, target) AS (
    SELECT cpsftesting.RAI.node('nieceNephew', [nieceNephew]), cpsftesting.RAI.node('auntUncle', [auntUncle1])
    FROM relationsPetitpas.auntUncle
);
select * from relationsPetitpas.auntUncleEdges;

call rai.create_data_stream('relationsPetitpas.auntUncleEdges');
call rai.delete_data_stream('relationsPetitpas.auntUncleEdges');
select rai.get_data_stream('cpsftesting.relationsPetitpas.auntUncleEdges');

//call rai.create_graph('auntUncleGraph', 'auntUncle');

call rai.create_graph('auntUncleEdgesGraph', 'relationsPetitpas.auntUncleEdges');
call rai.delete_graph('auntUncleEdgesGraph');

select * from table(rai.neighbor('auntUncleEdgesGraph', { 'result_table': 'cpsftesting.relationsPetitpas.auntUncleEdgesNeighbor' }));
SELECT LOOKUP(COL1):id as nieceNephewNode, LOOKUP(COL2):id as auntUncleNode FROM relationsPetitpas.auntUncleEdgesNeighbor
    order by nieceNephewNode;
select * from relationsPetitpas.auntUncleEdgesNeighbor;

SELECT * from table(rai.degree('auntUncleEdgesGraph', { 'result_table': 'cpsftesting.relationsPetitpas.auntUncleEdgesDegree' }));
select * from relationsPetitpas.auntUncleEdgesDegree;
SELECT LOOKUP(COL1):id as nieceNephew, COL2 as Degree FROM relationsPetitpas.auntUncleEdgesDegree
    order by Degree DESC;

select * from table(rai.pagerank('auntUncleEdgesGraph', { 'result_table': 'cpsftesting.relationsPetitpas.auntUncleEdgesPagerank' }));
SELECT LOOKUP(COL1):id as name, COL2 as Pagerank FROM relationsPetitpas.auntUncleEdgesPagerank
    order by Pagerank DESC;
