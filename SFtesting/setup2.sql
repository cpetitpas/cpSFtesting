use schema RAI;

drop procedure if exists CPSFTESTING.cptriangle.checkenginecreate(NUMBER, NUMBER, NUMBER);
set raiEnginePrev = (select * from cptriangle.raiEnginePrev);
set raiEngine = '';
set curDow = date_part(dow, current_date);
set curMonth = date_part(mm, current_date);
set curDay = date_part(dd, current_date);

CREATE or replace PROCEDURE cpsftesting.cptriangle.checkEngineCreate(curDow INTEGER, curMonth INTEGER, curDay INTEGER)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
raiEnginePrev varchar;
raiEngine varchar;
BEGIN
    set curDow = date_part(dow, current_date);
    set curMonth = date_part(mm, current_date);
    set curDay = date_part(dd, current_date);
    set raiEnginePrev = (select * from cptriangle.raiEnginePrev);
    IF (curDow = 4 AND $raiEnginePrev != 'cpSFtest'||$curMonth||'_'||$curDay) THEN
            set raiEngine = 'cpSFtest'||$curMonth||'_'||$curDay;
            select rai.create_rai_engine($raiEngine, 'S');
            set raiEnginePrev = $raiEngine;
            create or replace table cptriangle.raiEnginePrev(raiEnginePrev varchar);
            insert into cptriangle.raiEnginePrev values ($raiEnginePrev);
            select * from cptriangle.raiEnginePrev;
            RETURN 'engine cpSFtest'||$curMonth||'_'||$curDay||' created';
        ELSEIF (curDow = 4 AND $raiEnginePrev = 'cpSFtest'||$curMonth||'_'||$curDay) THEN
            set raiEngine = $raiEnginePrev;
            RETURN 'engine cpSFtest'||$curMonth||'_'||$curDay||' already PROVISIONED';
        ELSEIF (curDow != 4) THEN
            set raiEngine = $raiEnginePrev;
            RETURN 'engine cpSFtest'||$curMonth||'_'||$curDay||' not created - created ONLY on Thursday (after Wednesday deploy)';
        ELSE
            RETURN 'Unexpected input:'|| 'raiEngine = '||$raiEngine||', raiEnginePrev = '||$raiEnginePrev||', curDow = '||$curDow;
        END IF;
END;
$$
;

call cpsftesting.cptriangle.checkEngineCreate($curDow, $curMonth, $curDay);
set raiDatabase = 'cpSFtesting0927';
call rai.use_rai_engine($raiEngine);
call rai.use_rai_database($raiDatabase);

show variables;
