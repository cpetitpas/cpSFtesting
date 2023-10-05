use schema RAI;

set raiEngine = 'cpSFtest0927';
set raiDatabase = 'cpSFtesting0927';
call rai.use_rai_engine($raiEngine);
call rai.use_rai_database($raiDatabase);