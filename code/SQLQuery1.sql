CREATE DATABASE CentralDB;
GO


EXEC sp_dropserver 'HUE_SERVER', 'droplogins';
EXEC sp_dropserver 'SG_SERVER', 'droplogins';
EXEC sp_dropserver 'HN_SERVER', 'droplogins';

EXEC sp_addlinkedserver 
@server='HUE_SERVER',
@srvproduct='',
@provider='MSOLEDBSQL',
@datasrc='SQL_HUE';

EXEC sp_addlinkedserver 
@server='SG_SERVER',
@srvproduct='',
@provider='MSOLEDBSQL',
@datasrc='SQL_SAIGON';


EXEC sp_addlinkedserver 
@server='HN_SERVER',
@srvproduct='',
@provider='MSOLEDBSQL',
@datasrc='SQL_HANOI';

EXEC sp_addlinkedsrvlogin
@rmtsrvname = 'HUE_SERVER',
@useself = 'false',
@rmtuser = 'sa',
@rmtpassword = '123456Aa@';


EXEC sp_addlinkedsrvlogin
@rmtsrvname = 'SG_SERVER',
@useself = 'false',
@rmtuser = 'sa',
@rmtpassword = '123456Aa@';


EXEC sp_addlinkedsrvlogin
@rmtsrvname = 'HN_SERVER',
@useself = 'false',
@rmtuser = 'sa',
@rmtpassword = '123456Aa@';


EXEC sp_testlinkedserver N'HUE_SERVER';
GO
EXEC sp_testlinkedserver N'SG_SERVER';
GO
EXEC sp_testlinkedserver N'HN_SERVER'

GO

SELECT * 
FROM HUE_SERVER.Store_H.dbo.HoaDon

