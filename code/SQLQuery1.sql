

/* file này dùng tạo link server từ server cha Central đến server con để làm thao tác chuyển hàng từ server này server khác  */
IF DB_ID('CentralDB') IS NULL
BEGIN
	CREATE DATABASE CentralDB;
END
GO

-- Data source cho linked server 
-- Neu chay nhieu SQL instance/port tren cung may, dung 127.0.0.1,port.
DECLARE @HUE_DATASRC NVARCHAR(128) = N'127.0.0.1,1401';
DECLARE @SG_DATASRC NVARCHAR(128) = N'127.0.0.1,1402';
DECLARE @HN_DATASRC NVARCHAR(128) = N'127.0.0.1,1403';



DECLARE @REMOTE_USER NVARCHAR(128) = N'sa';
DECLARE @REMOTE_PASSWORD NVARCHAR(128) = N'123456';

IF EXISTS (SELECT 1 FROM sys.servers WHERE name = 'HUE_SERVER')
	EXEC sp_dropserver 'HUE_SERVER', 'droplogins';
IF EXISTS (SELECT 1 FROM sys.servers WHERE name = 'SG_SERVER')
	EXEC sp_dropserver 'SG_SERVER', 'droplogins';
IF EXISTS (SELECT 1 FROM sys.servers WHERE name = 'HN_SERVER')
	EXEC sp_dropserver 'HN_SERVER', 'droplogins';

EXEC sp_addlinkedserver 
@server='HUE_SERVER',
@srvproduct='',
@provider='MSOLEDBSQL',
@datasrc=@HUE_DATASRC;

EXEC sp_addlinkedserver 
@server='SG_SERVER',
@srvproduct='',
@provider='MSOLEDBSQL',
@datasrc=@SG_DATASRC;


EXEC sp_addlinkedserver 
@server='HN_SERVER',
@srvproduct='',
@provider='MSOLEDBSQL',
@datasrc=@HN_DATASRC;

EXEC sp_addlinkedsrvlogin
@rmtsrvname = 'HUE_SERVER',
@useself = 'false',
@rmtuser = @REMOTE_USER,
@rmtpassword = @REMOTE_PASSWORD;


EXEC sp_addlinkedsrvlogin
@rmtsrvname = 'SG_SERVER',
@useself = 'false',
@rmtuser = @REMOTE_USER,
@rmtpassword = @REMOTE_PASSWORD;


EXEC sp_addlinkedsrvlogin
@rmtsrvname = 'HN_SERVER',
@useself = 'false',
@rmtuser = @REMOTE_USER,
@rmtpassword = @REMOTE_PASSWORD;


EXEC sp_serveroption 'HUE_SERVER', 'data access', 'true';
EXEC sp_serveroption 'HUE_SERVER', 'rpc', 'true';
EXEC sp_serveroption 'HUE_SERVER', 'rpc out', 'true';

EXEC sp_serveroption 'SG_SERVER', 'data access', 'true';
EXEC sp_serveroption 'SG_SERVER', 'rpc', 'true';
EXEC sp_serveroption 'SG_SERVER', 'rpc out', 'true';

EXEC sp_serveroption 'HN_SERVER', 'data access', 'true';
EXEC sp_serveroption 'HN_SERVER', 'rpc', 'true';
EXEC sp_serveroption 'HN_SERVER', 'rpc out', 'true';


EXEC sp_testlinkedserver N'HUE_SERVER';
GO
EXEC sp_testlinkedserver N'SG_SERVER';
GO
EXEC sp_testlinkedserver N'HN_SERVER'

GO

-- Sau khi tạo link server thành công, có thể truy vấn dữ liệu từ các server khác nhau bằng cách sử dụng tên link server và tên database/schema/table tương ứng.
SELECT * 
FROM HUE_SERVER.Store_H.dbo.HoaDon

