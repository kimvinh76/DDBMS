
CREATE OR ALTER PROCEDURE dbo.usp_Central_DanhSachHangHoa
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        MaSP AS productCode,
        TenHang AS productName,
        CAST(Gia AS DECIMAL(10,2)) AS unitPrice
    FROM dbo.HangHoa
    ORDER BY MaSP;
END;
GO 


EXEC dbo.usp_Central_DanhSachHangHoa;
GO 



CREATE OR ALTER PROCEDURE dbo.usp_Central_HangHoaTheoMaSP
    @MaSP VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    IF NULLIF(LTRIM(RTRIM(@MaSP)), '') IS NULL
        THROW 50000, N'Mã sản phẩm không được để trống!', 1;

    SELECT TOP 1 MaSP, TenHang, CAST(Gia AS DECIMAL(10,2)) AS Gia
    FROM dbo.HangHoa
    WHERE MaSP = @MaSP;
END;
GO
EXEC dbo.usp_Central_HangHoaTheoMaSP @MaSP = 'MI_GOI';
GO

CREATE OR ALTER PROCEDURE dbo.usp_Central_ThemHangHoaMoi
    @MaSP VARCHAR(50),
    @TenHang NVARCHAR(100),
    @Gia DECIMAL(10,2)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF NULLIF(LTRIM(RTRIM(@MaSP)), '') IS NULL
        THROW 50000, N'Mã sản phẩm không được để trống!', 1;

    IF NULLIF(LTRIM(RTRIM(@TenHang)), '') IS NULL
        THROW 50000, N'Tên hàng không được để trống!', 1;

    IF @Gia <= 0
        THROW 50000, N'Giá bán phải lớn hơn 0!', 1;

    IF EXISTS (SELECT 1 FROM dbo.HangHoa WHERE MaSP = @MaSP)
        THROW 50000, N'Mã sản phẩm đã tồn tại tại Server Gốc!', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        INSERT INTO dbo.HangHoa (MaSP, TenHang, Gia)
        VALUES (@MaSP, @TenHang, @Gia);

        IF NOT EXISTS (SELECT 1 FROM dbo.TonKho WHERE MaSP = @MaSP AND ChiNhanh = 'HUE')
            INSERT INTO dbo.TonKho (MaSP, SoLuongTon, ChiNhanh) VALUES (@MaSP, 0, 'HUE');

        IF NOT EXISTS (SELECT 1 FROM dbo.TonKho WHERE MaSP = @MaSP AND ChiNhanh = 'SAIGON')
            INSERT INTO dbo.TonKho (MaSP, SoLuongTon, ChiNhanh) VALUES (@MaSP, 0, 'SAIGON');

        IF NOT EXISTS (SELECT 1 FROM dbo.TonKho WHERE MaSP = @MaSP AND ChiNhanh = 'HANOI')
            INSERT INTO dbo.TonKho (MaSP, SoLuongTon, ChiNhanh) VALUES (@MaSP, 0, 'HANOI');

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO
EXEC dbo.usp_Central_ThemHangHoaMoi @MaSP = 'SP_TEST_01', @TenHang = N'Sản phẩm Test', @Gia = 25000;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Central_CapNhatHangHoa
    @MaSP VARCHAR(50),
    @TenHang NVARCHAR(100) = NULL,
    @Gia DECIMAL(10,2) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF NULLIF(LTRIM(RTRIM(@MaSP)), '') IS NULL
        THROW 50000, N'Mã sản phẩm không được để trống!', 1;

    IF @Gia IS NOT NULL AND @Gia <= 0
        THROW 50000, N'Giá bán phải lớn hơn 0!', 1;

    IF NOT EXISTS (SELECT 1 FROM dbo.HangHoa WHERE MaSP = @MaSP)
        THROW 50001, N'Không tìm thấy sản phẩm để cập nhật!', 1;

    UPDATE dbo.HangHoa
    SET TenHang = COALESCE(NULLIF(LTRIM(RTRIM(@TenHang)), ''), TenHang),
        Gia = COALESCE(@Gia, Gia)
    WHERE MaSP = @MaSP;

    SELECT TOP 1 MaSP, TenHang, CAST(Gia AS DECIMAL(10,2)) AS Gia
    FROM dbo.HangHoa
    WHERE MaSP = @MaSP;
END;
GO
EXEC dbo.usp_Central_CapNhatHangHoa @MaSP = 'SP_TEST_01', @TenHang = N'Sản phẩm Test Đã Sửa', @Gia = 30000;
GO








EXEC dbo.usp_Central_XoaHangHoa @MaSP = 'SP_TEST_01';
GO



CREATE OR ALTER PROCEDURE dbo.usp_Central_ThemNhanVien
    @MaNV VARCHAR(50),
    @HoTen NVARCHAR(120),
    @ChucVu NVARCHAR(80),
    @ChiNhanh VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;

    IF NULLIF(LTRIM(RTRIM(@MaNV)), '') IS NULL
        THROW 50000, N'Mã nhân viên không được để trống!', 1;

    IF NULLIF(LTRIM(RTRIM(@HoTen)), '') IS NULL
        THROW 50000, N'Họ tên không được để trống!', 1;

    IF NULLIF(LTRIM(RTRIM(@ChucVu)), '') IS NULL
        THROW 50000, N'Chức vụ không được để trống!', 1;

    IF @ChiNhanh NOT IN ('HUE', 'SAIGON', 'HANOI')
        THROW 50000, N'Chi nhánh không hợp lệ (HUE/SAIGON/HANOI)!', 1;

    IF EXISTS (SELECT 1 FROM dbo.NhanVien WHERE MaNV = @MaNV)
        THROW 50000, N'Mã nhân viên đã tồn tại!', 1;

    INSERT INTO dbo.NhanVien (MaNV, HoTen, ChucVu, ChiNhanh)
    VALUES (@MaNV, @HoTen, @ChucVu, @ChiNhanh);

    SELECT TOP 1 * FROM dbo.NhanVien WHERE MaNV = @MaNV;
END;
GO
EXEC dbo.usp_Central_ThemNhanVien @MaNV = 'NV_TEST_01', @HoTen = N'Nhân Viên Test', @ChucVu = N'Quản Lý', @ChiNhanh = 'SAIGON';
GO

CREATE OR ALTER PROCEDURE dbo.usp_Central_CapNhatNhanVien
    @MaNV VARCHAR(50),
    @HoTen NVARCHAR(120) = NULL,
    @ChucVu NVARCHAR(80) = NULL,
    @ChiNhanh VARCHAR(10) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF NULLIF(LTRIM(RTRIM(@MaNV)), '') IS NULL
        THROW 50000, N'Mã nhân viên không được để trống!', 1;

    IF @ChiNhanh IS NOT NULL AND @ChiNhanh NOT IN ('HUE', 'SAIGON', 'HANOI')
        THROW 50000, N'Chi nhánh không hợp lệ (HUE/SAIGON/HANOI)!', 1;

    IF NOT EXISTS (SELECT 1 FROM dbo.NhanVien WHERE MaNV = @MaNV)
        THROW 50001, N'Không tìm thấy nhân viên để cập nhật!', 1;

    UPDATE dbo.NhanVien
    SET HoTen = COALESCE(NULLIF(LTRIM(RTRIM(@HoTen)), ''), HoTen),
        ChucVu = COALESCE(NULLIF(LTRIM(RTRIM(@ChucVu)), ''), ChucVu),
        ChiNhanh = COALESCE(@ChiNhanh, ChiNhanh)
    WHERE MaNV = @MaNV;

    SELECT TOP 1 * FROM dbo.NhanVien WHERE MaNV = @MaNV;
END;
GO
EXEC dbo.usp_Central_CapNhatNhanVien @MaNV = 'NV_TEST_01', @HoTen = N'Nhân Viên Đã Cập Nhật', @ChucVu = N'Thu Ngân', @ChiNhanh = 'HANOI';
GO

CREATE OR ALTER PROCEDURE dbo.usp_Central_XoaNhanVien
    @MaNV VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    IF NULLIF(LTRIM(RTRIM(@MaNV)), '') IS NULL
        THROW 50000, N'Mã nhân viên không được để trống!', 1;

    IF NOT EXISTS (SELECT 1 FROM dbo.NhanVien WHERE MaNV = @MaNV)
        THROW 50001, N'Không tìm thấy nhân viên để xóa!', 1;

    DELETE FROM dbo.NhanVien
    WHERE MaNV = @MaNV;

    SELECT CAST(1 AS BIT) AS deleted, @MaNV AS MaNV;
END;
GO

EXEC dbo.usp_Central_XoaNhanVien @MaNV = 'NV_TEST_01';
GO

CREATE OR ALTER PROCEDURE dbo.usp_Central_TaoHoaDon
    @MaHD VARCHAR(50),
    @MaNV VARCHAR(50),
    @MaSP VARCHAR(50),
    @SoLuongMua INT,
    @GhiChu NVARCHAR(255),
    @ChiNhanhLap VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @ChiNhanhLap NOT IN ('HUE', 'SAIGON', 'HANOI')
        THROW 50000, N'Chi nhánh lập hóa đơn không hợp lệ!', 1;

    IF NULLIF(LTRIM(RTRIM(@MaHD)), '') IS NULL
        THROW 50000, N'Mã hóa đơn không được để trống!', 1;

    IF NULLIF(LTRIM(RTRIM(@MaNV)), '') IS NULL
        THROW 50000, N'Mã nhân viên không được để trống!', 1;

    IF NULLIF(LTRIM(RTRIM(@MaSP)), '') IS NULL
        THROW 50000, N'Mã sản phẩm không được để trống!', 1;

    IF @SoLuongMua <= 0
        THROW 50000, N'Số lượng mua phải lớn hơn 0!', 1;

    DECLARE @DonGia DECIMAL(10,2);
    SELECT @DonGia = Gia FROM dbo.HangHoa WHERE MaSP = @MaSP;

    IF @DonGia IS NULL OR @DonGia <= 0
        THROW 50001, N'Không tìm thấy giá sản phẩm hợp lệ trong bảng HangHoa!', 1;

    IF NOT EXISTS (
        SELECT 1 FROM dbo.NhanVien WHERE MaNV = @MaNV AND ChiNhanh = @ChiNhanhLap
    )
        THROW 50002, N'Nhân viên không tồn tại ở chi nhánh lập hóa đơn!', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        IF (SELECT SoLuongTon FROM dbo.TonKho WHERE MaSP = @MaSP AND ChiNhanh = @ChiNhanhLap) < @SoLuongMua
            THROW 50003, N'Lỗi: Chi nhánh được chọn không đủ số lượng để bán!', 1;

        IF NOT EXISTS (SELECT 1 FROM dbo.HoaDon WHERE MaHD = @MaHD)
            INSERT INTO dbo.HoaDon (MaHD, GhiChu, ChiNhanh, MaNV)
            VALUES (@MaHD, @GhiChu, @ChiNhanhLap, @MaNV);

        INSERT INTO dbo.ChiTietHoaDon (MaHD, MaSP, SoLuong, DonGia)
        VALUES (@MaHD, @MaSP, @SoLuongMua, @DonGia);

        UPDATE dbo.TonKho
        SET SoLuongTon = SoLuongTon - @SoLuongMua
        WHERE MaSP = @MaSP AND ChiNhanh = @ChiNhanhLap;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO 

EXEC dbo.usp_Central_TaoHoaDon @MaHD = 'HD_CENTRAL_TEST_11 ', @MaNV = 'H001', @MaSP = 'MI_GOI', @SoLuongMua = 5, @GhiChu = N'Admin Trung tâm bán hàng ', @ChiNhanhLap = 'HUE';
GO


 


CREATE OR ALTER PROCEDURE dCREATE OR ALTER PROCEDURE dbo.usp_Central_DieuChuyenKho
    @MaSP VARCHAR(50),
    @SoLuong INT,
    @FromBranch VARCHAR(10),
    @ToBranch VARCHAR(10),
    @FromServer NVARCHAR(50),
    @FromDB NVARCHAR(50),
    @ToServer NVARCHAR(50),
    @ToDB NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @SQL NVARCHAR(MAX);

    BEGIN TRY
        BEGIN DISTRIBUTED TRANSACTION;

        SET @SQL = N'
            UPDATE [' + @FromServer + '].[' + @FromDB + '].dbo.TonKho
            SET SoLuongTon = SoLuongTon - @p_SoLuong
            WHERE MaSP = @p_MaSP AND ChiNhanh = @p_FromBranch;

            IF @@ROWCOUNT = 0
                THROW 50001, ''Source row not updated on linked server'', 1;

            UPDATE [' + @ToServer + '].[' + @ToDB + '].dbo.TonKho
            SET SoLuongTon = SoLuongTon + @p_SoLuong
            WHERE MaSP = @p_MaSP AND ChiNhanh = @p_ToBranch;

            IF @@ROWCOUNT = 0
                THROW 50002, ''Destination row not updated on linked server'', 1;
        ';

        EXEC sp_executesql @SQL,
            N'@p_MaSP VARCHAR(50), @p_SoLuong INT, @p_FromBranch VARCHAR(10), @p_ToBranch VARCHAR(10)',
            @p_MaSP = @MaSP, @p_SoLuong = @SoLuong, @p_FromBranch = @FromBranch, @p_ToBranch = @ToBranch;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 OR @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO
CREATE OR ALTER PROCEDURE dbo.usp_Central_XoaHangHoa
    @MaSP VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON; 

    IF NULLIF(LTRIM(RTRIM(@MaSP)), '') IS NULL
        THROW 50000, N'Mã sản phẩm không được để trống!', 1;

    IF NOT EXISTS (SELECT 1 FROM dbo.HangHoa WHERE MaSP = @MaSP)
        THROW 50001, N'Không tìm thấy sản phẩm để xóa!', 1;

    -- Kiểm tra hóa đơn trên toàn hệ thống (Central + Linked Servers)
    IF EXISTS (SELECT 1 FROM dbo.ChiTietHoaDon WHERE MaSP = @MaSP)
       OR EXISTS (SELECT 1 FROM [HUE_SERVER].[Store_H].dbo.ChiTietHoaDon WHERE MaSP = @MaSP)
       OR EXISTS (SELECT 1 FROM [SG_SERVER].[Store_SG].dbo.ChiTietHoaDon WHERE MaSP = @MaSP)
       OR EXISTS (SELECT 1 FROM [HN_SERVER].[Store_HN].dbo.ChiTietHoaDon WHERE MaSP = @MaSP)
    BEGIN
        THROW 50002, N'Lỗi nghiệp vụ: Sản phẩm này đã phát sinh hóa đơn bán hàng trên hệ thống, không thể xóa để bảo toàn dữ liệu lịch sử!', 1;
    END

    BEGIN TRY
        BEGIN DISTRIBUTED TRANSACTION;

        -- Xóa trên các chi nhánh thông qua Linked Server (HUE, SAIGON, HANOI)
        -- HUE
        IF EXISTS (SELECT 1 FROM [HUE_SERVER].[Store_H].dbo.HangHoa WHERE MaSP = @MaSP)
        BEGIN
            DELETE FROM [HUE_SERVER].[Store_H].dbo.TonKho WHERE MaSP = @MaSP;
            DELETE FROM [HUE_SERVER].[Store_H].dbo.HangHoa WHERE MaSP = @MaSP;
        END

        -- SAIGON
        IF EXISTS (SELECT 1 FROM [SG_SERVER].[Store_SG].dbo.HangHoa WHERE MaSP = @MaSP)
        BEGIN
            DELETE FROM [SG_SERVER].[Store_SG].dbo.TonKho WHERE MaSP = @MaSP;
            DELETE FROM [SG_SERVER].[Store_SG].dbo.HangHoa WHERE MaSP = @MaSP;
        END

        -- HANOI
        IF EXISTS (SELECT 1 FROM [HN_SERVER].[Store_HN].dbo.HangHoa WHERE MaSP = @MaSP)
        BEGIN
            DELETE FROM [HN_SERVER].[Store_HN].dbo.TonKho WHERE MaSP = @MaSP;
            DELETE FROM [HN_SERVER].[Store_HN].dbo.HangHoa WHERE MaSP = @MaSP;
        END

        -- Xóa trên CENTRAL
        DELETE FROM dbo.TonKho WHERE MaSP = @MaSP;
        DELETE FROM dbo.HangHoa WHERE MaSP = @MaSP;

        COMMIT TRANSACTION;

        SELECT CAST(1 AS BIT) AS deleted, @MaSP AS productCode;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO
