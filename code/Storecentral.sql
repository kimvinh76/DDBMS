/*
  CENTRAL DEPLOY SCRIPT
  - Idempotent (CREATE OR ALTER)
  - No test EXEC/INSERT side effects
  - Aligned for gradual backend migration
*/

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

   
    IF EXISTS (SELECT 1 FROM dbo.ChiTietHoaDon WHERE MaSP = @MaSP)
    BEGIN
        THROW 50002, N'Lỗi nghiệp vụ: Sản phẩm này đã phát sinh hóa đơn bán hàng, không thể xóa để bảo toàn dữ liệu lịch sử!', 1;
    END

    BEGIN TRY
        BEGIN TRANSACTION;

        
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

CREATE OR ALTER PROCEDURE dbo.usp_Central_NhapKho
    @MaSP VARCHAR(50),
    @SoLuongNhap INT,
    @ChiNhanhLap VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @ChiNhanhLap NOT IN ('HUE', 'SAIGON', 'HANOI')
        THROW 50000, N'Chi nhánh nhập kho không hợp lệ!', 1;

    IF NULLIF(LTRIM(RTRIM(@MaSP)), '') IS NULL
        THROW 50000, N'Mã sản phẩm không được để trống!', 1;

    IF @SoLuongNhap <= 0
        THROW 50000, N'Số lượng nhập kho phải lớn hơn 0!', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        IF NOT EXISTS (SELECT 1 FROM dbo.TonKho WHERE MaSP = @MaSP AND ChiNhanh = @ChiNhanhLap)
            THROW 50001, N'Sản phẩm không tồn tại trong kho của chi nhánh này!', 1;

        UPDATE dbo.TonKho
        SET SoLuongTon = SoLuongTon + @SoLuongNhap
        WHERE MaSP = @MaSP AND ChiNhanh = @ChiNhanhLap;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Central_DieuChuyenKho
    @TuChiNhanh VARCHAR(10),
    @DenChiNhanh VARCHAR(10),
    @MaSP VARCHAR(50),
    @SoLuongChuyen INT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @TuChiNhanh NOT IN ('HUE', 'SAIGON', 'HANOI') OR @DenChiNhanh NOT IN ('HUE', 'SAIGON', 'HANOI')
        THROW 50000, N'Chi nhánh điều chuyển không hợp lệ!', 1;

    IF @TuChiNhanh = @DenChiNhanh
        THROW 50000, N'Chi nhánh xuất và nhập không được trùng nhau!', 1;

    IF NULLIF(LTRIM(RTRIM(@MaSP)), '') IS NULL
        THROW 50000, N'Mã sản phẩm không được để trống!', 1;

    IF @SoLuongChuyen <= 0
        THROW 50001, N'Số lượng điều chuyển phải lớn hơn 0!', 1;

    DECLARE @TonKhoHienTai INT;
    SELECT @TonKhoHienTai = SoLuongTon
    FROM dbo.TonKho
    WHERE MaSP = @MaSP AND ChiNhanh = @TuChiNhanh;

    IF @TonKhoHienTai IS NULL
        THROW 50002, N'Sản phẩm không tồn tại ở chi nhánh xuất!', 1;

    IF @TonKhoHienTai < @SoLuongChuyen
        THROW 50003, N'Chi nhánh xuất không đủ số lượng để chuyển!', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        UPDATE dbo.TonKho
        SET SoLuongTon = SoLuongTon - @SoLuongChuyen
        WHERE MaSP = @MaSP AND ChiNhanh = @TuChiNhanh;

        UPDATE dbo.TonKho
        SET SoLuongTon = SoLuongTon + @SoLuongChuyen
        WHERE MaSP = @MaSP AND ChiNhanh = @DenChiNhanh;

        IF @@ROWCOUNT = 0
            INSERT INTO dbo.TonKho (MaSP, SoLuongTon, ChiNhanh)
            VALUES (@MaSP, @SoLuongChuyen, @DenChiNhanh);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO

