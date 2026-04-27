
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


 CREATE OR ALTER PROCEDURE dbo.usp_Central_DieuChuyenKho
    @TuChiNhanh VARCHAR(10),
    @DenChiNhanh VARCHAR(10),
    @MaSP VARCHAR(50),
    @SoLuongChuyen INT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON; 
	
    SET @TuChiNhanh = LTRIM(RTRIM(ISNULL(@TuChiNhanh, '')));
    SET @DenChiNhanh = LTRIM(RTRIM(ISNULL(@DenChiNhanh, '')));
    SET @MaSP = LTRIM(RTRIM(ISNULL(@MaSP, '')));
    --  Validate dữ liệu đầu vào
    IF @TuChiNhanh NOT IN ('HUE', 'SAIGON', 'HANOI') OR @DenChiNhanh NOT IN ('HUE', 'SAIGON', 'HANOI')
        THROW 50000, N'Chi nhánh điều chuyển không hợp lệ!', 1;

    IF @TuChiNhanh = @DenChiNhanh
        THROW 50000, N'Chi nhánh xuất và nhập không được trùng nhau!', 1;

    IF NULLIF(LTRIM(RTRIM(@MaSP)), '') IS NULL
        THROW 50000, N'Mã sản phẩm không được để trống!', 1;

    IF @SoLuongChuyen <= 0
        THROW 50001, N'Số lượng điều chuyển phải lớn hơn 0!', 1;

    -- 2. Ánh xạ tên Linked Server và Database tương ứng
    DECLARE @SrcServer NVARCHAR(50), @SrcDB NVARCHAR(50);
    DECLARE @DestServer NVARCHAR(50), @DestDB NVARCHAR(50);

    -- Máy chủ Nguồn (TuChiNhanh)
    IF @TuChiNhanh = 'HUE' BEGIN SET @SrcServer = 'HUE_SERVER'; SET @SrcDB = 'Store_H'; END
    ELSE IF @TuChiNhanh = 'SAIGON' BEGIN SET @SrcServer = 'SG_SERVER'; SET @SrcDB = 'Store_SG'; END
    ELSE IF @TuChiNhanh = 'HANOI' BEGIN SET @SrcServer = 'HN_SERVER'; SET @SrcDB = 'Store_HN'; END

    -- Máy chủ Đích (DenChiNhanh)
    IF @DenChiNhanh = 'HUE' BEGIN SET @DestServer = 'HUE_SERVER'; SET @DestDB = 'Store_H'; END
    ELSE IF @DenChiNhanh = 'SAIGON' BEGIN SET @DestServer = 'SG_SERVER'; SET @DestDB = 'Store_SG'; END
    ELSE IF @DenChiNhanh = 'HANOI' BEGIN SET @DestServer = 'HN_SERVER'; SET @DestDB = 'Store_HN'; END

    -- . Kiểm tra tồn kho tại máy chủ nguồn 
    DECLARE @SQL_Check NVARCHAR(MAX);
    DECLARE @TonKhoHienTai INT;
    
    SET @SQL_Check = N'SELECT @TonKhoOut = SoLuongTon FROM [' + @SrcServer + '].[' + @SrcDB + '].dbo.TonKho WHERE MaSP = @p_MaSP AND ChiNhanh = @p_ChiNhanh';
    
    EXEC sp_executesql 
        @stmt = @SQL_Check, 
        @params = N'@p_MaSP VARCHAR(50), @p_ChiNhanh VARCHAR(10), @TonKhoOut INT OUTPUT', 
        @p_MaSP = @MaSP, 
        @p_ChiNhanh = @TuChiNhanh, 
        @TonKhoOut = @TonKhoHienTai OUTPUT;

    IF @TonKhoHienTai IS NULL
        THROW 50002, N'Sản phẩm không tồn tại ở chi nhánh xuất!', 1;

    IF @TonKhoHienTai < @SoLuongChuyen
        THROW 50003, N'Chi nhánh xuất không đủ số lượng để chuyển!', 1;

    
    BEGIN TRY
        BEGIN DISTRIBUTED TRANSACTION;

        --  Trừ tồn kho chi nhánh xuất
        DECLARE @SQL_UpdateSrc NVARCHAR(MAX);
        SET @SQL_UpdateSrc = N'
            UPDATE [' + @SrcServer + '].[' + @SrcDB + '].dbo.TonKho
            SET SoLuongTon = SoLuongTon - @p_SoLuong
            WHERE MaSP = @p_MaSP AND ChiNhanh = @p_ChiNhanh;
        ';
        EXEC sp_executesql @SQL_UpdateSrc, N'@p_MaSP VARCHAR(50), @p_ChiNhanh VARCHAR(10), @p_SoLuong INT', @MaSP, @TuChiNhanh, @SoLuongChuyen;

        --  Cộng/Thêm tồn kho chi nhánh nhập
        DECLARE @SQL_UpdateDest NVARCHAR(MAX);
        SET @SQL_UpdateDest = N'
            UPDATE [' + @DestServer + '].[' + @DestDB + '].dbo.TonKho
            SET SoLuongTon = SoLuongTon + @p_SoLuong
            WHERE MaSP = @p_MaSP AND ChiNhanh = @p_ChiNhanh;

            -- Nếu đích chưa có sản phẩm này, tự động tạo mới
            IF @@ROWCOUNT = 0
            BEGIN
                INSERT INTO [' + @DestServer + '].[' + @DestDB + '].dbo.TonKho (MaSP, SoLuongTon, ChiNhanh)
                VALUES (@p_MaSP, @p_SoLuong, @p_ChiNhanh);
            END
        ';
        EXEC sp_executesql @SQL_UpdateDest, N'@p_MaSP VARCHAR(50), @p_ChiNhanh VARCHAR(10), @p_SoLuong INT', @MaSP, @DenChiNhanh, @SoLuongChuyen;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO



SELECT ChiNhanh, MaSP, SoLuongTon AS TonKho_TruocKhiChuyen 
FROM dbo.TonKho 
WHERE MaSP = 'MI_GOI' AND ChiNhanh IN ('HUE', 'SAIGON')
ORDER BY ChiNhanh;


EXEC dbo.usp_Central_DieuChuyenKho 
    @TuChiNhanh = 'HUE', 
    @DenChiNhanh = 'SAIGON', 
    @MaSP = 'MI_GOI', 
    @SoLuongChuyen = 1;



SELECT ChiNhanh, MaSP, SoLuongTon AS TonKho_SauKhiChuyen 
FROM dbo.TonKho 
WHERE MaSP = 'MI_GOI' AND ChiNhanh IN ('HUE', 'SAIGON')
ORDER BY ChiNhanh;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Central_DoanhThuQuocGia
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        hd.ChiNhanh AS BranchCode,
        SUM(CAST(ct.SoLuong * ct.DonGia AS DECIMAL(18,2))) AS Revenue
    FROM dbo.HoaDon hd
    INNER JOIN dbo.ChiTietHoaDon ct ON ct.MaHD = hd.MaHD
    WHERE hd.ChiNhanh IN ('HUE', 'SAIGON', 'HANOI')
    GROUP BY hd.ChiNhanh
    ORDER BY hd.ChiNhanh;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Central_DoanhThuVaSoDon_TheoNgay
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        hd.ChiNhanh,
        CAST(hd.NgayTao AS DATE) AS Ngay,
        COUNT(DISTINCT hd.MaHD) AS TongSoDonHang,
        SUM(CAST(ct.SoLuong * ct.DonGia AS DECIMAL(18,2))) AS TongDoanhThu
    FROM dbo.HoaDon hd
    INNER JOIN dbo.ChiTietHoaDon ct ON ct.MaHD = hd.MaHD
    WHERE hd.ChiNhanh IN ('HUE', 'SAIGON', 'HANOI')
    GROUP BY hd.ChiNhanh, CAST(hd.NgayTao AS DATE)
    ORDER BY Ngay DESC, hd.ChiNhanh;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Central_DoanhThuVaSoDon_TheoTuan
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        hd.ChiNhanh,
        DATEPART(YEAR, hd.NgayTao) AS Nam,
        DATEPART(WEEK, hd.NgayTao) AS TuanTrongNam,
        COUNT(DISTINCT hd.MaHD) AS TongSoDonHang,
        SUM(CAST(ct.SoLuong * ct.DonGia AS DECIMAL(18,2))) AS TongDoanhThu
    FROM dbo.HoaDon hd
    INNER JOIN dbo.ChiTietHoaDon ct ON ct.MaHD = hd.MaHD
    WHERE hd.ChiNhanh IN ('HUE', 'SAIGON', 'HANOI')
    GROUP BY hd.ChiNhanh, DATEPART(YEAR, hd.NgayTao), DATEPART(WEEK, hd.NgayTao)
    ORDER BY Nam DESC, TuanTrongNam DESC, hd.ChiNhanh;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Central_NhanVienBanTotNhatTuan
AS
BEGIN
    SET NOCOUNT ON;

    WITH TinhDoanhThu AS (
        SELECT
            hd.ChiNhanh,
            hd.MaNV,
            nv.HoTen,
            SUM(CAST(ct.SoLuong * ct.DonGia AS DECIMAL(18,2))) AS TongDoanhThu,
            ROW_NUMBER() OVER (
                PARTITION BY hd.ChiNhanh
                ORDER BY SUM(CAST(ct.SoLuong * ct.DonGia AS DECIMAL(18,2))) DESC
            ) AS Hang
        FROM dbo.HoaDon hd
        INNER JOIN dbo.ChiTietHoaDon ct ON ct.MaHD = hd.MaHD
        INNER JOIN dbo.NhanVien nv ON nv.MaNV = hd.MaNV AND nv.ChiNhanh = hd.ChiNhanh
        WHERE hd.ChiNhanh IN ('HUE', 'SAIGON', 'HANOI')
          AND DATEPART(WEEK, hd.NgayTao) = DATEPART(WEEK, GETDATE())
          AND DATEPART(YEAR, hd.NgayTao) = DATEPART(YEAR, GETDATE())
        GROUP BY hd.ChiNhanh, hd.MaNV, nv.HoTen
    )
    SELECT ChiNhanh, MaNV, HoTen, TongDoanhThu
    FROM TinhDoanhThu
    WHERE Hang = 1;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Central_SanPhamBanChayNhat_MoiChiNhanh
AS
BEGIN
    SET NOCOUNT ON;

    WITH TinhSoLuong AS (
        SELECT
            hd.ChiNhanh,
            ct.MaSP,
            hh.TenHang,
            SUM(ct.SoLuong) AS TongSoLuongBan,
            ROW_NUMBER() OVER (
                PARTITION BY hd.ChiNhanh
                ORDER BY SUM(ct.SoLuong) DESC
            ) AS Hang
        FROM dbo.HoaDon hd
        INNER JOIN dbo.ChiTietHoaDon ct ON ct.MaHD = hd.MaHD
        INNER JOIN dbo.HangHoa hh ON hh.MaSP = ct.MaSP
        WHERE hd.ChiNhanh IN ('HUE', 'SAIGON', 'HANOI')
          AND DATEPART(WEEK, hd.NgayTao) = DATEPART(WEEK, GETDATE())
          AND DATEPART(YEAR, hd.NgayTao) = DATEPART(YEAR, GETDATE())
        GROUP BY hd.ChiNhanh, ct.MaSP, hh.TenHang
    )
    SELECT ChiNhanh, MaSP, TenHang, TongSoLuongBan
    FROM TinhSoLuong
    WHERE Hang = 1;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Central_SoSanhDoanhThuTuan
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        hd.ChiNhanh,
        SUM(CASE WHEN DATEPART(WEEK, hd.NgayTao) = DATEPART(WEEK, GETDATE())
                 THEN CAST(ct.SoLuong * ct.DonGia AS DECIMAL(18,2)) ELSE 0 END) AS DoanhThuTuanNay,
        SUM(CASE WHEN DATEPART(WEEK, hd.NgayTao) = DATEPART(WEEK, GETDATE()) - 1
                 THEN CAST(ct.SoLuong * ct.DonGia AS DECIMAL(18,2)) ELSE 0 END) AS DoanhThuTuanTruoc
    FROM dbo.HoaDon hd
    INNER JOIN dbo.ChiTietHoaDon ct ON ct.MaHD = hd.MaHD
    WHERE hd.ChiNhanh IN ('HUE', 'SAIGON', 'HANOI')
      AND DATEPART(YEAR, hd.NgayTao) = DATEPART(YEAR, GETDATE())
      AND DATEPART(WEEK, hd.NgayTao) IN (DATEPART(WEEK, GETDATE()), DATEPART(WEEK, GETDATE()) - 1)
    GROUP BY hd.ChiNhanh
    ORDER BY hd.ChiNhanh;
END;
GO



