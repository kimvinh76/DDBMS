USE Store_SG;
GO


/* ===== PHẦN 1: VIEW TOÀN CỤC ===== */
CREATE OR ALTER VIEW dbo.v_NhanVien_ToanQuoc AS
SELECT MaNV, HoTen, ChucVu, ChiNhanh FROM dbo.NhanVien
UNION ALL
SELECT MaNV, HoTen, ChucVu, ChiNhanh FROM [LINK_HUE].[Store_H].dbo.NhanVien
UNION ALL
SELECT MaNV, HoTen, ChucVu, ChiNhanh FROM [LINK_HANOI].[Store_HN].dbo.NhanVien;
GO

CREATE OR ALTER VIEW dbo.v_HoaDonChiTiet_ToanQuoc AS
SELECT hd.MaHD, hd.NgayTao, hd.ChiNhanh, hd.MaNV, ct.MaSP, ct.SoLuong, ct.DonGia,
       CAST(ct.SoLuong * ct.DonGia AS DECIMAL(18,2)) AS ThanhTien
FROM dbo.HoaDon hd
JOIN dbo.ChiTietHoaDon ct ON hd.MaHD = ct.MaHD
UNION ALL
SELECT hd.MaHD, hd.NgayTao, hd.ChiNhanh, hd.MaNV, ct.MaSP, ct.SoLuong, ct.DonGia,
       CAST(ct.SoLuong * ct.DonGia AS DECIMAL(18,2)) AS ThanhTien
FROM [LINK_HUE].[Store_H].dbo.HoaDon hd
JOIN [LINK_HUE].[Store_H].dbo.ChiTietHoaDon ct ON hd.MaHD = ct.MaHD
UNION ALL
SELECT hd.MaHD, hd.NgayTao, hd.ChiNhanh, hd.MaNV, ct.MaSP, ct.SoLuong, ct.DonGia,
       CAST(ct.SoLuong * ct.DonGia AS DECIMAL(18,2)) AS ThanhTien
FROM [LINK_HANOI].[Store_HN].dbo.HoaDon hd
JOIN [LINK_HANOI].[Store_HN].dbo.ChiTietHoaDon ct ON hd.MaHD = ct.MaHD;
GO

CREATE OR ALTER VIEW dbo.v_TonKho_ToanQuoc AS
SELECT MaSP, SoLuongTon, ChiNhanh FROM dbo.TonKho
UNION ALL
SELECT MaSP, SoLuongTon, ChiNhanh FROM [LINK_HUE].[Store_H].dbo.TonKho
UNION ALL
SELECT MaSP, SoLuongTon, ChiNhanh FROM [LINK_HANOI].[Store_HN].dbo.TonKho;
GO

/* ===== PHẦN 2: PROC BÁO CÁO TOÀN CỤC ===== */
CREATE OR ALTER PROCEDURE dbo.usp_DanhSachNhanVienToanHeThong
AS
BEGIN
    SET NOCOUNT ON;
    SELECT * FROM dbo.v_NhanVien_ToanQuoc ORDER BY ChiNhanh, MaNV;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_DoanhThuVaSoDon_TheoNgay
AS
BEGIN
    SET NOCOUNT ON;
    SELECT
        ChiNhanh,
        CAST(NgayTao AS DATE) AS Ngay,
        COUNT(DISTINCT MaHD) AS TongSoDonHang,
        SUM(ThanhTien) AS TongDoanhThu
    FROM dbo.v_HoaDonChiTiet_ToanQuoc
    GROUP BY ChiNhanh, CAST(NgayTao AS DATE)
    ORDER BY Ngay DESC, ChiNhanh;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_DoanhThuVaSoDon_TheoTuan
AS
BEGIN
    SET NOCOUNT ON;
    SELECT
        ChiNhanh,
        DATEPART(YEAR, NgayTao) AS Nam,
        DATEPART(WEEK, NgayTao) AS TuanTrongNam,
        COUNT(DISTINCT MaHD) AS TongSoDonHang,
        SUM(ThanhTien) AS TongDoanhThu
    FROM dbo.v_HoaDonChiTiet_ToanQuoc
    GROUP BY ChiNhanh, DATEPART(YEAR, NgayTao), DATEPART(WEEK, NgayTao)
    ORDER BY Nam DESC, TuanTrongNam DESC, ChiNhanh;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_NhanVienBanTotNhatTuan
AS
BEGIN
    SET NOCOUNT ON;

    WITH TinhDoanhThu AS (
        SELECT
            v.ChiNhanh,
            v.MaNV,
            nv.HoTen,
            SUM(v.ThanhTien) AS TongDoanhThu,
            ROW_NUMBER() OVER (PARTITION BY v.ChiNhanh ORDER BY SUM(v.ThanhTien) DESC) AS Hang
        FROM dbo.v_HoaDonChiTiet_ToanQuoc v
        JOIN dbo.v_NhanVien_ToanQuoc nv
          ON v.MaNV = nv.MaNV AND v.ChiNhanh = nv.ChiNhanh
        WHERE DATEPART(WEEK, v.NgayTao) = DATEPART(WEEK, GETDATE())
          AND DATEPART(YEAR, v.NgayTao) = DATEPART(YEAR, GETDATE())
        GROUP BY v.ChiNhanh, v.MaNV, nv.HoTen
    )
    SELECT ChiNhanh, MaNV, HoTen, TongDoanhThu
    FROM TinhDoanhThu
    WHERE Hang = 1;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_SanPhamBanChayNhat_MoiChiNhanh
AS
BEGIN
    SET NOCOUNT ON;

    WITH TinhSoLuong AS (
        SELECT
            v.ChiNhanh,
            v.MaSP,
            hh.TenHang,
            SUM(v.SoLuong) AS TongSoLuongBan,
            ROW_NUMBER() OVER (PARTITION BY v.ChiNhanh ORDER BY SUM(v.SoLuong) DESC) AS Hang
        FROM dbo.v_HoaDonChiTiet_ToanQuoc v
        JOIN dbo.HangHoa hh ON v.MaSP = hh.MaSP
        WHERE DATEPART(WEEK, v.NgayTao) = DATEPART(WEEK, GETDATE())
          AND DATEPART(YEAR, v.NgayTao) = DATEPART(YEAR, GETDATE())
        GROUP BY v.ChiNhanh, v.MaSP, hh.TenHang
    )
    SELECT ChiNhanh, MaSP, TenHang, TongSoLuongBan
    FROM TinhSoLuong
    WHERE Hang = 1;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_SoSanhDoanhThuTuan
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        ChiNhanh,
        SUM(CASE WHEN DATEPART(WEEK, NgayTao) = DATEPART(WEEK, GETDATE())
                 THEN ThanhTien ELSE 0 END) AS DoanhThuTuanNay,
        SUM(CASE WHEN DATEPART(WEEK, NgayTao) = DATEPART(WEEK, GETDATE()) - 1
                 THEN ThanhTien ELSE 0 END) AS DoanhThuTuanTruoc
    FROM dbo.v_HoaDonChiTiet_ToanQuoc
    WHERE DATEPART(YEAR, NgayTao) = DATEPART(YEAR, GETDATE())
      AND DATEPART(WEEK, NgayTao) IN (DATEPART(WEEK, GETDATE()), DATEPART(WEEK, GETDATE()) - 1)
    GROUP BY ChiNhanh
    ORDER BY ChiNhanh;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_XemTonKhoTatCaChiNhanh
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        v.ChiNhanh,
        v.MaSP,
        h.TenHang,
        v.SoLuongTon
    FROM dbo.v_TonKho_ToanQuoc v
    JOIN dbo.HangHoa h ON v.MaSP = h.MaSP
    ORDER BY v.MaSP, v.ChiNhanh;
END;
GO

/* ===== PHẦN 3: PROC NGHIỆP VỤ CỤC BỘ  ===== */
CREATE OR ALTER PROCEDURE dbo.usp_Local_DanhSachNhanVien
AS
BEGIN
    SET NOCOUNT ON;
    SELECT * FROM dbo.NhanVien;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Local_ThemNhanVien
    @MaNV VARCHAR(50),
    @HoTen NVARCHAR(120),
    @ChucVu NVARCHAR(80)
AS
BEGIN
    SET NOCOUNT ON;

    IF NULLIF(LTRIM(RTRIM(@MaNV)), '') IS NULL
        THROW 50000, N'Mã nhân viên không được để trống!', 1;

    IF NULLIF(LTRIM(RTRIM(@HoTen)), '') IS NULL
        THROW 50000, N'Họ tên không được để trống!', 1;

    IF NULLIF(LTRIM(RTRIM(@ChucVu)), '') IS NULL
        THROW 50000, N'Chức vụ không được để trống!', 1;

    IF EXISTS (SELECT 1 FROM dbo.NhanVien WHERE MaNV = @MaNV)
        THROW 50001, N'Mã nhân viên đã tồn tại!', 1;

    INSERT INTO dbo.NhanVien (MaNV, HoTen, ChucVu, ChiNhanh)
    VALUES (@MaNV, @HoTen, @ChucVu, 'SAIGON');

    SELECT TOP 1 * FROM dbo.NhanVien WHERE MaNV = @MaNV;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Local_CapNhatNhanVien
    @MaNV VARCHAR(50),
    @HoTen NVARCHAR(120) = NULL,
    @ChucVu NVARCHAR(80) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF NULLIF(LTRIM(RTRIM(@MaNV)), '') IS NULL
        THROW 50000, N'Mã nhân viên không được để trống!', 1;

    UPDATE dbo.NhanVien
    SET HoTen = COALESCE(NULLIF(LTRIM(RTRIM(@HoTen)), ''), HoTen),
        ChucVu = COALESCE(NULLIF(LTRIM(RTRIM(@ChucVu)), ''), ChucVu)
    WHERE MaNV = @MaNV AND ChiNhanh = 'SAIGON';

    IF @@ROWCOUNT = 0
        THROW 50001, N'Không tìm thấy nhân viên để cập nhật!', 1;

    SELECT TOP 1 * FROM dbo.NhanVien WHERE MaNV = @MaNV;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Local_XoaNhanVien
    @MaNV VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    IF NULLIF(LTRIM(RTRIM(@MaNV)), '') IS NULL
        THROW 50000, N'Mã nhân viên không được để trống!', 1;

    IF NOT EXISTS (SELECT 1 FROM dbo.NhanVien WHERE MaNV = @MaNV AND ChiNhanh = 'SAIGON')
        THROW 50001, N'Không tìm thấy nhân viên để xóa!', 1;

    DELETE FROM dbo.NhanVien
    WHERE MaNV = @MaNV AND ChiNhanh = 'SAIGON';

    SELECT CAST(1 AS BIT) AS deleted, @MaNV AS MaNV;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Local_DanhSachHoaDon
AS
BEGIN
    SET NOCOUNT ON;

    IF COL_LENGTH('dbo.HoaDon', 'MaNV') IS NOT NULL
    BEGIN
        SELECT
            hd.MaHD,
            SUM(CAST(ctd.SoLuong * ctd.DonGia AS DECIMAL(18,2))) AS TongTien,
            COUNT(ctd.MaSP) AS SoMon,
            hd.GhiChu,
            hd.NgayTao,
            hd.ChiNhanh,
            hd.MaNV,
            MAX(nv.HoTen) AS HoTenNhanVien
        FROM dbo.HoaDon hd
        LEFT JOIN dbo.ChiTietHoaDon ctd ON ctd.MaHD = hd.MaHD
        LEFT JOIN dbo.NhanVien nv ON nv.MaNV = hd.MaNV
        WHERE hd.ChiNhanh = 'SAIGON'
        GROUP BY hd.MaHD, hd.GhiChu, hd.NgayTao, hd.ChiNhanh, hd.MaNV
        ORDER BY hd.NgayTao DESC;
    END
    ELSE
    BEGIN
        SELECT
            hd.MaHD,
            SUM(CAST(ctd.SoLuong * ctd.DonGia AS DECIMAL(18,2))) AS TongTien,
            COUNT(ctd.MaSP) AS SoMon,
            hd.GhiChu,
            hd.NgayTao,
            hd.ChiNhanh,
            NULL AS MaNV,
            NULL AS HoTenNhanVien
        FROM dbo.HoaDon hd
        LEFT JOIN dbo.ChiTietHoaDon ctd ON ctd.MaHD = hd.MaHD
        WHERE hd.ChiNhanh = 'SAIGON'
        GROUP BY hd.MaHD, hd.GhiChu, hd.NgayTao, hd.ChiNhanh
        ORDER BY hd.NgayTao DESC;
    END
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Local_ChiTietHoaDon
    @MaHD VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        ctd.MaHD,
        ctd.MaSP,
        hh.TenHang,
        ctd.SoLuong,
        ctd.DonGia,
        CAST(ctd.SoLuong * ctd.DonGia AS DECIMAL(18,2)) AS ThanhTien
    FROM dbo.ChiTietHoaDon ctd
    LEFT JOIN dbo.HangHoa hh ON hh.MaSP = ctd.MaSP
    INNER JOIN dbo.HoaDon hd ON hd.MaHD = ctd.MaHD
    WHERE ctd.MaHD = @MaHD
      AND hd.ChiNhanh = 'SAIGON';
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Local_TaoHoaDon
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

    IF @ChiNhanhLap <> 'SAIGON'
        THROW 50000, N'Procedure local chỉ cho phép @ChiNhanhLap = ''SAIGON''.', 1;

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

    IF NOT EXISTS (SELECT 1 FROM dbo.NhanVien WHERE MaNV = @MaNV AND ChiNhanh = 'SAIGON')
        THROW 50002, N'Nhân viên không tồn tại ở chi nhánh SAIGON!', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        IF (SELECT SoLuongTon FROM dbo.TonKho WHERE MaSP = @MaSP AND ChiNhanh = 'SAIGON') < @SoLuongMua
            THROW 50003, N'Kho cục bộ không đủ số lượng để bán!', 1;

        IF NOT EXISTS (SELECT 1 FROM dbo.HoaDon WHERE MaHD = @MaHD)
            INSERT INTO dbo.HoaDon (MaHD, GhiChu, ChiNhanh, MaNV)
            VALUES (@MaHD, @GhiChu, 'SAIGON', @MaNV);

        INSERT INTO dbo.ChiTietHoaDon (MaHD, MaSP, SoLuong, DonGia)
        VALUES (@MaHD, @MaSP, @SoLuongMua, @DonGia);

        UPDATE dbo.TonKho
        SET SoLuongTon = SoLuongTon - @SoLuongMua
        WHERE MaSP = @MaSP AND ChiNhanh = 'SAIGON';

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Local_TaoHoaDonNhieuDong
    @MaHD VARCHAR(50),
    @MaNV VARCHAR(50),
    @GhiChu NVARCHAR(255),
    @ChiNhanhLap VARCHAR(10),
    @ItemsJson NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @ChiNhanhLap <> 'SAIGON'
        THROW 50000, N'Procedure local chỉ cho phép @ChiNhanhLap = ''SAIGON''.', 1;

    IF NULLIF(LTRIM(RTRIM(@MaHD)), '') IS NULL
        THROW 50000, N'Mã hóa đơn không được để trống!', 1;

    IF NULLIF(LTRIM(RTRIM(@MaNV)), '') IS NULL
        THROW 50000, N'Mã nhân viên không được để trống!', 1;

    IF NULLIF(LTRIM(RTRIM(@ItemsJson)), '') IS NULL
        THROW 50000, N'Danh sách món hàng không được để trống!', 1;

    IF NOT EXISTS (SELECT 1 FROM dbo.NhanVien WHERE MaNV = @MaNV AND ChiNhanh = 'SAIGON')
        THROW 50001, N'Nhân viên không tồn tại ở chi nhánh SAIGON!', 1;

    DECLARE @Items TABLE (
        MaSP VARCHAR(50) NOT NULL,
        SoLuong INT NOT NULL,
        DonGia DECIMAL(10,2) NULL
    );

    INSERT INTO @Items (MaSP, SoLuong, DonGia)
    SELECT
        LTRIM(RTRIM(MaSP)),
        SoLuong,
        DonGia
    FROM OPENJSON(@ItemsJson)
    WITH (
        MaSP VARCHAR(50) '$.MaSP',
        SoLuong INT '$.SoLuong',
        DonGia DECIMAL(10,2) '$.DonGia'
    );

    IF NOT EXISTS (SELECT 1 FROM @Items)
        THROW 50002, N'Danh sách món hàng rỗng hoặc sai định dạng JSON!', 1;

    IF EXISTS (
        SELECT 1 FROM @Items
        WHERE NULLIF(LTRIM(RTRIM(MaSP)), '') IS NULL OR SoLuong IS NULL OR SoLuong <= 0
    )
        THROW 50003, N'Mỗi dòng món phải có MaSP và SoLuong > 0!', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        ;WITH Agg AS (
            SELECT MaSP, SUM(SoLuong) AS SoLuong, MAX(DonGia) AS DonGia
            FROM @Items
            GROUP BY MaSP
        )
        SELECT 1
        FROM Agg a
        LEFT JOIN dbo.HangHoa h ON h.MaSP = a.MaSP
        WHERE COALESCE(NULLIF(a.DonGia, 0), h.Gia, 0) <= 0;

        IF @@ROWCOUNT > 0
            THROW 50004, N'Không xác định được đơn giá cho một hoặc nhiều sản phẩm!', 1;

        ;WITH Agg AS (
            SELECT MaSP, SUM(SoLuong) AS SoLuong
            FROM @Items
            GROUP BY MaSP
        )
        SELECT 1
        FROM Agg a
        LEFT JOIN dbo.TonKho t ON t.MaSP = a.MaSP AND t.ChiNhanh = 'SAIGON'
        WHERE t.SoLuongTon IS NULL OR t.SoLuongTon < a.SoLuong;

        IF @@ROWCOUNT > 0
            THROW 50005, N'Kho cục bộ không đủ số lượng cho một hoặc nhiều sản phẩm!', 1;

        IF NOT EXISTS (SELECT 1 FROM dbo.HoaDon WHERE MaHD = @MaHD)
            INSERT INTO dbo.HoaDon (MaHD, GhiChu, ChiNhanh, MaNV)
            VALUES (@MaHD, @GhiChu, 'SAIGON', @MaNV);

        ;WITH Agg AS (
            SELECT MaSP, SUM(SoLuong) AS SoLuong, MAX(DonGia) AS DonGia
            FROM @Items
            GROUP BY MaSP
        )
        INSERT INTO dbo.ChiTietHoaDon (MaHD, MaSP, SoLuong, DonGia)
        SELECT
            @MaHD,
            a.MaSP,
            a.SoLuong,
            COALESCE(NULLIF(a.DonGia, 0), h.Gia)
        FROM Agg a
        JOIN dbo.HangHoa h ON h.MaSP = a.MaSP;

        ;WITH Agg AS (
            SELECT MaSP, SUM(SoLuong) AS SoLuong
            FROM @Items
            GROUP BY MaSP
        )
        UPDATE t
        SET t.SoLuongTon = t.SoLuongTon - a.SoLuong
        FROM dbo.TonKho t
        JOIN Agg a ON a.MaSP = t.MaSP
        WHERE t.ChiNhanh = 'SAIGON';

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Local_DanhSachTonKho
AS
BEGIN
    SET NOCOUNT ON;

    SELECT MaSP, SoLuongTon
    FROM dbo.TonKho
    WHERE ChiNhanh = 'SAIGON'
    ORDER BY MaSP;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Local_TonKhoTheoMaSP
    @MaSP VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    IF NULLIF(LTRIM(RTRIM(@MaSP)), '') IS NULL
        THROW 50000, N'Mã sản phẩm không được để trống!', 1;

    SELECT TOP 1 MaSP, SoLuongTon
    FROM dbo.TonKho
    WHERE ChiNhanh = 'SAIGON' AND MaSP = @MaSP;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Local_NhapKho
    @MaSP VARCHAR(50),
    @SoLuongNhap INT,
    @ChiNhanhLap VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @ChiNhanhLap <> 'SAIGON'
        THROW 50000, N'Procedure local chỉ cho phép @ChiNhanhLap = ''SAIGON''.', 1;

    IF NULLIF(LTRIM(RTRIM(@MaSP)), '') IS NULL
        THROW 50000, N'Mã sản phẩm không được để trống!', 1;

    IF @SoLuongNhap <= 0
        THROW 50000, N'Số lượng nhập kho phải lớn hơn 0!', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        IF NOT EXISTS (
            SELECT 1 FROM dbo.TonKho WHERE MaSP = @MaSP AND ChiNhanh = 'SAIGON'
        )
            THROW 50001, N'Sản phẩm không tồn tại trong kho của chi nhánh này!', 1;

        UPDATE dbo.TonKho
        SET SoLuongTon = SoLuongTon + @SoLuongNhap
        WHERE MaSP = @MaSP AND ChiNhanh = 'SAIGON';

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_Local_DanhSachHangHoa
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        MaSP AS productCode,
        TenHang AS productName,
        CAST(Gia AS DECIMAL(10,2)) AS unitPrice
    FROM dbo.HangHoa
    