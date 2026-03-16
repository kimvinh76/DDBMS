CREATE DATABASE Store_H;
GO

USE Store_H;
GO


CREATE TABLE NhanVien (
    MaNV INT PRIMARY KEY,
    TenNV NVARCHAR(100),
    MaChiNhanh VARCHAR(5)
);

CREATE TABLE HangHoa (
    MaHang INT PRIMARY KEY,
    TenHang NVARCHAR(100),
    Gia DECIMAL(10,2)
);

CREATE TABLE HoaDon (
    MaHoaDon INT PRIMARY KEY,
    NgayLap DATE,
    MaNV INT
);

CREATE TABLE ChiTietHoaDon (
    MaHoaDon INT,
    MaHang INT,
    SoLuong INT,
    PRIMARY KEY (MaHoaDon, MaHang)
);

CREATE TABLE TonKho (
    MaHang INT,
    SoLuong INT
);


INSERT INTO NhanVien VALUES
(1,N'Nguyễn Văn A','H'),
(2,N'Lê Văn B','H');

INSERT INTO HangHoa VALUES
(1,N'Mì gói',5000),
(2,N'Nước suối',7000),
(3,N'Bánh snack',12000);

INSERT INTO HoaDon VALUES
(1,'2026-03-20',1),
(2,'2026-03-21',2);

INSERT INTO ChiTietHoaDon VALUES
(1,1,2),
(1,2,1),
(2,3,4);