# DDBMS Website Demo - Quan ly cua hang tien loi

Demo website minh hoa 4 y tuong chinh trong do an co so du lieu phan tan:

1. Dang nhap theo chi nhanh (HUE, SAIGON, HANOI, CENTRAL).
2. CRUD cuc bo tren tung phan manh ngang.
3. Truy van toan cuc tai Central (Distributed Transparency).
4. Luan chuyen hang hoa bang giao dich phan tan (Distributed Transaction).

Ban mo rong hien tai:

- CRUD day du cho tung chi nhanh: NhanVien, HoaDon, TonKho.
- Dashboard thong ke rieng cho moi chi nhanh.
- Tach task thanh tung page rieng de de trinh bay:
  - Branch: dashboard, nhan vien, hoa don, ton kho.
  - Central: tong quan, nhan vien toan quoc, luan chuyen hang.

## Stack

- Frontend: HTML, CSS, JavaScript thuần
- Backend: Node.js + Express
- Database: SQL Server (qua package mssql)

## Chay nhanh

1. Cai package:

   npm install

2. Tao file moi truong tu mau:

   Copy `.env.example` thanh `.env`

3. Chay server:

   npm start

4. Mo trinh duyet:

   http://localhost:3000

## 2 che do demo

- `MOCK_MODE=true`: khong can SQL Server, du lieu gia lap co san de ban trinh bay UI + luong phan tan.
- `MOCK_MODE=false`: ket noi SQL Server that tai 4 port 1401/1402/1403/1404.

## API chinh

- `GET /api/employees?branch=HUE|SAIGON|HANOI`
- `POST /api/employees`
- `PUT /api/employees/:employeeId?branch=...`
- `DELETE /api/employees/:employeeId?branch=...`
- `GET /api/invoices?branch=HUE|SAIGON|HANOI`
- `POST /api/invoices`
- `PUT /api/invoices/:invoiceId?branch=...`
- `DELETE /api/invoices/:invoiceId?branch=...`
- `GET /api/branch-dashboard?branch=HUE|SAIGON|HANOI`
- `GET /api/inventory?branch=...`
- `POST /api/inventory`
- `PUT /api/inventory/:productCode?branch=...`
- `DELETE /api/inventory/:productCode?branch=...`
- `GET /api/all-employees?branch=CENTRAL`
- `GET /api/revenue/national?branch=CENTRAL`
- `POST /api/transfer-stock`
- `GET /api/inventory?branch=...&productCode=...`

## Luu y schema SQL

Code backend dang map theo ten bang cot pho bien:

- `NhanVien`
- `HoaDon`
- `TonKho`

Neu ten bang/cot cua ban khac, chi can sua file:

- `src/services/store-service.js`

## Luong trinh bay de tai

1. Vao trang dau, chon `Chi nhanh Hue`.
2. Xem danh sach nhan vien cuc bo (`/api/employees?branch=HUE`).
3. Them hoa don moi (`/api/invoices`) de tao du lieu tai Hue.
4. Dang xuat, vao `Tong cong ty`.
5. Tai `Danh sach Toan bo Nhan vien` va `Bao cao Doanh thu Toan quoc`.
6. Vao `Luan chuyen hang hoa`: chuyen 50 `MI_GOI` tu HUE sang SAIGON.
7. He thong hien ket qua commit va ton kho sau giao dich.
