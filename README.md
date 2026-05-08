# DDBMS - Huong Dan Cai Dat Nhanh



## 1. Yêu cầu

- Node.js 18+
- SQL Server + SSMS
- Đã bật TCP/IP trong SQL Server Configuration Manager

## 2 Chuẩn bị SQL

Trước khi chạy script, cần tạo sẵn 4 database đúng tên:

- `Store_H`
- `Store_SG`
- `Store_HN`
- `CentralDB`

Lưu ý quan trọng:
- File `maindb.sql` dùng lệnh `USE <DB>`, nên nếu chưa có đúng tên database thì script sẽ lỗi ngay.

Trong thư mục `code`, chạy lần lượt các file SQL sau trong SSMS:

1. `maindb.sql` (tạo bảng và dữ liệu nền)
2. `STOREPROCEDUREHUE.sql` (proc cho chi nhánh HUE)
3. `STOREPROCEDURESAIGON.sql` (proc cho chi nhánh SAIGON)
4. `STOREPROCEDUREHANOI.sql` (proc cho chi nhánh HANOI)
5. `STOREPROCEDURECENTRAL.sql` (proc cho CENTRAL)

## 3. Cấu hình host/port trong SSMS

### 3.1 Kiểm tra instance trong SSMS

- Server Name có dạng: `MAY\INSTANCE`
- Ví dụ: `DESKTOP-GVGU8JJ\MSSQLSERVER02`, `DESKTOP-GVGU8JJ\MSSQLSERVER05`

### 3.2 Đặt TCP Port cho từng instance

1. Mở SQL Server Configuration Manager.
2. Vào `SQL Server Network Configuration` -> `Protocols for <INSTANCE>`.
3. Enable `TCP/IP`.
4. Mở `TCP/IP` -> `Properties` -> tab `IP Addresses`.
5. Ở `IPAll`:
   - `TCP Dynamic Ports` = rỗng
   - `TCP Port` = port muốn dùng (ví dụ 1401/1402/1403/1404)
6. Restart lại service SQL của instance đó.

### 3.3 Map vào `.env`

- `*_DB_HOST`: tên máy hoặc IP (ví dụ `localhost` hoặc `DESKTOP-GVGU8JJ`)
- `*_DB_PORT`: port vừa đặt cho instance đó

Lưu ý quan trọng:
- Nếu host/port trên máy bạn khác, chỉ cần sửa lại trong `.env` cho đúng.

## 4. Tạo file môi trường

Copy `code/.env.example` thành `code/.env`.

Sửa tối thiểu các giá trị kết nối SQL theo máy bạn:

```env
PORT=3000
MOCK_MODE=false

HUE_DB_HOST=localhost
HUE_DB_PORT=1401
HUE_DB_USER=sa
HUE_DB_PASSWORD=YourPassword
HUE_DB_NAME=Store_H

SAIGON_DB_HOST=localhost
SAIGON_DB_PORT=1402
SAIGON_DB_USER=sa
SAIGON_DB_PASSWORD=YourPassword
SAIGON_DB_NAME=Store_SG

HANOI_DB_HOST=localhost
HANOI_DB_PORT=1403
HANOI_DB_USER=sa
HANOI_DB_PASSWORD=YourPassword
HANOI_DB_NAME=Store_HN

CENTRAL_DB_HOST=localhost
CENTRAL_DB_PORT=1404
CENTRAL_DB_USER=sa
CENTRAL_DB_PASSWORD=YourPassword
CENTRAL_DB_NAME=CentralDB

LINKED_HUE=HUE_SERVER
LINKED_SAIGON=SG_SERVER
LINKED_HANOI=HN_SERVER
```

Lưu ý quan trọng:
- Mật khẩu SQL trong `.env` phải đúng với tài khoản SQL Server (ví dụ `sa`).

## 5. Linked server theo mô hình bạn đang dùng

Mô hình hiện tại:
- Không bắt buộc tạo linked server ở CENTRAL.
- Tạo linked server trực tiếp ở từng chi nhánh để gọi qua chi nhánh khác.

Ví dụ:
- Ở node HUE có `LINK_SAIGON`, `LINK_HANOI`
- Ở node SAIGON có `LINK_HUE`, `LINK_HANOI`
- Ở node HANOI có `LINK_HUE`, `LINK_SAIGON`

Lưu ý quan trọng:
- Tên linked server trong proc/view phải khớp đúng với tên đã tạo trên node đó.

## 6. Chạy ứng dụng

Tại thư mục `code`:

```bash
npm install
npm run dev
```

Mở trình duyệt: `http://localhost:3000`

## 7. Nếu không kết nối được SQL

- Kiểm tra SQL Server service đang chạy
- Kiểm tra TCP/IP đã bật
- Kiểm tra lại host/port/user/password trong `code/.env`
