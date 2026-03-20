# Huong Dan Chay DDBMS Bang SQL Server 



## 1. Yeu cau

- Node.js 18+
- SQL Server + SSMS
- Project source trong thu muc code

## 2. Cai dat project

1. Mo terminal tai thu muc code
2. Chay lenh:

```bash
npm install
```

## 3. Tao schema va seed du lieu

1. Mo file code/SQLQuery3.sql bang SSMS
2. Chay  script tạo các db trên các server khác nhau
3. Script se tao/lam moi bang theo mo hinh:
   - ChiNhanh
   - NhanVien
   - HangHoa
   - HoaDon 
   - ChiTietHoaDon
   - TonKho

Ghi chu ngan:
- Central da co bo seed lon hon (NhanVien/HoaDon/ChiTietHoaDon)
- ChiTietHoaDon co nhieu dong hon HoaDon de demo bao cao

## 4. Tao file env mới theo may ban

1. Copy file code/.env.example thanh code/.env
2. Sua code/.env theo may ban:
   - Neu SQL cung may voi app: DB_HOST=localhost
   
3. Dat MOCK_MODE=false de chay SQL that

Mau toi thieu:

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

## 5. Linked Server (co the lam bang GUI như báo cáo tham khảo )

Ban can linked server neu dung cac chuc nang toan cuc tai Central:
- Danh sach nhan vien toan quoc
- Bao cao doanh thu toan quoc
- Chuyen kho phan tan




## 6. Luu y quan trong

- code/.env la file rieng tung may, khong dung chung gia tri cho tat ca

- .env.example la mau chung cho team
