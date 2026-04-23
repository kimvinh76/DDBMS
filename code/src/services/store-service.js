const { sql, isMockMode, getPool } = require("../db/sqlserver");
const mock = require("../data/mock-store");

function dbNameByBranch(branch) {
  if (branch === "HUE") return "Store_H";
  if (branch === "SAIGON") return "Store_SG";
  return "Store_HN";
}

function linkedServerNames() {
  return {
    HUE: process.env.LINKED_HUE || "HUE_SERVER",
    SAIGON: process.env.LINKED_SAIGON || "SG_SERVER",
    HANOI: process.env.LINKED_HANOI || "HN_SERVER",
  };
}

function isReadProcMode() {
  return String(process.env.READONLY_USE_SP || "0") === "1";
}

function isEmployeeWriteProcMode() {
  return String(process.env.EMPLOYEE_WRITE_USE_SP || "0") === "1";
}

function isInventoryImportProcMode() {
  return String(process.env.INVENTORY_IMPORT_USE_SP || "0") === "1";
}

function isInvoiceCreateProcMode() {
  return String(process.env.INVOICE_CREATE_USE_SP || "0") === "1";
}

function invoiceProcEnabledBranches() {
  return String(process.env.INVOICE_CREATE_SP_BRANCHES || "")
    .split(",")
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);
}

function isInvoiceProcEnabledForBranch(branch) {
  if (!isInvoiceCreateProcMode()) {
    return false;
  }
  const enabled = invoiceProcEnabledBranches();
  return enabled.includes(String(branch || "").toUpperCase());
}

function localProcNamesByBranch() {
  return {
    listEmployees: "dbo.usp_Local_DanhSachNhanVien",
    createEmployee: "dbo.usp_Local_ThemNhanVien",
    updateEmployee: "dbo.usp_Local_CapNhatNhanVien",
    deleteEmployee: "dbo.usp_Local_XoaNhanVien",
    listInvoices: "dbo.usp_Local_DanhSachHoaDon",
    invoiceDetails: "dbo.usp_Local_ChiTietHoaDon",
    createInvoiceLines: "dbo.usp_Local_TaoHoaDonNhieuDong",
    listInventory: "dbo.usp_Local_DanhSachTonKho",
    inventoryByCode: "dbo.usp_Local_TonKhoTheoMaSP",
    updateInventory: "dbo.usp_Local_CapNhatTonKhoTongQuat",
    productByCode: "dbo.usp_Local_HangHoaTheoMaSP",
    dashboardSummary: "dbo.usp_Local_DashboardTongQuan",
    dashboardRevenue7d: "dbo.usp_Local_DashboardDoanhThu7Ngay",
    dashboardTopStock: "dbo.usp_Local_DashboardTopTonKho",
  };
}

function centralProcNames() {
  return {
    createEmployee: "dbo.usp_Central_ThemNhanVien",
    updateEmployee: "dbo.usp_Central_CapNhatNhanVien",
    deleteEmployee: "dbo.usp_Central_XoaNhanVien",
    createProduct: "dbo.usp_Central_ThemHangHoaMoi",
    updateProduct: "dbo.usp_Central_CapNhatHangHoa",
    deleteProduct: "dbo.usp_Central_XoaHangHoa",
  };
}

function normalizeAnalyticsBranch(branch) {
  const normalized = String(branch || "").trim().toUpperCase();
  if (["HUE", "SAIGON", "HANOI"].includes(normalized)) {
    return normalized;
  }
  return "HUE";
}

function sourceInfoByBranch(branch) {
  const linked = linkedServerNames();
  if (branch === "HUE") {
    return { server: linked.HUE, db: dbNameByBranch("HUE") };
  }
  if (branch === "SAIGON") {
    return { server: linked.SAIGON, db: dbNameByBranch("SAIGON") };
  }
  return { server: linked.HANOI, db: dbNameByBranch("HANOI") };
}

async function executeAnalyticsProcFromCentral(pool, sourceBranch, procName) {
  const source = sourceInfoByBranch(sourceBranch);
  const query = `EXEC [${source.server}].[${source.db}].dbo.${procName};`;
  const rs = await pool.request().query(query);
  return rs.recordset || [];
}

/*
 * Kiểm tra bảng `HoaDon` có cột `MaNV` hay không.
 * Truy vấn: SELECT CASE WHEN COL_LENGTH('dbo.HoaDon', 'MaNV') IS NULL THEN 0 ELSE 1 END AS hasColumn;
 * Dùng để quyết định có cần xử lý/validate nhân viên (MaNV) khi thao tác hóa đơn.
 */
async function hasHoaDonEmployeeColumnWithPool(pool) {
  const rs = await pool
    .request()
    .query("SELECT CASE WHEN COL_LENGTH('dbo.HoaDon', 'MaNV') IS NULL THEN 0 ELSE 1 END AS hasColumn;");
  return Boolean((rs.recordset[0] || {}).hasColumn);
}

/*
 * Tương tự nhưng chạy trong transaction hiện có.
 * Truy vấn: SELECT CASE WHEN COL_LENGTH('dbo.HoaDon', 'MaNV') IS NULL THEN 0 ELSE 1 END AS hasColumn;
 * Dùng trong luồng giao dịch của hóa đơn để kiểm tra cột `MaNV`.
 */
async function hasHoaDonEmployeeColumnWithTransaction(transaction) {
  const rs = await new sql.Request(transaction)
    .query("SELECT CASE WHEN COL_LENGTH('dbo.HoaDon', 'MaNV') IS NULL THEN 0 ELSE 1 END AS hasColumn;");
  return Boolean((rs.recordset[0] || {}).hasColumn);
}

/*
 * Kiểm tra tồn tại `MaNV` trong chi nhánh (trong transaction).
 * Truy vấn: SELECT TOP 1 MaNV FROM NhanVien WHERE MaNV = @MaNV AND ChiNhanh = @ChiNhanh
 * Ném lỗi nếu không tìm thấy. Dùng cho tạo/cập nhật hóa đơn.
 */
async function ensureEmployeeExists(transaction, branch, employeeId) {
  if (!employeeId) {
    return;
  }

  const rs = await new sql.Request(transaction)
    .input("MaNV", sql.VarChar(50), employeeId)
    .input("ChiNhanh", sql.VarChar(10), branch)
    .query(`
      SELECT TOP 1 MaNV
      FROM NhanVien
      WHERE MaNV = @MaNV AND ChiNhanh = @ChiNhanh;
    `);

  if (!rs.recordset.length) {
    throw new Error(`Employee ${employeeId} not found in ${branch}`);
  }
}

/*
 * Lấy số tồn hiện tại cho sản phẩm ở chi nhánh (trong transaction).
 * Truy vấn: SELECT TOP 1 SoLuongTon FROM TonKho WHERE ChiNhanh = @ChiNhanh AND MaSP = @MaSP
 * Trả về số (0 nếu không có). Dùng bởi deductStock/addStock và luồng hóa đơn.
 */
async function getStockByBranch(transaction, branch, productCode) {
  const rs = await new sql.Request(transaction)
    .input("ChiNhanh", sql.VarChar(10), branch)
    .input("MaSP", sql.VarChar(50), productCode)
    .query(`
      SELECT TOP 1 SoLuongTon
      FROM TonKho
      WHERE ChiNhanh = @ChiNhanh AND MaSP = @MaSP;
    `);
  return Number((rs.recordset[0] || {}).SoLuongTon);
}

/*
 * Trừ số lượng khỏi tồn kho (`TonKho`) trong transaction.
 * Kiểm tra tồn bằng `getStockByBranch`, sau đó chạy UPDATE TonKho ...
 * Ném lỗi nếu sản phẩm không tồn tại hoặc tồn không đủ.
 */
async function deductStock(transaction, branch, productCode, quantity) {
  const current = await getStockByBranch(transaction, branch, productCode);
  if (!Number.isFinite(current)) {
    throw new Error(`Product ${productCode} not found in inventory ${branch}`);
  }
  if (current < quantity) {
    throw new Error(
      `Insufficient stock for ${productCode} in ${branch}: current ${current}, required ${quantity}`,
    );
  }

  await new sql.Request(transaction)
    .input("ChiNhanh", sql.VarChar(10), branch)
    .input("MaSP", sql.VarChar(50), productCode)
    .input("SoLuong", sql.Int, quantity)
    .query(`
      UPDATE TonKho
      SET SoLuongTon = SoLuongTon - @SoLuong
      WHERE ChiNhanh = @ChiNhanh AND MaSP = @MaSP;
    `);
}

/*
 * Cộng số lượng vào tồn kho. Nếu dòng chưa có thì insert mới.
 * Thực hiện UPDATE TonKho ...; nếu @@ROWCOUNT = 0 thì INSERT INTO TonKho(...)
 * Dùng khi hoàn/hủy hóa đơn hoặc điều chỉnh tồn bằng tay.
 */
async function addStock(transaction, branch, productCode, quantity) {
  await new sql.Request(transaction)
    .input("ChiNhanh", sql.VarChar(10), branch)
    .input("MaSP", sql.VarChar(50), productCode)
    .input("SoLuong", sql.Int, quantity)
    .query(`
      UPDATE TonKho
      SET SoLuongTon = SoLuongTon + @SoLuong
      WHERE ChiNhanh = @ChiNhanh AND MaSP = @MaSP;

      IF @@ROWCOUNT = 0
      BEGIN
        INSERT INTO TonKho (MaSP, SoLuongTon, ChiNhanh)
        VALUES (@MaSP, @SoLuong, @ChiNhanh);
      END
    `);
}

/*
 * Liệt kê nhân viên từ DB chi nhánh.
 * Truy vấn: SELECT * FROM NhanVien
 * Nếu ở mock mode thì gọi `mock.listEmployeesByBranch`.
 */
async function listEmployeesByBranch(branch) {
  if (isMockMode()) {
    return mock.listEmployeesByBranch(branch);
  }

  const pool = await getPool(branch);
  if (isReadProcMode()) {
    // SP mode (Phase 1): đọc danh sách nhân viên qua proc local.
    const procs = localProcNamesByBranch();
    const result = await pool.request().execute(procs.listEmployees);
    return result.recordset;
  }

  const result = await pool.request().query("SELECT * FROM NhanVien;");
  return result.recordset;
}

/*
 * Tạo nhân viên mới trong DB chi nhánh.
 * Truy vấn: INSERT INTO NhanVien(...) rồi SELECT TOP 1 * để trả về bản ghi tạo.
 * Nếu mock mode thì dùng `mock.createEmployee`.
 */
async function createEmployee(branch, payload) {
  if (isMockMode()) {
    return mock.createEmployee(branch, payload);
  }

  const pool = await getPool(branch);
  const maNV = payload.MaNV || `${branch[0]}${String(Date.now()).slice(-4)}`;

  if (isEmployeeWriteProcMode()) {
    // SP mode (Phase 2): CRUD nhân viên qua proc local/central.
    const localProcs = localProcNamesByBranch();
    const centralProcs = centralProcNames();

    if (branch === "CENTRAL") {
      const targetBranch = String(payload.ChiNhanh || payload.branch || "")
        .trim()
        .toUpperCase();
      if (!["HUE", "SAIGON", "HANOI"].includes(targetBranch)) {
        throw new Error("ChiNhanh is required for CENTRAL and must be HUE/SAIGON/HANOI");
      }

      const rs = await pool
        .request()
        .input("MaNV", sql.VarChar(50), maNV)
        .input("HoTen", sql.NVarChar(120), payload.HoTen)
        .input("ChucVu", sql.NVarChar(80), payload.ChucVu)
        .input("ChiNhanh", sql.VarChar(10), targetBranch)
        .execute(centralProcs.createEmployee);
      return rs.recordset[0] || null;
    }

    const rs = await pool
      .request()
      .input("MaNV", sql.VarChar(50), maNV)
      .input("HoTen", sql.NVarChar(120), payload.HoTen)
      .input("ChucVu", sql.NVarChar(80), payload.ChucVu)
      .execute(localProcs.createEmployee);
    return rs.recordset[0] || null;
  }

  await pool
    .request()
    .input("MaNV", sql.VarChar(50), maNV)
    .input("ChiNhanh", sql.VarChar(10), branch)
    .input("HoTen", sql.NVarChar(120), payload.HoTen)
    .input("ChucVu", sql.NVarChar(80), payload.ChucVu).query(`
      INSERT INTO NhanVien (MaNV, HoTen, ChucVu, ChiNhanh)
      VALUES (@MaNV, @HoTen, @ChucVu, @ChiNhanh);
    `);

  const inserted = await pool
    .request()
    .input("MaNV", sql.VarChar(50), maNV)
    .query("SELECT TOP 1 * FROM NhanVien WHERE MaNV = @MaNV;");
  return inserted.recordset[0];
}

/*
 * Cập nhật nhân viên trong DB chi nhánh.
 * Truy vấn: UPDATE NhanVien SET ... WHERE MaNV = @MaNV; sau đó SELECT TOP 1 * trả về bản ghi.
 * Nếu mock mode thì dùng `mock.updateEmployee`.
 */
async function updateEmployee(branch, employeeId, payload) {
  if (isMockMode()) {
    return mock.updateEmployee(branch, employeeId, payload);
  }

  const pool = await getPool(branch);
  if (isEmployeeWriteProcMode()) {
    // SP mode (Phase 2): cập nhật nhân viên qua proc local/central.
    const localProcs = localProcNamesByBranch();
    const centralProcs = centralProcNames();

    if (branch === "CENTRAL") {
      const targetBranch = payload?.ChiNhanh
        ? String(payload.ChiNhanh).trim().toUpperCase()
        : null;

      const rs = await pool
        .request()
        .input("MaNV", sql.VarChar(50), employeeId)
        .input("HoTen", sql.NVarChar(120), payload.HoTen || null)
        .input("ChucVu", sql.NVarChar(80), payload.ChucVu || null)
        .input("ChiNhanh", sql.VarChar(10), targetBranch)
        .execute(centralProcs.updateEmployee);

      if (!rs.recordset.length) {
        throw new Error(`Employee ${employeeId} not found in CENTRAL`);
      }
      return rs.recordset[0];
    }

    const rs = await pool
      .request()
      .input("MaNV", sql.VarChar(50), employeeId)
      .input("HoTen", sql.NVarChar(120), payload.HoTen || null)
      .input("ChucVu", sql.NVarChar(80), payload.ChucVu || null)
      .execute(localProcs.updateEmployee);

    if (!rs.recordset.length) {
      throw new Error(`Employee ${employeeId} not found in ${branch}`);
    }
    return rs.recordset[0];
  }

  await pool
    .request()
    .input("MaNV", sql.VarChar(50), employeeId)
    .input("HoTen", sql.NVarChar(120), payload.HoTen || null)
    .input("ChucVu", sql.NVarChar(80), payload.ChucVu || null).query(`
      UPDATE NhanVien
      SET HoTen = COALESCE(@HoTen, HoTen),
          ChucVu = COALESCE(@ChucVu, ChucVu)
      WHERE MaNV = @MaNV;
    `);

  const updated = await pool
    .request()
    .input("MaNV", sql.VarChar(50), employeeId)
    .query("SELECT TOP 1 * FROM NhanVien WHERE MaNV = @MaNV;");

  if (!updated.recordset.length) {
    throw new Error(`Employee ${employeeId} not found in ${branch}`);
  }
  return updated.recordset[0];
}

/*
 * Xóa nhân viên trong DB chi nhánh.
 * Trình tự: SELECT TOP 1 * để kiểm tra tồn tại, rồi DELETE FROM NhanVien.
 * Nếu mock mode thì dùng `mock.deleteEmployee`.
 */
async function deleteEmployee(branch, employeeId) {
  if (isMockMode()) {
    return mock.deleteEmployee(branch, employeeId);
  }

  const pool = await getPool(branch);
  if (isEmployeeWriteProcMode()) {
    // SP mode (Phase 2): xóa nhân viên qua proc local/central.
    const localProcs = localProcNamesByBranch();
    const centralProcs = centralProcNames();

    const beforeDelete = await pool
      .request()
      .input("MaNV", sql.VarChar(50), employeeId)
      .query("SELECT TOP 1 * FROM NhanVien WHERE MaNV = @MaNV;");

    if (!beforeDelete.recordset.length) {
      throw new Error(`Employee ${employeeId} not found in ${branch}`);
    }

    if (branch === "CENTRAL") {
      await pool
        .request()
        .input("MaNV", sql.VarChar(50), employeeId)
        .execute(centralProcs.deleteEmployee);
    } else {
      await pool
        .request()
        .input("MaNV", sql.VarChar(50), employeeId)
        .execute(localProcs.deleteEmployee);
    }

    return beforeDelete.recordset[0];
  }

  const beforeDelete = await pool
    .request()
    .input("MaNV", sql.VarChar(50), employeeId)
    .query("SELECT TOP 1 * FROM NhanVien WHERE MaNV = @MaNV;");

  if (!beforeDelete.recordset.length) {
    throw new Error(`Employee ${employeeId} not found in ${branch}`);
  }

  await pool
    .request()
    .input("MaNV", sql.VarChar(50), employeeId)
    .query("DELETE FROM NhanVien WHERE MaNV = @MaNV;");

  return beforeDelete.recordset[0];
}

/*
 * Liệt kê tóm tắt hóa đơn cho chi nhánh. Nếu có cột `MaNV` sẽ JOIN `NhanVien`.
 * Truy vấn gồm JOIN giữa `HoaDon`, `ChiTietHoaDon` (và `NhanVien` nếu có).
 * Nếu mock mode thì gọi `mock.listInvoicesByBranch`.
 */
async function listInvoicesByBranch(branch) {
  if (isMockMode()) {
    return mock.listInvoicesByBranch(branch);
  }

  const pool = await getPool(branch);
  if (isReadProcMode()) {
    // SP mode (Phase 1): đọc danh sách hóa đơn qua proc local.
    const procs = localProcNamesByBranch();
    const result = await pool.request().execute(procs.listInvoices);
    return result.recordset;
  }

  const hasMaNV = await hasHoaDonEmployeeColumnWithPool(pool);
  const query = hasMaNV
    ? `
      SELECT
        hd.MaHD,
        SUM(CAST(ctd.SoLuong * ctd.DonGia AS DECIMAL(18,2))) AS TongTien,
        COUNT(ctd.MaSP) AS SoMon,
        hd.GhiChu,
        hd.NgayTao,
        hd.ChiNhanh,
        hd.MaNV,
        MAX(nv.HoTen) AS HoTenNhanVien
      FROM HoaDon hd
      LEFT JOIN ChiTietHoaDon ctd ON ctd.MaHD = hd.MaHD
      LEFT JOIN NhanVien nv ON nv.MaNV = hd.MaNV
      GROUP BY hd.MaHD, hd.GhiChu, hd.NgayTao, hd.ChiNhanh, hd.MaNV
      ORDER BY hd.NgayTao DESC;
    `
    : `
      SELECT
        hd.MaHD,
        SUM(CAST(ctd.SoLuong * ctd.DonGia AS DECIMAL(18,2))) AS TongTien,
        COUNT(ctd.MaSP) AS SoMon,
        hd.GhiChu,
        hd.NgayTao,
        hd.ChiNhanh,
        NULL AS MaNV,
        NULL AS HoTenNhanVien
      FROM HoaDon hd
      LEFT JOIN ChiTietHoaDon ctd ON ctd.MaHD = hd.MaHD
      GROUP BY hd.MaHD, hd.GhiChu, hd.NgayTao, hd.ChiNhanh
      ORDER BY hd.NgayTao DESC;
    `;
  const result = await pool.request().query(query);
  return result.recordset;
}

/*
 * Lấy chi tiết dòng hóa đơn theo `MaHD`.
 * Truy vấn: SELECT các cột từ `ChiTietHoaDon` và JOIN `HangHoa` để lấy `TenHang`.
 * Nếu mock mode thì gọi `mock.getInvoiceDetailsLocal`.
 */
async function getInvoiceDetails(branch, invoiceId) {
  if (isMockMode()) {
    return mock.getInvoiceDetailsLocal(branch, invoiceId);
  }

  const pool = await getPool(branch);
  if (isReadProcMode()) {
    // SP mode (Phase 1): đọc chi tiết hóa đơn qua proc local.
    const procs = localProcNamesByBranch();
    const result = await pool
      .request()
      .input("MaHD", sql.VarChar(50), invoiceId)
      .execute(procs.invoiceDetails);
    return result.recordset;
  }

  const result = await pool
    .request()
    .input("MaHD", sql.VarChar(50), invoiceId)
    .query(`
      SELECT 
        ctd.MaHD,
        ctd.MaSP,
        hh.TenHang,
        ctd.SoLuong,
        ctd.DonGia,
        CAST(ctd.SoLuong * ctd.DonGia AS DECIMAL(18,2)) AS ThanhTien
      FROM ChiTietHoaDon ctd
      LEFT JOIN HangHoa hh ON hh.MaSP = ctd.MaSP
      WHERE ctd.MaHD = @MaHD
    `);
  return result.recordset;
}

/*
 * Tạo hóa đơn (giao dịch) trong DB chi nhánh.
 * Bước chính khi không dùng mock:
 *  - BEGIN TRANSACTION
 *  - Nếu `HoaDon.MaNV` tồn tại thì validate nhân viên (ensureEmployeeExists)
 *  - Với mỗi item: SELECT `TenHang`,`Gia` từ `dbo.HangHoa`; nếu chưa có thì INSERT (upsert)
 *    Truy vấn: SELECT TOP 1 TenHang, Gia FROM dbo.HangHoa WHERE MaSP = @MaSP
 *  - INSERT INTO HoaDon (MaHD, GhiChu, NgayTao, ChiNhanh[, MaNV])
 *  - INSERT INTO ChiTietHoaDon cho từng item
 *  - Gọi `deductStock(...)` để trừ `TonKho`
 *  - COMMIT (ROLLBACK khi lỗi)
 */
async function createInvoice(payload) {
  if (isMockMode()) {
    return mock.createInvoiceLocal(payload);
  }

  if (isInvoiceProcEnabledForBranch(payload.branch)) {
    // SP mode (Phase 4): tạo hóa đơn nhiều dòng qua proc local theo từng chi nhánh bật cờ.
    const pool = await getPool(payload.branch);
    const procs = localProcNamesByBranch();
    const maHD = `HD_${Date.now()}`;
    const employeeId = String(payload.employeeId || "").trim();

    if (!employeeId) {
      throw new Error("employeeId is required for invoice creation");
    }

    const rawItems = Array.isArray(payload.items) && payload.items.length
      ? payload.items
      : [
          {
            productCode: payload.productCode,
            unitPrice: payload.unitPrice,
            quantity: payload.quantity,
          },
        ];

    const mergedByProduct = new Map();
    for (const rawItem of rawItems) {
      const productCode = String(rawItem?.productCode || "").trim();
      const quantity = Number(rawItem?.quantity || 0);
      const unitPrice = Number(rawItem?.unitPrice || 0);

      if (!productCode || quantity <= 0) {
        throw new Error("Each invoice item must include productCode and quantity > 0");
      }

      const existing = mergedByProduct.get(productCode);
      if (!existing) {
        mergedByProduct.set(productCode, {
          MaSP: productCode,
          SoLuong: quantity,
          DonGia: unitPrice > 0 ? unitPrice : null,
        });
        continue;
      }

      existing.SoLuong += quantity;
      if (existing.DonGia === null && unitPrice > 0) {
        existing.DonGia = unitPrice;
      }
    }

    const itemsPayload = Array.from(mergedByProduct.values());

    await pool
      .request()
      .input("MaHD", sql.VarChar(50), maHD)
      .input("MaNV", sql.VarChar(50), employeeId)
      .input("GhiChu", sql.NVarChar(255), String(payload.note || "").trim())
      .input("ChiNhanhLap", sql.VarChar(10), payload.branch)
      .input("ItemsJson", sql.NVarChar(sql.MAX), JSON.stringify(itemsPayload))
      .execute(procs.createInvoiceLines);

    const hasMaNV = await hasHoaDonEmployeeColumnWithPool(pool);
    const header = await pool
      .request()
      .input("MaHD", sql.VarChar(50), maHD)
      .query(
        hasMaNV
          ? `
            SELECT TOP 1 MaHD, GhiChu, NgayTao, ChiNhanh, MaNV
            FROM HoaDon
            WHERE MaHD = @MaHD;
          `
          : `
            SELECT TOP 1 MaHD, GhiChu, NgayTao, ChiNhanh, NULL AS MaNV
            FROM HoaDon
            WHERE MaHD = @MaHD;
          `,
      );

    const lines = await getInvoiceDetails(payload.branch, maHD);
    const totalAmount = lines.reduce((sum, line) => sum + Number(line.ThanhTien || 0), 0);
    const first = lines[0] || null;
    const hd = header.recordset[0] || {
      MaHD: maHD,
      GhiChu: String(payload.note || "").trim(),
      NgayTao: new Date(),
      ChiNhanh: payload.branch,
      MaNV: employeeId,
    };

    return {
      MaHD: hd.MaHD,
      TongTien: totalAmount,
      GhiChu: hd.GhiChu,
      ChiNhanh: hd.ChiNhanh,
      MaNV: hd.MaNV,
      items: lines.map((line) => ({
        productCode: line.MaSP,
        productName: line.TenHang,
        quantity: Number(line.SoLuong || 0),
        unitPrice: Number(line.DonGia || 0),
        totalAmount: Number(line.ThanhTien || 0),
      })),
      ...(first
        ? {
            MaSP: first.MaSP,
            SoLuong: Number(first.SoLuong || 0),
            TenHang: first.TenHang,
            DonGia: Number(first.DonGia || 0),
          }
        : {}),
      NgayTao: new Date(hd.NgayTao || new Date()).toISOString(),
    };
  }

  const pool = await getPool(payload.branch);
  const transaction = new sql.Transaction(pool);
  await transaction.begin();

  try {
    const hasMaNV = await hasHoaDonEmployeeColumnWithTransaction(transaction);
    const employeeId = String(payload.employeeId || "").trim() || null;
    const maHD = `HD_${Date.now()}`;
    const rawItems = Array.isArray(payload.items) && payload.items.length
      ? payload.items
      : [
          {
            productCode: payload.productCode,
            productName: payload.productName,
            unitPrice: payload.unitPrice,
            quantity: payload.quantity,
            totalAmount: payload.totalAmount,
          },
        ];

    const normalizedItems = [];
    for (const rawItem of rawItems) {
      const productCode = String(rawItem?.productCode || "").trim();
      const quantity = Number(rawItem?.quantity || 0);
      if (!productCode || quantity <= 0) {
        throw new Error("Each invoice item must include productCode and quantity > 0");
      }

      const productRs = await new sql.Request(transaction)
        .input("MaSP", sql.VarChar(50), productCode)
        .query(`
          SELECT TOP 1 TenHang, Gia
          FROM dbo.HangHoa
          WHERE MaSP = @MaSP;
        `);
      const product = productRs.recordset[0] || null;
      const unitPrice =
        Number(rawItem?.unitPrice || 0) > 0
          ? Number(rawItem.unitPrice)
          : Number(product?.Gia || 0);

      if (unitPrice <= 0) {
        throw new Error(
          `Cannot resolve unit price for product ${productCode}. Please input unitPrice or define Gia in HangHoa first.`,
        );
      }

      const productName =
        product?.TenHang || String(rawItem?.productName || "").trim() || productCode;

      await new sql.Request(transaction)
        .input("MaSP", sql.VarChar(50), productCode)
        .input("TenHang", sql.NVarChar(100), productName)
        .input("Gia", sql.Decimal(10, 2), unitPrice).query(`
          IF OBJECT_ID('dbo.HangHoa', 'U') IS NOT NULL
          BEGIN
            IF NOT EXISTS (SELECT 1 FROM dbo.HangHoa WHERE MaSP = @MaSP)
            BEGIN
              INSERT INTO dbo.HangHoa (MaSP, TenHang, Gia)
              VALUES (@MaSP, @TenHang, @Gia);
            END
          END
        `);

      normalizedItems.push({
        productCode,
        productName,
        quantity,
        unitPrice,
        totalAmount: Number(quantity || 0) * Number(unitPrice || 0),
      });
    }

    const mergedItemsByProduct = new Map();
    for (const item of normalizedItems) {
      const key = item.productCode;
      const existing = mergedItemsByProduct.get(key);
      if (!existing) {
        mergedItemsByProduct.set(key, { ...item });
        continue;
      }

      if (Number(existing.unitPrice || 0) !== Number(item.unitPrice || 0)) {
        throw new Error(
          `Duplicate product ${key} has different unit prices in the same invoice`,
        );
      }

      existing.quantity += Number(item.quantity || 0);
      existing.totalAmount =
        Number(existing.quantity || 0) * Number(existing.unitPrice || 0);
    }

    const mergedItems = Array.from(mergedItemsByProduct.values());

    const totalAmount = mergedItems.reduce(
      (sum, item) => sum + Number(item.totalAmount || 0),
      0,
    );

    // If HoaDon.MaNV exists, enforce that submitted employee belongs to that branch.
    if (hasMaNV) {
      await ensureEmployeeExists(transaction, payload.branch, employeeId);
    }

    if (hasMaNV) {
      await new sql.Request(transaction)
        .input("MaHD", sql.VarChar(50), maHD)
        .input("GhiChu", sql.NVarChar(255), payload.note)
        .input("ChiNhanh", sql.VarChar(10), payload.branch)
        .input("MaNV", sql.VarChar(50), employeeId)
        .query(`
          INSERT INTO HoaDon (MaHD, GhiChu, NgayTao, ChiNhanh, MaNV)
          VALUES (@MaHD, @GhiChu, GETDATE(), @ChiNhanh, @MaNV);
        `);
    } else {
      await new sql.Request(transaction)
        .input("MaHD", sql.VarChar(50), maHD)
        .input("GhiChu", sql.NVarChar(255), payload.note)
        .input("ChiNhanh", sql.VarChar(10), payload.branch)
        .query(`
          INSERT INTO HoaDon (MaHD, GhiChu, NgayTao, ChiNhanh)
          VALUES (@MaHD, @GhiChu, GETDATE(), @ChiNhanh);
        `);
    }

    for (const item of mergedItems) {
      await new sql.Request(transaction)
        .input("MaHD", sql.VarChar(50), maHD)
        .input("MaSP", sql.VarChar(50), item.productCode)
        .input("SoLuong", sql.Int, item.quantity)
        .input("DonGia", sql.Decimal(10, 2), item.unitPrice).query(`
          IF OBJECT_ID('dbo.ChiTietHoaDon', 'U') IS NOT NULL
          BEGIN
            INSERT INTO dbo.ChiTietHoaDon (MaHD, MaSP, SoLuong, DonGia)
            VALUES (@MaHD, @MaSP, @SoLuong, @DonGia);
          END
        `);

      await deductStock(
        transaction,
        payload.branch,
        item.productCode,
        item.quantity,
      );
    }

    await transaction.commit();

    const firstItem = mergedItems[0] || null;
    return {
      MaHD: maHD,
      TongTien: totalAmount,
      GhiChu: payload.note,
      ChiNhanh: payload.branch,
      MaNV: employeeId,
      items: mergedItems,
      ...(firstItem
        ? {
            MaSP: firstItem.productCode,
            SoLuong: firstItem.quantity,
            TenHang: firstItem.productName,
            DonGia: Number(firstItem.unitPrice || 0),
          }
        : {}),
      NgayTao: new Date().toISOString(),
    };
  } catch (error) {
    await transaction.rollback();
    throw error;
  }
}


async function updateInvoice(branch, invoiceId, payload) {
  if (isMockMode()) {
    return mock.updateInvoiceLocal(branch, invoiceId, payload);
  }

  const pool = await getPool(branch);
  const transaction = new sql.Transaction(pool);
  await transaction.begin();

  try {
    const hasMaNV = await hasHoaDonEmployeeColumnWithTransaction(transaction);
    const currentRs = await new sql.Request(transaction)
      .input("MaHD", sql.VarChar(50), invoiceId)
      .query(
        hasMaNV
          ? `
            SELECT
              hd.MaHD,
              hd.GhiChu,
              hd.NgayTao,
              hd.ChiNhanh,
              hd.MaNV,
              ctd.MaSP,
              ctd.SoLuong,
              ctd.DonGia,
              hh.TenHang
            FROM HoaDon hd
            INNER JOIN ChiTietHoaDon ctd ON ctd.MaHD = hd.MaHD
            LEFT JOIN HangHoa hh ON hh.MaSP = ctd.MaSP
            WHERE hd.MaHD = @MaHD;
          `
          : `
            SELECT
              hd.MaHD,
              hd.GhiChu,
              hd.NgayTao,
              hd.ChiNhanh,
              NULL AS MaNV,
              ctd.MaSP,
              ctd.SoLuong,
              ctd.DonGia,
              hh.TenHang
            FROM HoaDon hd
            INNER JOIN ChiTietHoaDon ctd ON ctd.MaHD = hd.MaHD
            LEFT JOIN HangHoa hh ON hh.MaSP = ctd.MaSP
            WHERE hd.MaHD = @MaHD;
          `,
      );

    if (!currentRs.recordset.length) {
      throw new Error(`Invoice ${invoiceId} not found in ${branch}`);
    }

    const current = currentRs.recordset[0];
    const nextMaSP = payload.productCode || current.MaSP;
    const nextSoLuong = payload.quantity > 0 ? payload.quantity : Number(current.SoLuong || 0);
    const nextProductRs = await new sql.Request(transaction)
      .input("MaSP", sql.VarChar(50), nextMaSP)
      .query(`
        SELECT TOP 1 TenHang, Gia
        FROM dbo.HangHoa
        WHERE MaSP = @MaSP;
      `);
    const nextProduct = nextProductRs.recordset[0] || null;
    const nextDonGia =
      payload.unitPrice > 0
        ? payload.unitPrice
        : payload.productCode && payload.productCode !== current.MaSP
          ? Number(nextProduct?.Gia || 0)
          : Number(current.DonGia || nextProduct?.Gia || 0);
    if (nextDonGia <= 0) {
      throw new Error(
        `Cannot resolve unit price for product ${nextMaSP}. Please input unitPrice or define Gia in HangHoa first.`,
      );
    }
    const nextTenHang =
      payload.productName || nextProduct?.TenHang || current.TenHang || nextMaSP;
    const nextGhiChu = payload.note !== null ? payload.note : current.GhiChu;
    const nextMaNV = payload.employeeId
      ? payload.employeeId
      : current.MaNV || null;

    if (hasMaNV) {
      await ensureEmployeeExists(transaction, branch, nextMaNV);
    }

    if (hasMaNV) {
      await new sql.Request(transaction)
        .input("MaHD", sql.VarChar(50), invoiceId)
        .input("GhiChu", sql.NVarChar(255), nextGhiChu)
        .input("MaNV", sql.VarChar(50), nextMaNV)
        .query(`
          UPDATE HoaDon
          SET GhiChu = @GhiChu,
              MaNV = @MaNV
          WHERE MaHD = @MaHD;
        `);
    } else {
      await new sql.Request(transaction)
        .input("MaHD", sql.VarChar(50), invoiceId)
        .input("GhiChu", sql.NVarChar(255), nextGhiChu)
        .query(`
          UPDATE HoaDon
          SET GhiChu = @GhiChu
          WHERE MaHD = @MaHD;
        `);
    }

    await new sql.Request(transaction)
      .input("MaSP", sql.VarChar(50), nextMaSP)
      .input("TenHang", sql.NVarChar(100), nextTenHang)
      .input("Gia", sql.Decimal(10, 2), nextDonGia).query(`
        IF OBJECT_ID('dbo.HangHoa', 'U') IS NOT NULL
        BEGIN
          IF EXISTS (SELECT 1 FROM dbo.HangHoa WHERE MaSP = @MaSP)
          BEGIN
            UPDATE dbo.HangHoa
            SET TenHang = @TenHang
            WHERE MaSP = @MaSP;
          END
          ELSE
          BEGIN
            INSERT INTO dbo.HangHoa (MaSP, TenHang, Gia)
            VALUES (@MaSP, @TenHang, @Gia);
          END
        END
      `);

    await new sql.Request(transaction)
      .input("MaHD", sql.VarChar(50), invoiceId)
      .input("MaSP", sql.VarChar(50), nextMaSP)
      .input("SoLuong", sql.Int, nextSoLuong)
      .input("DonGia", sql.Decimal(10, 2), nextDonGia).query(`
        IF OBJECT_ID('dbo.ChiTietHoaDon', 'U') IS NOT NULL
        BEGIN
          DELETE FROM dbo.ChiTietHoaDon WHERE MaHD = @MaHD;
          INSERT INTO dbo.ChiTietHoaDon (MaHD, MaSP, SoLuong, DonGia)
          VALUES (@MaHD, @MaSP, @SoLuong, @DonGia);
        END
      `);

    const currentQty = Number(current.SoLuong || 0);
    if (current.MaSP === nextMaSP) {
      const delta = nextSoLuong - currentQty;
      if (delta > 0) {
        await deductStock(transaction, branch, nextMaSP, delta);
      } else if (delta < 0) {
        await addStock(transaction, branch, nextMaSP, Math.abs(delta));
      }
    } else {
      await addStock(transaction, branch, current.MaSP, currentQty);
      await deductStock(transaction, branch, nextMaSP, nextSoLuong);
    }

    await transaction.commit();
    return {
      MaHD: invoiceId,
      MaSP: nextMaSP,
      SoLuong: nextSoLuong,
      TongTien: Number(nextSoLuong || 0) * Number(nextDonGia || 0),
      GhiChu: nextGhiChu,
      ChiNhanh: branch,
      MaNV: nextMaNV,
      TenHang: nextTenHang,
      DonGia: Number(nextDonGia || 0),
      NgayTao: current.NgayTao,
    };
  } catch (error) {
    await transaction.rollback();
    throw error;
  }
}


async function deleteInvoice(branch, invoiceId) {
  if (isMockMode()) {
    return mock.deleteInvoiceLocal(branch, invoiceId);
  }

  const pool = await getPool(branch);
  const transaction = new sql.Transaction(pool);
  await transaction.begin();

  try {
    const beforeDelete = await new sql.Request(transaction)
      .input("MaHD", sql.VarChar(50), invoiceId)
      .query("SELECT TOP 1 * FROM HoaDon WHERE MaHD = @MaHD;");

    if (!beforeDelete.recordset.length) {
      throw new Error(`Invoice ${invoiceId} not found in ${branch}`);
    }

    const detailsBeforeDelete = await new sql.Request(transaction)
      .input("MaHD", sql.VarChar(50), invoiceId)
      .query(`
        IF OBJECT_ID('dbo.ChiTietHoaDon', 'U') IS NOT NULL
        BEGIN
          SELECT MaSP, SoLuong FROM dbo.ChiTietHoaDon WHERE MaHD = @MaHD;
        END
      `);

    await new sql.Request(transaction)
      .input("MaHD", sql.VarChar(50), invoiceId).query(`
        IF OBJECT_ID('dbo.ChiTietHoaDon', 'U') IS NOT NULL
        BEGIN
          DELETE FROM dbo.ChiTietHoaDon WHERE MaHD = @MaHD;
        END
      `);

    await new sql.Request(transaction)
      .input("MaHD", sql.VarChar(50), invoiceId)
      .query("DELETE FROM HoaDon WHERE MaHD = @MaHD;");

    for (const detail of detailsBeforeDelete.recordset || []) {
      await addStock(
        transaction,
        branch,
        String(detail.MaSP || "").trim(),
        Number(detail.SoLuong || 0),
      );
    }

    await transaction.commit();
    return beforeDelete.recordset[0];
  } catch (error) {
    await transaction.rollback();
    throw error;
  }
}

/*
 * Liệt kê nhân viên từ tất cả chi nhánh bằng linked servers (từ CENTRAL).
 * Truy vấn: UNION ALL select từ bảng `NhanVien` trên các linked server.
 * Nếu mock mode thì dùng `mock.listAllEmployees()`.
 */
async function listAllEmployeesFromCentral() {
  if (isMockMode()) {
    return mock.listAllEmployees();
  }

  const linked = linkedServerNames();
  const pool = await getPool("CENTRAL");
  if (isReadProcMode()) {
    try {
      // VIEW mode (Phase 1): ưu tiên đọc view toàn cục đã dựng ở branch SQL.
      const viaView = await pool.request().query(`
        SELECT MaNV, HoTen, ChucVu, ChiNhanh
        FROM [${linked.HUE}].[Store_H].dbo.v_NhanVien_ToanQuoc;
      `);
      return viaView.recordset;
    } catch (_viewError) {
      // Fallback: giữ query linked table cũ khi view chưa sẵn sàng.
    }
  }

  const result = await pool.request().query(`
    SELECT * FROM [${linked.HUE}].[Store_H].dbo.NhanVien
    UNION ALL
    SELECT * FROM [${linked.SAIGON}].[Store_SG].dbo.NhanVien
    UNION ALL
    SELECT * FROM [${linked.HANOI}].[Store_HN].dbo.NhanVien;
  `);

  return result.recordset;
}

/*
 * Tổng hợp doanh thu toàn quốc bằng cách query `ChiTietHoaDon` trên linked servers.
 * Truy vấn: UNION ALL `ChiTietHoaDon` từ từng server, sau đó SUM theo branch.
 * Nếu mock mode thì dùng `mock.revenueReport()`.
 */
async function getNationalRevenue() {
  if (isMockMode()) {
    return mock.revenueReport();
  }

  const linked = linkedServerNames();
  const pool = await getPool("CENTRAL");
  if (isReadProcMode()) {
    try {
      // VIEW mode (Phase 1): ưu tiên tổng hợp doanh thu từ view toàn cục.
      const viewRs = await pool.request().query(`
        SELECT ChiNhanh AS BranchCode, SUM(ThanhTien) AS Revenue
        FROM [${linked.HUE}].[Store_H].dbo.v_HoaDonChiTiet_ToanQuoc
        GROUP BY ChiNhanh;
      `);

      const byBranch = viewRs.recordset.map((row) => ({
        branch: row.BranchCode,
        revenue: Number(row.Revenue || 0),
      }));
      const nationalRevenue = byBranch.reduce((sum, item) => sum + item.revenue, 0);

      return {
        mode: "SQL_SERVER",
        generatedAt: new Date().toISOString(),
        byBranch,
        nationalRevenue,
      };
    } catch (_viewError) {
      // Fallback: giữ query linked table cũ khi view chưa sẵn sàng.
    }
  }

  const result = await pool.request().query(`
    SELECT BranchCode, SUM(LineAmount) AS Revenue
    FROM (
      SELECT 'HUE' AS BranchCode, CAST(ctd.SoLuong * ctd.DonGia AS DECIMAL(18,2)) AS LineAmount
      FROM [${linked.HUE}].[Store_H].dbo.ChiTietHoaDon ctd
      UNION ALL
      SELECT 'SAIGON' AS BranchCode, CAST(ctd.SoLuong * ctd.DonGia AS DECIMAL(18,2)) AS LineAmount
      FROM [${linked.SAIGON}].[Store_SG].dbo.ChiTietHoaDon ctd
      UNION ALL
      SELECT 'HANOI' AS BranchCode, CAST(ctd.SoLuong * ctd.DonGia AS DECIMAL(18,2)) AS LineAmount
      FROM [${linked.HANOI}].[Store_HN].dbo.ChiTietHoaDon ctd
    ) x
    GROUP BY BranchCode;
  `);

  const byBranch = result.recordset.map((row) => ({
    branch: row.BranchCode,
    revenue: Number(row.Revenue || 0),
  }));
  const nationalRevenue = byBranch.reduce((sum, item) => sum + item.revenue, 0);

  return {
    mode: "SQL_SERVER",
    generatedAt: new Date().toISOString(),
    byBranch,
    nationalRevenue,
  };
}
// gọi các proc tổng hợp analytics từ CENTRAL, trả về object tổng hợp gồm doanh thu theo ngày, theo tuần, top nhân viên, top sản phẩm, so sánh doanh thu tuần.
async function getCentralAnalyticsOverview(sourceBranch) {
  if (isMockMode()) {
    return {
      mode: "MOCK",
      analyticsSourceBranch: normalizeAnalyticsBranch(sourceBranch),
      generatedAt: new Date().toISOString(),
      daily: [],
      weekly: [],
      topEmployees: [],
      topProducts: [],
      weekCompare: [],
    };
  }

  const pool = await getPool("CENTRAL");
  const analyticsSourceBranch = normalizeAnalyticsBranch(
    sourceBranch || process.env.ANALYTICS_SP_SOURCE_BRANCH || "HUE",
  );

  const [dailyRows, weeklyRows, topEmployeeRows, topProductRows, compareRows] = await Promise.all([
    executeAnalyticsProcFromCentral(pool, analyticsSourceBranch, "usp_DoanhThuVaSoDon_TheoNgay"),
    executeAnalyticsProcFromCentral(pool, analyticsSourceBranch, "usp_DoanhThuVaSoDon_TheoTuan"),
    executeAnalyticsProcFromCentral(pool, analyticsSourceBranch, "usp_NhanVienBanTotNhatTuan"),
    executeAnalyticsProcFromCentral(pool, analyticsSourceBranch, "usp_SanPhamBanChayNhat_MoiChiNhanh"),
    executeAnalyticsProcFromCentral(pool, analyticsSourceBranch, "usp_SoSanhDoanhThuTuan"),
  ]);

  const daily = dailyRows.map((row) => ({
    branch: row.ChiNhanh,
    date: row.Ngay,
    totalOrders: Number(row.TongSoDonHang || 0),
    totalRevenue: Number(row.TongDoanhThu || 0),
  }));

  const weekly = weeklyRows.map((row) => ({
    branch: row.ChiNhanh,
    year: Number(row.Nam || 0),
    week: Number(row.TuanTrongNam || 0),
    totalOrders: Number(row.TongSoDonHang || 0),
    totalRevenue: Number(row.TongDoanhThu || 0),
  }));

  const topEmployees = topEmployeeRows.map((row) => ({
    branch: row.ChiNhanh,
    employeeId: row.MaNV,
    employeeName: row.HoTen,
    totalRevenue: Number(row.TongDoanhThu || 0),
  }));

  const topProducts = topProductRows.map((row) => ({
    branch: row.ChiNhanh,
    productCode: row.MaSP,
    productName: row.TenHang,
    totalSold: Number(row.TongSoLuongBan || 0),
  }));

  const weekCompare = compareRows.map((row) => ({
    branch: row.ChiNhanh,
    thisWeekRevenue: Number(row.DoanhThuTuanNay || 0),
    lastWeekRevenue: Number(row.DoanhThuTuanTruoc || 0),
  }));

  return {
    mode: "SQL_SERVER",
    analyticsSourceBranch,
    generatedAt: new Date().toISOString(),
    daily,
    weekly,
    topEmployees,
    topProducts,
    weekCompare,
  };
}

/*
 * Liệt kê tồn kho cho chi nhánh. Nếu có `productCode` trả 1 dòng, nếu không trả tất cả `TonKho` của chi nhánh.
 * Truy vấn:
 *  - SELECT MaSP, SoLuongTon FROM TonKho WHERE ChiNhanh = @ChiNhanh ORDER BY MaSP
 *  - SELECT MaSP, SoLuongTon FROM TonKho WHERE ChiNhanh = @ChiNhanh AND MaSP = @MaSP
 * Nếu mock mode thì gọi hàm mock tương ứng.
 */
async function listInventory(branch, productCode) {
  if (isMockMode()) {
    if (!productCode) {
      return mock.listInventory(branch);
    }
    return mock.getInventory(branch, productCode);
  }

  const pool = await getPool(branch);
  if (isReadProcMode()) {
    // SP mode (Phase 1): đọc tồn kho qua proc local.
    const procs = localProcNamesByBranch();
    if (!productCode) {
      const rows = await pool.request().execute(procs.listInventory);
      return rows.recordset.map((row) => ({
        branch,
        productCode: row.MaSP,
        quantity: Number(row.SoLuongTon || 0),
      }));
    }

    const result = await pool
      .request()
      .input("MaSP", sql.VarChar(50), productCode)
      .execute(procs.inventoryByCode);

    const row = result.recordset[0] || { MaSP: productCode, SoLuongTon: 0 };
    return {
      branch,
      productCode: row.MaSP,
      quantity: Number(row.SoLuongTon || 0),
    };
  }

  if (!productCode) {
    const rows = await pool
      .request()
      .input("ChiNhanh", sql.VarChar(10), branch)
      .query(
        "SELECT MaSP, SoLuongTon FROM TonKho WHERE ChiNhanh = @ChiNhanh ORDER BY MaSP;",
      );
    return rows.recordset.map((row) => ({
      branch,
      productCode: row.MaSP,
      quantity: Number(row.SoLuongTon || 0),
    }));
  }

  const result = await pool
    .request()
    .input("ChiNhanh", sql.VarChar(10), branch)
    .input("MaSP", sql.VarChar(50), productCode)
    .query(
      "SELECT MaSP, SoLuongTon FROM TonKho WHERE ChiNhanh = @ChiNhanh AND MaSP = @MaSP;",
    );

  const row = result.recordset[0] || { MaSP: productCode, SoLuongTon: 0 };
  return {
    branch,
    productCode: row.MaSP,
    quantity: Number(row.SoLuongTon || 0),
  };
}

/*
 * Liệt kê sản phẩm từ bảng `HangHoa` ở CENTRAL.
 * Truy vấn: SELECT MaSP AS productCode, TenHang AS productName, Gia AS unitPrice FROM dbo.HangHoa
 * Nếu mock mode thì dùng `mock.listProducts()`.
 */
async function listProducts() {
  if (isMockMode()) {
    return mock.listProducts();
  }

  const pool = await getPool("CENTRAL");
  if (isReadProcMode()) {
    // SP mode (Phase 1): đọc danh mục hàng hóa qua proc central.
    const result = await pool.request().execute("dbo.usp_Central_DanhSachHangHoa");
    return result.recordset;
  }

  const result = await pool.request().query(`
    IF OBJECT_ID('dbo.HangHoa', 'U') IS NOT NULL
      SELECT MaSP AS productCode, TenHang AS productName, Gia AS unitPrice FROM dbo.HangHoa
    ELSE
      SELECT '' AS productCode, '' AS productName, 0 AS unitPrice WHERE 1=0
  `);
  return result.recordset;
}

/*
 * Tạo sản phẩm ở CENTRAL (`HangHoa`) và đảm bảo có `HangHoa` + `TonKho` trên mỗi chi nhánh (qua linked servers).
 * Thực hiện:
 *  - INSERT vào `dbo.HangHoa` trên CENTRAL nếu chưa có
 *  - Với mỗi branch: INSERT vào [linkedServer].[db].dbo.HangHoa nếu chưa có
 *  - Với mỗi branch: INSERT vào [linkedServer].[db].dbo.TonKho (MaSP, 0, ChiNhanh) nếu chưa có
 * Nếu mock mode thì dùng `mock.createProduct(payload)`.
 */
async function createProduct(payload) {
  if (isMockMode()) {
    return mock.createProduct(payload);
  }

  const pool = await getPool("CENTRAL");
  const centralProcs = centralProcNames();
  const code = String(payload.productCode || "").trim();
  const name = String(payload.productName || code).trim();
  const price = Number(payload.unitPrice || 0);

  // CHỈ CẦN GỌI ĐÚNG SP Ở CENTRAL. REPLICATION SẼ TỰ ĐẨY XUỐNG CHI NHÁNH!
  await pool.request()
    .input("MaSP", sql.VarChar(50), code)
    .input("TenHang", sql.NVarChar(100), name)
    .input("Gia", sql.Decimal(10, 2), price)
    .execute(centralProcs.createProduct);

  return { productCode: code, productName: name, unitPrice: price };
}
/*
 * Cập nhật thông tin sản phẩm ở CENTRAL (`HangHoa`).
 * Truy vấn: UPDATE dbo.HangHoa SET ... WHERE MaSP = @MaSP
 * Nếu mock mode thì dùng `mock.updateProduct`.
 */
async function updateProduct(productCode, payload) {
  if (isMockMode()) {
    return mock.updateProduct(productCode, payload);
  }
  const pool = await getPool("CENTRAL");
  const centralProcs = centralProcNames();
  const code = String(productCode || "").trim();

  await pool.request()
    .input("MaSP", sql.VarChar(50), code)
    .input(
      "TenHang",
      sql.NVarChar(100),
      payload.productName !== undefined ? String(payload.productName).trim() : null,
    )
    .input(
      "Gia",
      sql.Decimal(10, 2),
      payload.unitPrice !== undefined ? Number(payload.unitPrice || 0) : null,
    )
    .execute(centralProcs.updateProduct);

  return { updated: true, productCode: code };
}

/*
 * Xóa sản phẩm khỏi CENTRAL (`HangHoa`).
 * Truy vấn: DELETE FROM dbo.HangHoa WHERE MaSP = @MaSP
  * Đồng thời xóa sản phẩm khỏi tất cả chi nhánh qua linked servers:
 */
async function deleteProduct(productCode) {
  if (isMockMode()) {
    return mock.deleteProduct(productCode);
  }
  
  const pool = await getPool("CENTRAL");
  const centralProcs = centralProcNames();
  const code = String(productCode || "").trim();

  // Khai báo chính xác tên Linked Server (theo ảnh SSMS) và tên Database của 3 miền
  const branchTargets = [
    { code: "HUE", server: "HUE_SERVER", db: "Store_H" },
    { code: "SAIGON", server: "SG_SERVER", db: "Store_SG" },
    { code: "HANOI", server: "HN_SERVER", db: "Store_HN" }
  ];

  
  for (const target of branchTargets) {
    await pool.request()
      .input("MaSP", sql.VarChar(50), code)
      .query(`
     
        IF EXISTS (SELECT 1 FROM [${target.server}].[${target.db}].dbo.ChiTietHoaDon WHERE MaSP = @MaSP)
        BEGIN
            THROW 50002, N'Lỗi: Sản phẩm đã phát sinh hóa đơn tại chi nhánh ${target.code}, không thể xóa!', 1;
        END

        
        IF EXISTS (SELECT 1 FROM [${target.server}].[${target.db}].dbo.HangHoa WHERE MaSP = @MaSP)
        BEGIN
          DELETE FROM [${target.server}].[${target.db}].dbo.TonKho WHERE MaSP = @MaSP;
          DELETE FROM [${target.server}].[${target.db}].dbo.HangHoa WHERE MaSP = @MaSP;
        END
      `);
  }

 
  await pool.request()
    .input("MaSP", sql.VarChar(50), code)
    .execute(centralProcs.deleteProduct); 

  return { deleted: true, productCode: code };
}
/*
 * Lấy thông tin sản phẩm (MaSP, TenHang, Gia) từ `HangHoa` của chi nhánh.
 * Truy vấn: SELECT TOP 1 MaSP, TenHang, Gia FROM HangHoa WHERE MaSP = @MaSP
 * Nếu mock mode thì dùng `mock.getProductByCode`.
 */
async function getProductByCode(branch, productCode) {
  if (isMockMode()) {
    return mock.getProductByCode(branch, productCode);
  }

  const code = String(productCode || "").trim();
  if (!code) {
    throw new Error("productCode is required");
  }

  if (isReadProcMode()) {
    if (branch === "CENTRAL") {
      // SP mode (Phase 1): đọc hàng hóa theo mã từ central proc.
      const centralPool = await getPool("CENTRAL");
      const rs = await centralPool
        .request()
        .input("MaSP", sql.VarChar(50), code)
        .execute("dbo.usp_Central_HangHoaTheoMaSP");

      const row = rs.recordset[0] || null;
      if (!row) {
        return null;
      }

      return {
        branch,
        productCode: row.MaSP,
        productName: row.TenHang,
        unitPrice: Number(row.Gia || 0),
      };
    }

    const pool = await getPool(branch);
    const procs = localProcNamesByBranch();
    // SP mode (Phase 1): đọc hàng hóa theo mã từ proc local của chi nhánh.
    const rs = await pool
      .request()
      .input("MaSP", sql.VarChar(50), code)
      .execute(procs.productByCode);

    const row = rs.recordset[0] || null;
    if (!row) {
      return null;
    }

    return {
      branch,
      productCode: row.MaSP,
      productName: row.TenHang,
      unitPrice: Number(row.Gia || 0),
    };
  }

  const pool = await getPool(branch);
  const rs = await pool
    .request()
    .input("MaSP", sql.VarChar(50), code)
    .query(`
      SELECT TOP 1 MaSP, TenHang, Gia
      FROM HangHoa
      WHERE MaSP = @MaSP;
    `);

  const row = rs.recordset[0] || null;
  if (!row) {
    return null;
  }

  return {
    branch,
    productCode: row.MaSP,
    productName: row.TenHang,
    unitPrice: Number(row.Gia || 0),
  };
}

/*
 * Tạo một dòng tồn kho (`TonKho`) cho sản phẩm ở chi nhánh.
 * Bước:
 *  - Kiểm tra sản phẩm tồn tại trong `HangHoa` của chi nhánh
 *  - INSERT INTO `TonKho` nếu chưa có
 *  - Trả về dòng vừa tạo
 * Nếu mock mode thì dùng `mock.createInventoryItem`.
 */
async function createInventoryItem(branch, payload) {
  if (isMockMode()) {
    return mock.createInventoryItem(branch, payload);
  }

  const pool = await getPool(branch);
  const productCode = String(payload.productCode || "").trim();

  const productCheck = await pool
    .request()
    .input("MaSP", sql.VarChar(50), productCode)
    .query("SELECT TOP 1 MaSP FROM HangHoa WHERE MaSP = @MaSP;");

  if (!productCheck.recordset.length) {
    throw new Error(
      `Product ${productCode} not found in ${branch}. Please create product first from Central product management.`,
    );
  }

  await pool
    .request()
    .input("ChiNhanh", sql.VarChar(10), branch)
    .input("MaSP", sql.VarChar(50), productCode)
    .input("SoLuongTon", sql.Int, payload.quantity).query(`
      IF EXISTS (SELECT 1 FROM TonKho WHERE MaSP = @MaSP AND ChiNhanh = @ChiNhanh)
      BEGIN
        THROW 50000, 'Inventory item already exists in this branch', 1;
      END

      INSERT INTO TonKho (MaSP, SoLuongTon, ChiNhanh)
      VALUES (@MaSP, @SoLuongTon, @ChiNhanh);
    `);

  const inserted = await pool
    .request()
    .input("ChiNhanh", sql.VarChar(10), branch)
    .input("MaSP", sql.VarChar(50), productCode)
    .query(
      "SELECT TOP 1 MaSP, SoLuongTon FROM TonKho WHERE ChiNhanh = @ChiNhanh AND MaSP = @MaSP;",
    );

  return {
    branch,
    productCode: inserted.recordset[0].MaSP,
    quantity: Number(inserted.recordset[0].SoLuongTon || 0),
  };
}

/*
 * Cập nhật số lượng tồn cho sản phẩm ở chi nhánh.
 * Truy vấn: UPDATE TonKho SET SoLuongTon = @SoLuongTon WHERE ChiNhanh = @ChiNhanh AND MaSP = @MaSP
 * Sau đó SELECT TOP 1 để trả về dòng cập nhật. Nếu mock mode thì dùng `mock.updateInventoryItem`.
 */
async function updateInventoryItem(branch, productCode, quantity) {
  if (isMockMode()) {
    return mock.updateInventoryItem(branch, productCode, quantity);
  }

  const pool = await getPool(branch);
  if (isInventoryImportProcMode()) {
    // SP mode (Phase 3+): cập nhật tồn kho qua proc tổng quát cho cả tăng/giảm.
    const procs = localProcNamesByBranch();
    const targetQty = Number(quantity || 0);
    const updatedRs = await pool
      .request()
      .input("MaSP", sql.VarChar(50), productCode)
      .input("SoLuongTonMoi", sql.Int, targetQty)
      .input("ChiNhanhLap", sql.VarChar(10), branch)
      .execute(procs.updateInventory);

    const updated = (updatedRs.recordset || [])[0] || null;
    if (!updated) {
      throw new Error(`Product ${productCode} not found in ${branch}`);
    }

    return {
      branch,
      productCode: updated.MaSP,
      quantity: Number(updated.SoLuongTon || 0),
    };
  }

  await pool
    .request()
    .input("ChiNhanh", sql.VarChar(10), branch)
    .input("MaSP", sql.VarChar(50), productCode)
    .input("SoLuongTon", sql.Int, quantity).query(`
      UPDATE TonKho
      SET SoLuongTon = @SoLuongTon
      WHERE ChiNhanh = @ChiNhanh AND MaSP = @MaSP;
    `);

  const updated = await pool
    .request()
    .input("ChiNhanh", sql.VarChar(10), branch)
    .input("MaSP", sql.VarChar(50), productCode)
    .query(
      "SELECT TOP 1 MaSP, SoLuongTon FROM TonKho WHERE ChiNhanh = @ChiNhanh AND MaSP = @MaSP;",
    );

  if (!updated.recordset.length) {
    throw new Error(`Product ${productCode} not found in ${branch}`);
  }
  return {
    branch,
    productCode: updated.recordset[0].MaSP,
    quantity: Number(updated.recordset[0].SoLuongTon || 0),
  };
}

/*
 * Xóa một dòng tồn kho khỏi chi nhánh.
 * Trình tự: SELECT TOP 1 để kiểm tra, sau đó DELETE FROM TonKho.
 * Trả về dữ liệu đã xóa. Nếu mock mode thì dùng `mock.deleteInventoryItem`.
 */
async function deleteInventoryItem(branch, productCode) {
  if (isMockMode()) {
    return mock.deleteInventoryItem(branch, productCode);
  }

  const pool = await getPool(branch);
  const beforeDelete = await pool
    .request()
    .input("ChiNhanh", sql.VarChar(10), branch)
    .input("MaSP", sql.VarChar(50), productCode)
    .query(
      "SELECT TOP 1 MaSP, SoLuongTon FROM TonKho WHERE ChiNhanh = @ChiNhanh AND MaSP = @MaSP;",
    );

  if (!beforeDelete.recordset.length) {
    throw new Error(`Product ${productCode} not found in ${branch}`);
  }

  await pool
    .request()
    .input("ChiNhanh", sql.VarChar(10), branch)
    .input("MaSP", sql.VarChar(50), productCode)
    .query("DELETE FROM TonKho WHERE ChiNhanh = @ChiNhanh AND MaSP = @MaSP;");

  return {
    branch,
    productCode: beforeDelete.recordset[0].MaSP,
    quantity: Number(beforeDelete.recordset[0].SoLuongTon || 0),
  };
}

/*
 * Tạo số liệu cho dashboard chi nhánh bằng các truy vấn tổng hợp:
 *  - COUNT nhân viên, COUNT hóa đơn
 *  - SUM doanh thu từ `ChiTietHoaDon`
 *  - SUM tổng tồn (`TonKho`) và đếm sản phẩm tồn thấp
 * Thực thi nhiều SELECT song song và trả về kết quả tổng hợp.
 * Nếu mock mode thì dùng `mock.branchDashboard`.
 */
async function getBranchDashboard(branch) {
  if (isMockMode()) {
    return mock.branchDashboard(branch);
  }

  const pool = await getPool(branch);
  if (isReadProcMode()) {
    // SP mode (Phase 1+): dashboard branch dùng proc local thay cho query trực tiếp.
    const procs = localProcNamesByBranch();
    const [summaryRs, revenueRs, topStockRs] = await Promise.all([
      //gọi SP tổng hợp summary (employeeCount, invoiceCount, revenue, totalStockUnits, lowStockProducts)
      pool.request().execute(procs.dashboardSummary),
      pool.request().execute(procs.dashboardRevenue7d),
      pool.request().input("TopN", sql.Int, 8).execute(procs.dashboardTopStock),
    ]);

    const summary = (summaryRs.recordset || [])[0] || {};
    const sevenDayRevenue = {};
    for (const row of revenueRs.recordset || []) {
      const key = new Date(row.Ngay || Date.now()).toISOString().slice(0, 10);
      sevenDayRevenue[key] = Number(row.DoanhThu || 0);
    }

    const topStockByProduct = (topStockRs.recordset || []).map((row) => ({
      productCode: row.MaSP,
      productName: row.TenHang,
      quantity: Number(row.SoLuongTon || 0),
    }));

    return {
      mode: "SQL_SERVER",
      branch,
      employeeCount: Number(summary.employeeCount || 0),
      invoiceCount: Number(summary.invoiceCount || 0),
      revenue: Number(summary.revenue || 0),
      totalStockUnits: Number(summary.totalStockUnits || 0),
      lowStockProducts: Number(summary.lowStockProducts || 0),
      sevenDayRevenue,
      topStockByProduct,
      generatedAt: new Date().toISOString(),
    };
  }

  const [employeesRs, invoicesRs, revenueRs, stockRs] = await Promise.all([
    pool.request().query("SELECT COUNT(1) AS count FROM NhanVien;"),
    pool.request().query("SELECT COUNT(1) AS count FROM HoaDon;"),
    pool
      .request()
      .query(
        "SELECT ISNULL(SUM(CAST(SoLuong * DonGia AS DECIMAL(18,2))), 0) AS revenue FROM ChiTietHoaDon;",
      ),
    pool
      .request()
      .query(
        "SELECT ISNULL(SUM(SoLuongTon), 0) AS totalStockUnits, SUM(CASE WHEN SoLuongTon < 50 THEN 1 ELSE 0 END) AS lowStockProducts FROM TonKho;",
      ),
  ]);

  return {
    mode: "SQL_SERVER",
    branch,
    employeeCount: Number((employeesRs.recordset[0] || {}).count || 0),
    invoiceCount: Number((invoicesRs.recordset[0] || {}).count || 0),
    revenue: Number((revenueRs.recordset[0] || {}).revenue || 0),
    totalStockUnits: Number((stockRs.recordset[0] || {}).totalStockUnits || 0),
    lowStockProducts: Number(
      (stockRs.recordset[0] || {}).lowStockProducts || 0,
    ),
    generatedAt: new Date().toISOString(),
  };
}

/*
 * Thực hiện chuyển tồn phân tán giữa hai chi nhánh qua CENTRAL và linked servers.
 * Bước khi không dùng mock:
 *  - Đọc dòng `TonKho` nguồn và đích từ linked servers
 *  - Kiểm tra số lượng đủ
 *  - Thực thi giao dịch phân tán (BEGIN DISTRIBUTED TRANSACTION) để UPDATE cả hai bên
 *  - Bắt lỗi liên quan linked-server/MSDTC và trả về gợi ý
 *  - Đọc lại lượng sau chuyển và trả before/after
 * Nếu mock mode thì dùng `mock.transferStock`.
 */
async function transferStockDistributed(payload) {
  if (isMockMode()) {
    return mock.transferStock(payload);
  }

  const linked = linkedServerNames();
  const fromServer = linked[payload.fromBranch];
  const toServer = linked[payload.toBranch];
  const fromDb = dbNameByBranch(payload.fromBranch);
  const toDb = dbNameByBranch(payload.toBranch);

  if (!fromServer || !toServer) {
    throw new Error("Cannot resolve linked server names for transfer");
  }

  const pool = await getPool("CENTRAL");

  const beforeFrom = await pool
    .request()
    .input("MaSP", sql.VarChar(50), payload.productCode)
    .input("FromBranch", sql.VarChar(10), payload.fromBranch)
    .query(
      `SELECT TOP 1 SoLuongTon FROM [${fromServer}].[${fromDb}].dbo.TonKho WHERE MaSP = @MaSP AND ChiNhanh = @FromBranch;`,
    );

  const beforeTo = await pool
    .request()
    .input("MaSP", sql.VarChar(50), payload.productCode)
    .input("ToBranch", sql.VarChar(10), payload.toBranch)
    .query(
      `SELECT TOP 1 SoLuongTon FROM [${toServer}].[${toDb}].dbo.TonKho WHERE MaSP = @MaSP AND ChiNhanh = @ToBranch;`,
    );

  if (!beforeFrom.recordset.length) {
    throw new Error(
      `Product ${payload.productCode} not found in source inventory ${payload.fromBranch}`,
    );
  }
  if (!beforeTo.recordset.length) {
    throw new Error(
      `Product ${payload.productCode} not found in destination inventory ${payload.toBranch}`,
    );
  }

  const fromQty = Number((beforeFrom.recordset[0] || {}).SoLuongTon || 0);
  if (fromQty < payload.quantity) {
    throw new Error(
      `Insufficient stock in ${payload.fromBranch}: current ${fromQty}, requested ${payload.quantity}`,
    );
  }

  try {
    await pool
      .request()
      .input("MaSP", sql.VarChar(50), payload.productCode)
      .input("SoLuong", sql.Int, payload.quantity)
      .input("FromBranch", sql.VarChar(10), payload.fromBranch)
      .input("ToBranch", sql.VarChar(10), payload.toBranch).query(`
        SET XACT_ABORT ON;
        BEGIN TRY
          BEGIN DISTRIBUTED TRANSACTION;
            UPDATE [${fromServer}].[${fromDb}].dbo.TonKho
            SET SoLuongTon = SoLuongTon - @SoLuong
            WHERE MaSP = @MaSP AND ChiNhanh = @FromBranch;

            IF @@ROWCOUNT = 0
              THROW 50001, 'Source row not updated on linked server', 1;

            UPDATE [${toServer}].[${toDb}].dbo.TonKho
            SET SoLuongTon = SoLuongTon + @SoLuong
            WHERE MaSP = @MaSP AND ChiNhanh = @ToBranch;

            IF @@ROWCOUNT = 0
              THROW 50002, 'Destination row not updated on linked server', 1;

          COMMIT TRANSACTION;
        END TRY
        BEGIN CATCH
          IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
          THROW;
        END CATCH;
      `);
  } catch (error) {
    const message = String(error.message || "");
    const linkedHints =
      message.toLowerCase().includes("distributed transaction") ||
      message.toLowerCase().includes("msdtc") ||
      message.toLowerCase().includes("linked server");

    if (linkedHints) {
      throw new Error(
        `Transfer failed due to linked-server/distributed-transaction setup. Check MSDTC (Network DTC Access + Inbound/Outbound) on all SQL hosts and enable RPC OUT for linked servers (${fromServer}, ${toServer}). Original error: ${message}`,
      );
    }
    throw error;
  }

  const afterFrom = await pool
    .request()
    .input("MaSP", sql.VarChar(50), payload.productCode)
    .input("FromBranch", sql.VarChar(10), payload.fromBranch)
    .query(
      `SELECT TOP 1 SoLuongTon FROM [${fromServer}].[${fromDb}].dbo.TonKho WHERE MaSP = @MaSP AND ChiNhanh = @FromBranch;`,
    );

  const afterTo = await pool
    .request()
    .input("MaSP", sql.VarChar(50), payload.productCode)
    .input("ToBranch", sql.VarChar(10), payload.toBranch)
    .query(
      `SELECT TOP 1 SoLuongTon FROM [${toServer}].[${toDb}].dbo.TonKho WHERE MaSP = @MaSP AND ChiNhanh = @ToBranch;`,
    );

  return {
    mode: "SQL_SERVER",
    status: "COMMITTED",
    transactionType: "BEGIN DISTRIBUTED TRANSACTION",
    productCode: payload.productCode,
    quantity: payload.quantity,
    fromBranch: payload.fromBranch,
    toBranch: payload.toBranch,
    before: {
      from: fromQty,
      to: Number((beforeTo.recordset[0] || {}).SoLuongTon || 0),
    },
    after: {
      from: Number((afterFrom.recordset[0] || {}).SoLuongTon || 0),
      to: Number((afterTo.recordset[0] || {}).SoLuongTon || 0),
    },
    committedAt: new Date().toISOString(),
  };
}

module.exports = {
  createEmployee,
  updateEmployee,
  deleteEmployee,
  listEmployeesByBranch,
  listInvoicesByBranch,
  getInvoiceDetails,
  createInvoice,
  updateInvoice,
  deleteInvoice,
  listAllEmployeesFromCentral,
  getNationalRevenue,
  getCentralAnalyticsOverview,
  transferStockDistributed,
  listInventory,
  listProducts,
  createProduct,
  updateProduct,
  deleteProduct,
  getProductByCode,
  createInventoryItem,
  updateInventoryItem,
  deleteInventoryItem,
  getBranchDashboard,
};
