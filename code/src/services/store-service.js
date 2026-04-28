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

// Các tên proc local/central được định nghĩa tĩnh ở đây để dễ quản lý. 
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
// Tên proc ở CENTRAL để thao tác hàng hóa 
function centralProcNames() {
  return {
    createProduct: "dbo.usp_Central_ThemHangHoaMoi",
    updateProduct: "dbo.usp_Central_CapNhatHangHoa",
    deleteProduct: "dbo.usp_Central_XoaHangHoa",
  };
} 
// Tên proc DÙNG CHUNG cho các bảng nhân bản toàn phần 
function commonProcNames() {
  return {
    listProducts: "dbo.usp_Chung_DanhSachHangHoa",
    productByCode: "dbo.usp_Chung_HangHoaTheoMaSP",
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
  // 1. Lấy danh sách tên Linked Server (vd: HUE_SERVER, SG_SERVER...)
  const linked = linkedServerNames();
  
  // 2. Trả về đúng tên Server và Database của chi nhánh đó
  if (branch === "HUE") {
    return { server: linked.HUE, db: dbNameByBranch("HUE") };     // vd: { server: 'HUE_SERVER', db: 'Store_H' }
  }
  if (branch === "SAIGON") {
    return { server: linked.SAIGON, db: dbNameByBranch("SAIGON") }; // vd: { server: 'SG_SERVER', db: 'Store_SG' }
  }
  
  
  return { server: linked.HANOI, db: dbNameByBranch("HANOI") };
}


async function executeAnalyticsProcOnBranch(pool, procName) {
  const rs = await pool.request().execute(`dbo.${procName}`);
  return rs.recordset || [];
}

async function executeAnalyticsProcOnCentral(pool, procName) {
  const rs = await pool.request().execute(`dbo.${procName}`);
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



async function listEmployeesByBranch(branch) {
  if (isMockMode()) return mock.listEmployeesByBranch(branch);
// Kết nối tới đúng Database của chi nhánh (HUE, SAIGON, hoặc HANOI)
  const pool = await getPool(branch);
  const procs = localProcNamesByBranch();
  // Thực thi Store Procedure tại chi nhánh để lấy danh sách nhân viên nội bộ

  const result = await pool.request().execute(procs.listEmployees);
  return result.recordset;
}

async function createEmployee(branch, payload) {
  if (isMockMode()) return mock.createEmployee(branch, payload);

  //  Xác định đúng chi nhánh đích để mở Pool
  // Nếu gọi từ CENTRAL, lấy targetBranch từ payload. Nếu gọi từ Local, lấy chính nó.
  const targetBranch = branch === "CENTRAL" 
    ? String(payload.ChiNhanh || payload.branch || "").trim().toUpperCase() 
    : branch;

  if (!["HUE", "SAIGON", "HANOI"].includes(targetBranch)) {
    throw new Error("ChiNhanh is required and must be HUE/SAIGON/HANOI");
  }

  // Kết nối TRỰC TIẾP tới Database chi nhánh đích
  const pool = await getPool(targetBranch);
  const localProcs = localProcNamesByBranch();
  const maNV = payload.MaNV || `${targetBranch[0]}${String(Date.now()).slice(-4)}`;

  // Chỉ gọi duy nhất 1 Proc của chi nhánh Local
  const rs = await pool
    .request()
    .input("MaNV", sql.VarChar(50), maNV)
    .input("HoTen", sql.NVarChar(120), payload.HoTen)
    .input("ChucVu", sql.NVarChar(80), payload.ChucVu)
    .execute(localProcs.createEmployee);
    
  return rs.recordset[0] || null;
}

/* --- CẬP NHẬT NHÂN VIÊN --- */
async function updateEmployee(branch, employeeId, payload) {
  if (isMockMode()) return mock.updateEmployee(branch, employeeId, payload);

  const targetBranch = branch === "CENTRAL" 
    ? String(payload.ChiNhanh || payload.branch || "").trim().toUpperCase() 
    : branch;

  if (!["HUE", "SAIGON", "HANOI"].includes(targetBranch)) {
    throw new Error("ChiNhanh is required and must be HUE/SAIGON/HANOI");
  }

  // Kết nối TRỰC TIẾP tới Database chi nhánh đích
  const pool = await getPool(targetBranch);
  const localProcs = localProcNamesByBranch();

  const rs = await pool
    .request()
    .input("MaNV", sql.VarChar(50), employeeId)
    .input("HoTen", sql.NVarChar(120), payload.HoTen || null)
    .input("ChucVu", sql.NVarChar(80), payload.ChucVu || null)
    .execute(localProcs.updateEmployee);

  if (!rs.recordset || !rs.recordset.length) {
    throw new Error(`Employee ${employeeId} not found in ${targetBranch}`);
  }
  return rs.recordset[0];
}

/* --- XÓA NHÂN VIÊN --- */
async function deleteEmployee(branch, employeeId, payload) {
  if (isMockMode()) return mock.deleteEmployee(branch, employeeId);

  // Khi xóa từ Central, Front-end cần gửi kèm ChiNhanh của nhân viên đó
  const targetBranch = branch === "CENTRAL" 
    ? String(payload?.ChiNhanh || payload?.branch || "").trim().toUpperCase() 
    : branch;

  if (!["HUE", "SAIGON", "HANOI"].includes(targetBranch)) {
    throw new Error("ChiNhanh is required to delete employee from CENTRAL");
  }

  const pool = await getPool(targetBranch);
  const localProcs = localProcNamesByBranch();

  // Đọc dữ liệu trước khi xóa để trả về cho UI
  const beforeDelete = await pool
    .request()
    .input("MaNV", sql.VarChar(50), employeeId)
    .query("SELECT TOP 1 * FROM NhanVien WHERE MaNV = @MaNV;");

  if (!beforeDelete.recordset.length) {
    throw new Error(`Employee ${employeeId} not found in ${targetBranch}`);
  }

  // Gọi Proc Local để xóa
  await pool
    .request()
    .input("MaNV", sql.VarChar(50), employeeId)
    .execute(localProcs.deleteEmployee);

  return beforeDelete.recordset[0];
}


//Lấy danh sách hóa đơn theo chi nhánh được truyền vào. Node.js kết nối đến DB của chi nhánh đó và gọi thủ tục usp_Local_DanhSachHoaDon để lấy dữ liệu tổng hợp.
async function listInvoicesByBranch(branch) {
  if (isMockMode()) {
    return mock.listInvoicesByBranch(branch);
  }

  const pool = await getPool(branch);
  const procs = localProcNamesByBranch();
  
  // Gọi thẳng Proc
  const result = await pool.request().execute(procs.listInvoices);
  return result.recordset;
}

//Lấy thông tin chi tiết một hóa đơn cụ thể. Node.js kết nối DB tương ứng, truyền invoiceId vào thủ tục usp_Local_ChiTietHoaDon để lấy ra danh sách sản phẩm, số lượng, và thành tiền.
async function getInvoiceDetails(branch, invoiceId) {
  if (isMockMode()) {
    return mock.getInvoiceDetailsLocal(branch, invoiceId);
  }

  const pool = await getPool(branch);
  const procs = localProcNamesByBranch();
  
  // Gọi thẳng Proc, truyền vào MaHD
  const result = await pool
    .request()
    .input("MaHD", sql.VarChar(50), invoiceId)
    .execute(procs.invoiceDetails);
    
  return result.recordset;
}
//Tạo một hóa đơn mới. Hàm này chuẩn bị dữ liệu sản phẩm thành chuỗi JSON, sau đó gọi thủ tục usp_Local_TaoHoaDonNhieuDong tại chi nhánh.
async function createInvoice(payload) {
 
  if (isMockMode()) {
    return mock.createInvoiceLocal(payload);
  }

  // 2. Mặc định dùng Stored Procedure 
  const pool = await getPool(payload.branch);
  const procs = localProcNamesByBranch();
  const maHD = `HD_${Date.now()}`;
  const employeeId = String(payload.employeeId || "").trim();

  if (!employeeId) {
    throw new Error("employeeId is required for invoice creation");
  }

  // Chuẩn bị mảng dữ liệu (items)
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

  // Thực thi Proc truyền vào JSON
  await pool
    .request()
    .input("MaHD", sql.VarChar(50), maHD)
    .input("MaNV", sql.VarChar(50), employeeId)
    .input("GhiChu", sql.NVarChar(255), String(payload.note || "").trim())
    .input("ChiNhanhLap", sql.VarChar(10), payload.branch)
    .input("ItemsJson", sql.NVarChar(sql.MAX), JSON.stringify(itemsPayload))
    .execute(procs.createInvoiceLines);

  // Lấy lại thông tin hóa đơn sau khi Proc chạy xong để trả về cho Frontend
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

/* ---  TỔNG HỢP DOANH THU TOÀN QUỐC --- */
async function getNationalRevenue(callerBranch) {
  // Mock mode: trả dữ liệu giả để test giao diện/luồng API.
  if (isMockMode()) return mock.revenueReport();

  // Chuẩn hóa caller để rẽ nhánh đúng CENTRAL/BRANCH.
  const normalizedCaller = String(callerBranch || "").trim().toUpperCase();

  let rows;
  let mode;
  if (normalizedCaller === "CENTRAL") {
    const pool = await getPool("CENTRAL");
    // CENTRAL: gọi proc ở DB gốc, không đi qua linked server.truy vấn tại db CENTRALDB
    rows = await executeAnalyticsProcOnCentral(pool, "usp_Central_DoanhThuQuocGia");
    mode = "SQL_SERVER_CENTRAL_PROC";
  } else {
    const pool = await getPool(normalizedCaller);
    // BRANCH: đọc view toàn cục tại branch (view đã gom dữ liệu qua linked server).
    const rs = await pool.request().query(`
      SELECT ChiNhanh AS BranchCode, SUM(ThanhTien) AS Revenue
      FROM dbo.v_HoaDonChiTiet_ToanQuoc
      GROUP BY ChiNhanh;
    `);
    rows = rs.recordset || [];
    mode = "SQL_SERVER_BRANCH_VIEW";
  }

  // Chuẩn hóa output về kiểu số để frontend render an toàn.
  const byBranch = rows.map((row) => ({
    branch: row.BranchCode,
    revenue: Number(row.Revenue || 0),
  }));
  const nationalRevenue = byBranch.reduce((sum, item) => sum + item.revenue, 0);

  return {
    mode,
    callerBranch: normalizedCaller,
    generatedAt: new Date().toISOString(),
    byBranch,
    nationalRevenue,
  };
}
 
async function getCentralAnalyticsOverview(callerBranch, sourceBranch) {

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

  // Chuẩn hóa caller trước khi quyết định chạy proc central hay branch.
  const normalizedCaller = String(callerBranch || "").trim().toUpperCase();
  let analyticsSourceBranch;
  let mode;
  let dailyRows;
  let weeklyRows;
  let topEmployeeRows;
  let topProductRows;
  let compareRows;

  if (normalizedCaller === "CENTRAL") {
    const pool = await getPool("CENTRAL");
    analyticsSourceBranch = "CENTRAL";
    mode = "SQL_SERVER_CENTRAL_PROC";

    // nếu là CENTRAL: toàn bộ analytics lấy qua các proc ở server trung tâm. proc này lấy dữ liệu Trực tiếp tại db trung tâm
    [dailyRows, weeklyRows, topEmployeeRows, topProductRows, compareRows] = await Promise.all([
      executeAnalyticsProcOnCentral(pool, "usp_Central_DoanhThuVaSoDon_TheoNgay"),
      executeAnalyticsProcOnCentral(pool, "usp_Central_DoanhThuVaSoDon_TheoTuan"),
      executeAnalyticsProcOnCentral(pool, "usp_Central_NhanVienBanTotNhatTuan"),
      executeAnalyticsProcOnCentral(pool, "usp_Central_SanPhamBanChayNhat_MoiChiNhanh"),
      executeAnalyticsProcOnCentral(pool, "usp_Central_SoSanhDoanhThuTuan"),
    ]);
  } else {
    const pool = await getPool(normalizedCaller);
    //  BRANCH: sourceBranch chỉ dùng để hiển thị nguồn đọc analytics trên UI.
    analyticsSourceBranch = normalizeAnalyticsBranch(
      sourceBranch || normalizedCaller || process.env.ANALYTICS_SP_SOURCE_BRANCH || "HUE",
    );
    mode = "SQL_SERVER_BRANCH_VIEW_PROC";

    // nếu là chi nhánh BRANCH: gọi các proc analytics đã triển khai ở DB chi nhánh. proc này lấy dữ liệu qua link server ko cần vào db trung tâm
    [dailyRows, weeklyRows, topEmployeeRows, topProductRows, compareRows] = await Promise.all([
      executeAnalyticsProcOnBranch(pool, "usp_DoanhThuVaSoDon_TheoNgay"),
      executeAnalyticsProcOnBranch(pool, "usp_DoanhThuVaSoDon_TheoTuan"),
      executeAnalyticsProcOnBranch(pool, "usp_NhanVienBanTotNhatTuan"),
      executeAnalyticsProcOnBranch(pool, "usp_SanPhamBanChayNhat_MoiChiNhanh"),
      executeAnalyticsProcOnBranch(pool, "usp_SoSanhDoanhThuTuan"),
    ]);
  }

  // Mapping recordset -> DTO trả cho frontend (thống nhất tên field + ép kiểu số).
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
    mode,
    callerBranch: normalizedCaller,
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
 * Liệt kê tồn kho cho chi nhánh. 
 * Nếu có `productCode` trả 1 dòng, nếu không trả tất cả `TonKho` của chi nhánh.
 */
async function listInventory(branch, productCode) {
  if (isMockMode()) {
    if (!productCode) {
      return mock.listInventory(branch);
    }
    return mock.getInventory(branch, productCode);
  }

  const pool = await getPool(branch);
  const procs = localProcNamesByBranch();

  // 1. Trường hợp không truyền productCode (Lấy tất cả)
  if (!productCode) {
    const rows = await pool.request().execute(procs.listInventory);
    return rows.recordset.map((row) => ({
      branch,
      productCode: row.MaSP,
      quantity: Number(row.SoLuongTon || 0),
    }));
  }

  // 2. Trường hợp có truyền productCode (Lấy 1 sản phẩm)
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


//lấy chi tiết sản phẩm 
async function getProductByCode(branch, productCode) {
  if (isMockMode()) {
    return mock.getProductByCode(branch, productCode);
  }

  const code = String(productCode || "").trim();
  if (!code) {
    throw new Error("productCode is required");
  }

  // Kết nối thẳng vào DB của chi nhánh truyền xuống (có thể là HUE, SAIGON...)
  const pool = await getPool(branch);
  const commonProcs = commonProcNames();

  const rs = await pool
    .request()
    .input("MaSP", sql.VarChar(50), code)
    .execute(commonProcs.productByCode); // Gọi Proc dùng chung

  const row = rs.recordset[0] || null;
  if (!row) return null;

  return {
    branch,
    productCode: row.MaSP,
    productName: row.TenHang,
    unitPrice: Number(row.Gia || 0),
  };
}

// lấy danh sách sản phẩm 
async function listProducts(branch = "CENTRAL") {
  if (isMockMode()) {
    return mock.listProducts();
  }

  // Kết nối thẳng vào DB của chi nhánh tương ứng
  const pool = await getPool(branch);
  const commonProcs = commonProcNames();
  
  // Đọc danh mục hàng hóa qua proc chung
  const result = await pool.request().execute(commonProcs.listProducts);
  
  return result.recordset;
}
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


//hàm cập nhật tồn kho tổng quát cho một sản phẩm tại chi nhánh. Hàm này sẽ gọi thủ tục usp_Local_CapNhatTonKhoTongQuat tại chi nhánh để cập nhật số lượng tồn kho mới. 
async function updateInventoryItem(branch, productCode, quantity) {
  if (isMockMode()) {
    return mock.updateInventoryItem(branch, productCode, quantity);
  }

  const pool = await getPool(branch);
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


/**
 * Lấy toàn bộ dữ liệu để vẽ giao diện Dashboard (Tổng quan) cho MỘT chi nhánh.
 * Hàm này sẽ gọi cùng lúc 3 Proc nội bộ của chi nhánh đó để gom dữ liệu cho lẹ.
 */
async function getBranchDashboard(branch) {
  if (isMockMode()) {
    return mock.branchDashboard(branch);
  }

  const pool = await getPool(branch);
  const procs = localProcNamesByBranch();

  // Gọi 3 Proc song song (đã bỏ if và bỏ query raw)
  const [summaryRs, revenueRs, topStockRs] = await Promise.all([
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
    mode: "SQL_SERVER_PROC", 
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

async function transferStockDistributed(payload) {
  if (isMockMode()) {
    return mock.transferStock(payload);
  }

  const pool = await getPool("CENTRAL");
  
  // Chuẩn hóa dữ liệu đầu vào cho an toàn
  const quantity = Number(payload.quantity || 0);
  const fromBranch = String(payload.fromBranch || "").toUpperCase();
  const toBranch = String(payload.toBranch || "").toUpperCase();
  const productCode = String(payload.productCode || "").trim();

  // Chuẩn bị thông tin Linked Server để đọc số liệu
  const linked = linkedServerNames();
  const fromServer = linked[fromBranch];
  const toServer = linked[toBranch];
  const fromDb = dbNameByBranch(fromBranch);
  const toDb = dbNameByBranch(toBranch);

  if (!fromServer || !toServer) {
    throw new Error("Cannot resolve linked server names for transfer");
  }


  //  ĐỌC TỒN KHO TRƯỚC KHI CHUYỂN (Cho UI)

  const beforeFrom = await pool.request()
    .input("MaSP", sql.VarChar(50), productCode)
    .input("FromBranch", sql.VarChar(10), fromBranch)
    .query(`SELECT TOP 1 SoLuongTon FROM [${fromServer}].[${fromDb}].dbo.TonKho WHERE MaSP = @MaSP AND ChiNhanh = @FromBranch;`);
  
  const beforeTo = await pool.request()
    .input("MaSP", sql.VarChar(50), productCode)
    .input("ToBranch", sql.VarChar(10), toBranch)
    .query(`SELECT TOP 1 SoLuongTon FROM [${toServer}].[${toDb}].dbo.TonKho WHERE MaSP = @MaSP AND ChiNhanh = @ToBranch;`);

  const beforeFromQty = Number((beforeFrom.recordset[0] || {}).SoLuongTon || 0);
  const beforeToQty = Number((beforeTo.recordset[0] || {}).SoLuongTon || 0);


  //  THỰC THI GIAO DỊCH QUA STORED PROCEDURE
  
  try {
    await pool
      .request()
      .input("TuChiNhanh", sql.VarChar(10), fromBranch)
      .input("DenChiNhanh", sql.VarChar(10), toBranch)
      .input("MaSP", sql.VarChar(50), productCode)
      .input("SoLuongChuyen", sql.Int, quantity)
      .execute("dbo.usp_Central_DieuChuyenKho");
  } catch (error) {
    const message = String(error.message || "");
    const linkedHints =
      message.toLowerCase().includes("distributed transaction") ||
      message.toLowerCase().includes("msdtc") ||
      message.toLowerCase().includes("linked server");

    if (linkedHints) {
      throw new Error(`Transfer failed due to linked-server/distributed-transaction setup. Please check MSDTC configs. Original error: ${message}`);
    }
    throw error;
  }
  //  ĐỌC TỒN KHO SAU KHI CHUYỂN (Cho UI)

  const afterFrom = await pool.request()
    .input("MaSP", sql.VarChar(50), productCode)
    .input("FromBranch", sql.VarChar(10), fromBranch)
    .query(`SELECT TOP 1 SoLuongTon FROM [${fromServer}].[${fromDb}].dbo.TonKho WHERE MaSP = @MaSP AND ChiNhanh = @FromBranch;`);
  
  const afterTo = await pool.request()
    .input("MaSP", sql.VarChar(50), productCode)
    .input("ToBranch", sql.VarChar(10), toBranch)
    .query(`SELECT TOP 1 SoLuongTon FROM [${toServer}].[${toDb}].dbo.TonKho WHERE MaSP = @MaSP AND ChiNhanh = @ToBranch;`);

  const afterFromQty = Number((afterFrom.recordset[0] || {}).SoLuongTon || 0);
  const afterToQty = Number((afterTo.recordset[0] || {}).SoLuongTon || 0);

  
  //  TRẢ VỀ FORMAT ĐẦY ĐỦ CỦA FRONTEND

  return {
    mode: "SQL_SERVER_PROC",
    status: "COMMITTED",
    productCode: productCode,
    quantity: quantity,
    fromBranch: fromBranch,
    toBranch: toBranch,
    before: {
      from: beforeFromQty,
      to: beforeToQty,
    },
    after: {
      from: afterFromQty,
      to: afterToQty,
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

  getNationalRevenue,
  getCentralAnalyticsOverview,
  transferStockDistributed,
  listInventory,
  listProducts,
  createProduct,
  updateProduct,
  deleteProduct,
  getProductByCode, 
  updateInventoryItem,
 getBranchDashboard,
};
