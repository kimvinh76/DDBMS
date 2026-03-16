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

async function listEmployeesByBranch(branch) {
  if (isMockMode()) {
    return mock.listEmployeesByBranch(branch);
  }

  const pool = await getPool(branch);
  const result = await pool.request().query("SELECT * FROM NhanVien;");
  return result.recordset;
}

async function createEmployee(branch, payload) {
  if (isMockMode()) {
    return mock.createEmployee(branch, payload);
  }

  const pool = await getPool(branch);
  const result = await pool
    .request()
    .input(
      "MaNV",
      sql.VarChar(50),
      payload.MaNV || `${branch[0]}${String(Date.now()).slice(-4)}`,
    )
    .input("HoTen", sql.NVarChar(120), payload.HoTen)
    .input("ChucVu", sql.NVarChar(80), payload.ChucVu).query(`
      INSERT INTO NhanVien (MaNV, HoTen, ChucVu)
      OUTPUT inserted.*
      VALUES (@MaNV, @HoTen, @ChucVu);
    `);
  return result.recordset[0];
}

async function updateEmployee(branch, employeeId, payload) {
  if (isMockMode()) {
    return mock.updateEmployee(branch, employeeId, payload);
  }

  const pool = await getPool(branch);
  const result = await pool
    .request()
    .input("MaNV", sql.VarChar(50), employeeId)
    .input("HoTen", sql.NVarChar(120), payload.HoTen || null)
    .input("ChucVu", sql.NVarChar(80), payload.ChucVu || null).query(`
      UPDATE NhanVien
      SET HoTen = COALESCE(@HoTen, HoTen),
          ChucVu = COALESCE(@ChucVu, ChucVu)
      OUTPUT inserted.*
      WHERE MaNV = @MaNV;
    `);
  if (!result.recordset.length) {
    throw new Error(`Employee ${employeeId} not found in ${branch}`);
  }
  return result.recordset[0];
}

async function deleteEmployee(branch, employeeId) {
  if (isMockMode()) {
    return mock.deleteEmployee(branch, employeeId);
  }

  const pool = await getPool(branch);
  const result = await pool
    .request()
    .input("MaNV", sql.VarChar(50), employeeId)
    .query("DELETE FROM NhanVien OUTPUT deleted.* WHERE MaNV = @MaNV;");
  if (!result.recordset.length) {
    throw new Error(`Employee ${employeeId} not found in ${branch}`);
  }
  return result.recordset[0];
}

async function listInvoicesByBranch(branch) {
  if (isMockMode()) {
    return mock.listInvoicesByBranch(branch);
  }

  const pool = await getPool(branch);
  const result = await pool
    .request()
    .query("SELECT * FROM HoaDon ORDER BY NgayTao DESC;");
  return result.recordset;
}

async function createInvoice(payload) {
  if (isMockMode()) {
    return mock.createInvoiceLocal(payload);
  }

  const pool = await getPool(payload.branch);
  const result = await pool
    .request()
    .input("MaHD", sql.VarChar(50), `HD_${Date.now()}`)
    .input("MaSP", sql.VarChar(50), payload.productCode)
    .input("SoLuong", sql.Int, payload.quantity)
    .input("TongTien", sql.Decimal(18, 2), payload.totalAmount)
    .input("GhiChu", sql.NVarChar(255), payload.note).query(`
      INSERT INTO HoaDon (MaHD, MaSP, SoLuong, TongTien, GhiChu, NgayTao)
      OUTPUT inserted.*
      VALUES (@MaHD, @MaSP, @SoLuong, @TongTien, @GhiChu, GETDATE());
    `);

  return result.recordset[0];
}

async function updateInvoice(branch, invoiceId, payload) {
  if (isMockMode()) {
    return mock.updateInvoiceLocal(branch, invoiceId, payload);
  }

  const pool = await getPool(branch);
  const result = await pool
    .request()
    .input("MaHD", sql.VarChar(50), invoiceId)
    .input("MaSP", sql.VarChar(50), payload.productCode || null)
    .input("SoLuong", sql.Int, payload.quantity > 0 ? payload.quantity : null)
    .input(
      "TongTien",
      sql.Decimal(18, 2),
      payload.totalAmount > 0 ? payload.totalAmount : null,
    )
    .input("GhiChu", sql.NVarChar(255), payload.note ?? null).query(`
      UPDATE HoaDon
      SET MaSP = COALESCE(@MaSP, MaSP),
          SoLuong = COALESCE(@SoLuong, SoLuong),
          TongTien = COALESCE(@TongTien, TongTien),
          GhiChu = COALESCE(@GhiChu, GhiChu)
      OUTPUT inserted.*
      WHERE MaHD = @MaHD;
    `);
  if (!result.recordset.length) {
    throw new Error(`Invoice ${invoiceId} not found in ${branch}`);
  }
  return result.recordset[0];
}

async function deleteInvoice(branch, invoiceId) {
  if (isMockMode()) {
    return mock.deleteInvoiceLocal(branch, invoiceId);
  }

  const pool = await getPool(branch);
  const result = await pool
    .request()
    .input("MaHD", sql.VarChar(50), invoiceId)
    .query("DELETE FROM HoaDon OUTPUT deleted.* WHERE MaHD = @MaHD;");
  if (!result.recordset.length) {
    throw new Error(`Invoice ${invoiceId} not found in ${branch}`);
  }
  return result.recordset[0];
}

async function listAllEmployeesFromCentral() {
  if (isMockMode()) {
    return mock.listAllEmployees();
  }

  const linked = linkedServerNames();
  const pool = await getPool("CENTRAL");
  const result = await pool.request().query(`
    SELECT * FROM [${linked.HUE}].[Store_H].dbo.NhanVien
    UNION ALL
    SELECT * FROM [${linked.SAIGON}].[Store_SG].dbo.NhanVien
    UNION ALL
    SELECT * FROM [${linked.HANOI}].[Store_HN].dbo.NhanVien;
  `);

  return result.recordset;
}

async function getNationalRevenue() {
  if (isMockMode()) {
    return mock.revenueReport();
  }

  const linked = linkedServerNames();
  const pool = await getPool("CENTRAL");
  const result = await pool.request().query(`
    SELECT BranchCode, SUM(TongTien) AS Revenue
    FROM (
      SELECT 'HUE' AS BranchCode, TongTien FROM [${linked.HUE}].[Store_H].dbo.HoaDon
      UNION ALL
      SELECT 'SAIGON' AS BranchCode, TongTien FROM [${linked.SAIGON}].[Store_SG].dbo.HoaDon
      UNION ALL
      SELECT 'HANOI' AS BranchCode, TongTien FROM [${linked.HANOI}].[Store_HN].dbo.HoaDon
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

async function listInventory(branch, productCode) {
  if (isMockMode()) {
    if (!productCode) {
      return mock.listInventory(branch);
    }
    return mock.getInventory(branch, productCode);
  }

  const pool = await getPool(branch);
  if (!productCode) {
    const rows = await pool
      .request()
      .query("SELECT MaSP, SoLuongTon FROM TonKho ORDER BY MaSP;");
    return rows.recordset.map((row) => ({
      branch,
      productCode: row.MaSP,
      quantity: Number(row.SoLuongTon || 0),
    }));
  }

  const result = await pool
    .request()
    .input("MaSP", sql.VarChar(50), productCode)
    .query("SELECT MaSP, SoLuongTon FROM TonKho WHERE MaSP = @MaSP;");

  const row = result.recordset[0] || { MaSP: productCode, SoLuongTon: 0 };
  return {
    branch,
    productCode: row.MaSP,
    quantity: Number(row.SoLuongTon || 0),
  };
}

async function createInventoryItem(branch, payload) {
  if (isMockMode()) {
    return mock.createInventoryItem(branch, payload);
  }

  const pool = await getPool(branch);
  const result = await pool
    .request()
    .input("MaSP", sql.VarChar(50), payload.productCode)
    .input("SoLuongTon", sql.Int, payload.quantity).query(`
      INSERT INTO TonKho (MaSP, SoLuongTon)
      OUTPUT inserted.*
      VALUES (@MaSP, @SoLuongTon);
    `);
  return {
    branch,
    productCode: result.recordset[0].MaSP,
    quantity: Number(result.recordset[0].SoLuongTon || 0),
  };
}

async function updateInventoryItem(branch, productCode, quantity) {
  if (isMockMode()) {
    return mock.updateInventoryItem(branch, productCode, quantity);
  }

  const pool = await getPool(branch);
  const result = await pool
    .request()
    .input("MaSP", sql.VarChar(50), productCode)
    .input("SoLuongTon", sql.Int, quantity).query(`
      UPDATE TonKho
      SET SoLuongTon = @SoLuongTon
      OUTPUT inserted.*
      WHERE MaSP = @MaSP;
    `);
  if (!result.recordset.length) {
    throw new Error(`Product ${productCode} not found in ${branch}`);
  }
  return {
    branch,
    productCode: result.recordset[0].MaSP,
    quantity: Number(result.recordset[0].SoLuongTon || 0),
  };
}

async function deleteInventoryItem(branch, productCode) {
  if (isMockMode()) {
    return mock.deleteInventoryItem(branch, productCode);
  }

  const pool = await getPool(branch);
  const result = await pool
    .request()
    .input("MaSP", sql.VarChar(50), productCode)
    .query("DELETE FROM TonKho OUTPUT deleted.* WHERE MaSP = @MaSP;");
  if (!result.recordset.length) {
    throw new Error(`Product ${productCode} not found in ${branch}`);
  }
  return {
    branch,
    productCode: result.recordset[0].MaSP,
    quantity: Number(result.recordset[0].SoLuongTon || 0),
  };
}

async function getBranchDashboard(branch) {
  if (isMockMode()) {
    return mock.branchDashboard(branch);
  }

  const pool = await getPool(branch);
  const [employeesRs, invoicesRs, revenueRs, stockRs] = await Promise.all([
    pool.request().query("SELECT COUNT(1) AS count FROM NhanVien;"),
    pool.request().query("SELECT COUNT(1) AS count FROM HoaDon;"),
    pool
      .request()
      .query("SELECT ISNULL(SUM(TongTien), 0) AS revenue FROM HoaDon;"),
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

async function transferStockDistributed(payload) {
  if (isMockMode()) {
    return mock.transferStock(payload);
  }

  const linked = linkedServerNames();
  const fromServer = linked[payload.fromBranch];
  const toServer = linked[payload.toBranch];

  if (!fromServer || !toServer) {
    throw new Error("Cannot resolve linked server names for transfer");
  }

  const pool = await getPool("CENTRAL");

  const beforeFrom = await pool
    .request()
    .input("MaSP", sql.VarChar(50), payload.productCode)
    .query(
      `SELECT SoLuongTon FROM [${fromServer}].[${payload.fromBranch === "HUE" ? "Store_H" : payload.fromBranch === "SAIGON" ? "Store_SG" : "Store_HN"}].dbo.TonKho WHERE MaSP = @MaSP;`,
    );

  const beforeTo = await pool
    .request()
    .input("MaSP", sql.VarChar(50), payload.productCode)
    .query(
      `SELECT SoLuongTon FROM [${toServer}].[${payload.toBranch === "HUE" ? "Store_H" : payload.toBranch === "SAIGON" ? "Store_SG" : "Store_HN"}].dbo.TonKho WHERE MaSP = @MaSP;`,
    );

  const fromQty = Number((beforeFrom.recordset[0] || {}).SoLuongTon || 0);
  if (fromQty < payload.quantity) {
    throw new Error(
      `Insufficient stock in ${payload.fromBranch}: current ${fromQty}, requested ${payload.quantity}`,
    );
  }

  await pool
    .request()
    .input("MaSP", sql.VarChar(50), payload.productCode)
    .input("SoLuong", sql.Int, payload.quantity).query(`
      BEGIN DISTRIBUTED TRANSACTION;
        UPDATE [${fromServer}].[${payload.fromBranch === "HUE" ? "Store_H" : payload.fromBranch === "SAIGON" ? "Store_SG" : "Store_HN"}].dbo.TonKho
        SET SoLuongTon = SoLuongTon - @SoLuong
        WHERE MaSP = @MaSP;

        UPDATE [${toServer}].[${payload.toBranch === "HUE" ? "Store_H" : payload.toBranch === "SAIGON" ? "Store_SG" : "Store_HN"}].dbo.TonKho
        SET SoLuongTon = SoLuongTon + @SoLuong
        WHERE MaSP = @MaSP;
      COMMIT TRANSACTION;
    `);

  const afterFrom = await pool
    .request()
    .input("MaSP", sql.VarChar(50), payload.productCode)
    .query(
      `SELECT SoLuongTon FROM [${fromServer}].[${payload.fromBranch === "HUE" ? "Store_H" : payload.fromBranch === "SAIGON" ? "Store_SG" : "Store_HN"}].dbo.TonKho WHERE MaSP = @MaSP;`,
    );

  const afterTo = await pool
    .request()
    .input("MaSP", sql.VarChar(50), payload.productCode)
    .query(
      `SELECT SoLuongTon FROM [${toServer}].[${payload.toBranch === "HUE" ? "Store_H" : payload.toBranch === "SAIGON" ? "Store_SG" : "Store_HN"}].dbo.TonKho WHERE MaSP = @MaSP;`,
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
  createInvoice,
  updateInvoice,
  deleteInvoice,
  listAllEmployeesFromCentral,
  getNationalRevenue,
  transferStockDistributed,
  listInventory,
  createInventoryItem,
  updateInventoryItem,
  deleteInventoryItem,
  getBranchDashboard,
};
