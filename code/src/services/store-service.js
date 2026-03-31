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

async function hasHoaDonEmployeeColumnWithPool(pool) {
  const rs = await pool
    .request()
    .query("SELECT CASE WHEN COL_LENGTH('dbo.HoaDon', 'MaNV') IS NULL THEN 0 ELSE 1 END AS hasColumn;");
  return Boolean((rs.recordset[0] || {}).hasColumn);
}

async function hasHoaDonEmployeeColumnWithTransaction(transaction) {
  const rs = await new sql.Request(transaction)
    .query("SELECT CASE WHEN COL_LENGTH('dbo.HoaDon', 'MaNV') IS NULL THEN 0 ELSE 1 END AS hasColumn;");
  return Boolean((rs.recordset[0] || {}).hasColumn);
}

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

  const maNV = payload.MaNV || `${branch[0]}${String(Date.now()).slice(-4)}`;
  const pool = await getPool(branch);
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

async function updateEmployee(branch, employeeId, payload) {
  if (isMockMode()) {
    return mock.updateEmployee(branch, employeeId, payload);
  }

  const pool = await getPool(branch);
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

async function deleteEmployee(branch, employeeId) {
  if (isMockMode()) {
    return mock.deleteEmployee(branch, employeeId);
  }

  const pool = await getPool(branch);
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

async function listInvoicesByBranch(branch) {
  if (isMockMode()) {
    return mock.listInvoicesByBranch(branch);
  }

  const pool = await getPool(branch);
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

async function getInvoiceDetails(branch, invoiceId) {
  if (isMockMode()) {
    return mock.getInvoiceDetailsLocal(branch, invoiceId);
  }

  const pool = await getPool(branch);
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

async function createInvoice(payload) {
  if (isMockMode()) {
    return mock.createInvoiceLocal(payload);
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

async function getProductByCode(branch, productCode) {
  if (isMockMode()) {
    return mock.getProductByCode(branch, productCode);
  }

  const code = String(productCode || "").trim();
  if (!code) {
    throw new Error("productCode is required");
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

async function createInventoryItem(branch, payload) {
  if (isMockMode()) {
    return mock.createInventoryItem(branch, payload);
  }

  const pool = await getPool(branch);
  const productName = payload.productName || payload.productCode;
  const unitPrice = Number.isFinite(Number(payload.unitPrice))
    ? Number(payload.unitPrice)
    : 0;

  await pool
    .request()
    .input("ChiNhanh", sql.VarChar(10), branch)
    .input("MaSP", sql.VarChar(50), payload.productCode)
    .input("TenHang", sql.NVarChar(100), productName)
    .input("Gia", sql.Decimal(10, 2), unitPrice)
    .input("SoLuongTon", sql.Int, payload.quantity).query(`
      IF NOT EXISTS (SELECT 1 FROM HangHoa WHERE MaSP = @MaSP)
      BEGIN
        INSERT INTO HangHoa (MaSP, TenHang, Gia)
        VALUES (@MaSP, @TenHang, @Gia);
      END

      INSERT INTO TonKho (MaSP, SoLuongTon, ChiNhanh)
      VALUES (@MaSP, @SoLuongTon, @ChiNhanh);
    `);

  const inserted = await pool
    .request()
    .input("ChiNhanh", sql.VarChar(10), branch)
    .input("MaSP", sql.VarChar(50), payload.productCode)
    .query(
      "SELECT TOP 1 MaSP, SoLuongTon FROM TonKho WHERE ChiNhanh = @ChiNhanh AND MaSP = @MaSP;",
    );

  return {
    branch,
    productCode: inserted.recordset[0].MaSP,
    quantity: Number(inserted.recordset[0].SoLuongTon || 0),
  };
}

async function updateInventoryItem(branch, productCode, quantity) {
  if (isMockMode()) {
    return mock.updateInventoryItem(branch, productCode, quantity);
  }

  const pool = await getPool(branch);
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
  transferStockDistributed,
  listInventory,
  getProductByCode,
  createInventoryItem,
  updateInventoryItem,
  deleteInventoryItem,
  getBranchDashboard,
};
