const employees = {
  HUE: [
    {
      MaNV: "H001",
      HoTen: "Nguyen Van A",
      ChiNhanh: "HUE",
      ChucVu: "Thu ngan",
    },
    { MaNV: "H002", HoTen: "Le Van B", ChiNhanh: "HUE", ChucVu: "Quan ly kho" },
    { MaNV: "H003", HoTen: "Pham Thi C", ChiNhanh: "HUE", ChucVu: "Ban hang" },
  ],
  SAIGON: [
    {
      MaNV: "S001",
      HoTen: "Tran Thi C",
      ChiNhanh: "SAIGON",
      ChucVu: "Ban hang",
    },
    {
      MaNV: "S002",
      HoTen: "Pham Van D",
      ChiNhanh: "SAIGON",
      ChucVu: "Thu ngan",
    },
    {
      MaNV: "S003",
      HoTen: "Le Thi F",
      ChiNhanh: "SAIGON",
      ChucVu: "Quan ly kho",
    },
  ],
  HANOI: [
    { MaNV: "N001", HoTen: "Do Thi E", ChiNhanh: "HANOI", ChucVu: "Ban hang" },
    {
      MaNV: "N002",
      HoTen: "Hoang Van F",
      ChiNhanh: "HANOI",
      ChucVu: "Quan ly cua hang",
    },
    {
      MaNV: "N003",
      HoTen: "Nguyen Thi I",
      ChiNhanh: "HANOI",
      ChucVu: "Thu ngan",
    },
  ],
};

const products = {
  MI_GOI: { TenHang: "Mi goi", Gia: 12000 },
  SUA_HOP: { TenHang: "Sua hop", Gia: 12000 },
  NUOC_SUOI: { TenHang: "Nuoc suoi", Gia: 7000 },
  BANH_SNACK: { TenHang: "Banh snack", Gia: 15000 },
  CA_PHE_LON: { TenHang: "Ca phe lon", Gia: 18000 },
  TRA_XANH: { TenHang: "Tra xanh", Gia: 10000 },
};

const inventory = {
  HUE: {
    MI_GOI: 220,
    SUA_HOP: 80,
    NUOC_SUOI: 140,
    BANH_SNACK: 95,
    CA_PHE_LON: 75,
    TRA_XANH: 130,
  },
  SAIGON: {
    MI_GOI: 110,
    SUA_HOP: 120,
    NUOC_SUOI: 160,
    BANH_SNACK: 100,
    CA_PHE_LON: 90,
    TRA_XANH: 145,
  },
  HANOI: {
    MI_GOI: 170,
    SUA_HOP: 60,
    NUOC_SUOI: 150,
    BANH_SNACK: 105,
    CA_PHE_LON: 85,
    TRA_XANH: 120,
  },
};

const invoiceSeeds = [
  ["HD_HUE_001", "HUE", "MI_GOI", 10],
  ["HD_HUE_002", "HUE", "SUA_HOP", 5],
  ["HD_HUE_003", "HUE", "NUOC_SUOI", 12],
  ["HD_HUE_004", "HUE", "BANH_SNACK", 4],
  ["HD_HUE_005", "HUE", "CA_PHE_LON", 6],
  ["HD_HUE_006", "HUE", "TRA_XANH", 7],
  ["HD_HUE_007", "HUE", "MI_GOI", 3],
  ["HD_HUE_008", "HUE", "BANH_SNACK", 2],
  ["HD_HUE_009", "HUE", "NUOC_SUOI", 8],
  ["HD_HUE_010", "HUE", "TRA_XANH", 5],
  ["HD_SG_001", "SAIGON", "SUA_HOP", 8],
  ["HD_SG_002", "SAIGON", "MI_GOI", 6],
  ["HD_SG_003", "SAIGON", "NUOC_SUOI", 10],
  ["HD_SG_004", "SAIGON", "BANH_SNACK", 5],
  ["HD_SG_005", "SAIGON", "CA_PHE_LON", 7],
  ["HD_SG_006", "SAIGON", "TRA_XANH", 9],
  ["HD_SG_007", "SAIGON", "MI_GOI", 4],
  ["HD_SG_008", "SAIGON", "BANH_SNACK", 3],
  ["HD_SG_009", "SAIGON", "NUOC_SUOI", 6],
  ["HD_SG_010", "SAIGON", "TRA_XANH", 4],
  ["HD_HN_001", "HANOI", "MI_GOI", 6],
  ["HD_HN_002", "HANOI", "SUA_HOP", 4],
  ["HD_HN_003", "HANOI", "NUOC_SUOI", 9],
  ["HD_HN_004", "HANOI", "BANH_SNACK", 6],
  ["HD_HN_005", "HANOI", "CA_PHE_LON", 5],
  ["HD_HN_006", "HANOI", "TRA_XANH", 8],
  ["HD_HN_007", "HANOI", "MI_GOI", 3],
  ["HD_HN_008", "HANOI", "BANH_SNACK", 2],
  ["HD_HN_009", "HANOI", "NUOC_SUOI", 7],
  ["HD_HN_010", "HANOI", "TRA_XANH", 5],
];

const invoices = invoiceSeeds.map(([MaHD, ChiNhanh, MaSP, SoLuong]) => {
  const product = products[MaSP] || { TenHang: MaSP, Gia: 0 };
  return {
    MaHD,
    ChiNhanh,
    MaSP,
    SoLuong,
    TongTien: Number(product.Gia || 0) * Number(SoLuong || 0),
    GhiChu: "Hoa don khoi tao",
    NgayTao: new Date().toISOString(),
    TenHang: product.TenHang,
    DonGia: Number(product.Gia || 0),
  };
});

function branchPrefix(branch) {
  if (branch === "HUE") return "H";
  if (branch === "SAIGON") return "S";
  return "N";
}

function listEmployeesByBranch(branch) {
  return [...(employees[branch] || [])];
}

function createEmployee(branch, payload) {
  const list = employees[branch] || (employees[branch] = []);
  const maNV =
    payload.MaNV || `${branchPrefix(branch)}${String(Date.now()).slice(-4)}`;
  if (list.find((item) => item.MaNV === maNV)) {
    throw new Error(`Employee ${maNV} already exists in ${branch}`);
  }
  const created = {
    MaNV: maNV,
    HoTen: payload.HoTen,
    ChiNhanh: branch,
    ChucVu: payload.ChucVu,
  };
  list.push(created);
  return created;
}

function updateEmployee(branch, employeeId, payload) {
  const list = employees[branch] || [];
  const index = list.findIndex((item) => item.MaNV === employeeId);
  if (index < 0) {
    throw new Error(`Employee ${employeeId} not found in ${branch}`);
  }
  const next = {
    ...list[index],
    HoTen: payload.HoTen || list[index].HoTen,
    ChucVu: payload.ChucVu || list[index].ChucVu,
  };
  list[index] = next;
  return next;
}

function deleteEmployee(branch, employeeId) {
  const list = employees[branch] || [];
  const index = list.findIndex((item) => item.MaNV === employeeId);
  if (index < 0) {
    throw new Error(`Employee ${employeeId} not found in ${branch}`);
  }
  const [deleted] = list.splice(index, 1);
  return deleted;
}

function listAllEmployees() {
  return Object.keys(employees).flatMap((branch) => employees[branch]);
}

function defaultEmployeeId(branch) {
  const first = (employees[branch] || [])[0];
  return first ? first.MaNV : null;
}

function createInvoiceLocal(payload) {
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

  const normalizedItems = rawItems
    .map((rawItem) => {
      const productCode = String(rawItem?.productCode || "").trim();
      const quantity = Number(rawItem?.quantity || 0);
      if (!productCode || quantity <= 0) {
        return null;
      }

      const existing = products[productCode] || {
        TenHang: String(rawItem?.productName || "").trim() || productCode,
        Gia: Number(rawItem?.unitPrice || 0),
      };

      const unitPrice =
        Number(rawItem?.unitPrice || 0) > 0
          ? Number(rawItem.unitPrice)
          : Number(existing.Gia || 0);
      const productName =
        String(rawItem?.productName || "").trim() || existing.TenHang;

      products[productCode] = {
        TenHang: productName,
        Gia: unitPrice,
      };

      return {
        productCode,
        productName,
        unitPrice,
        quantity,
        totalAmount: Number(quantity || 0) * Number(unitPrice || 0),
      };
    })
    .filter(Boolean);

  if (!normalizedItems.length) {
    throw new Error("Each invoice item must include productCode and quantity > 0");
  }

  const mergedItemsByProduct = new Map();
  for (const item of normalizedItems) {
    const existing = mergedItemsByProduct.get(item.productCode);
    if (!existing) {
      mergedItemsByProduct.set(item.productCode, { ...item });
      continue;
    }

    if (Number(existing.unitPrice || 0) !== Number(item.unitPrice || 0)) {
      throw new Error(
        `Duplicate product ${item.productCode} has different unit prices in the same invoice`,
      );
    }

    existing.quantity += Number(item.quantity || 0);
    existing.totalAmount =
      Number(existing.quantity || 0) * Number(existing.unitPrice || 0);
  }

  const mergedItems = Array.from(mergedItemsByProduct.values());

  const maHD = `HD_${Date.now()}`;
  const createdAt = new Date().toISOString();
  for (const item of mergedItems) {
    invoices.push({
      MaHD: maHD,
      ChiNhanh: payload.branch,
      MaNV: payload.employeeId || defaultEmployeeId(payload.branch),
      MaSP: item.productCode,
      SoLuong: item.quantity,
      TongTien: item.totalAmount,
      GhiChu: payload.note,
      NgayTao: createdAt,
      TenHang: item.productName,
      DonGia: Number(item.unitPrice || 0),
    });
  }

  const totalAmount = mergedItems.reduce(
    (sum, item) => sum + Number(item.totalAmount || 0),
    0,
  );
  const firstItem = mergedItems[0];

  return {
    MaHD: maHD,
    ChiNhanh: payload.branch,
    MaNV: payload.employeeId || defaultEmployeeId(payload.branch),
    GhiChu: payload.note,
    NgayTao: createdAt,
    TongTien: totalAmount,
    items: mergedItems,
    MaSP: firstItem.productCode,
    SoLuong: firstItem.quantity,
    TenHang: firstItem.productName,
    DonGia: Number(firstItem.unitPrice || 0),
  };
}

function listInvoicesByBranch(branch) {
  const branchInvoices = invoices.filter((item) => item.ChiNhanh === branch);
  const groups = new Map();

  for (const item of branchInvoices) {
    if (!groups.has(item.MaHD)) {
      groups.set(item.MaHD, {
        MaHD: item.MaHD,
        TongTien: 0,
        SoMon: 0,
        GhiChu: item.GhiChu,
        NgayTao: item.NgayTao,
        ChiNhanh: item.ChiNhanh,
        MaNV: item.MaNV,
        HoTenNhanVien: null, // Would fetch from employees if we cared
      });
    }
    const g = groups.get(item.MaHD);
    g.TongTien += Number(item.SoLuong || 0) * Number(item.DonGia || 0);
    g.SoMon += 1;
  }
  
  return Array.from(groups.values()).sort((a, b) => new Date(b.NgayTao) - new Date(a.NgayTao));
}

function getInvoiceDetailsLocal(branch, invoiceId) {
  return invoices
    .filter((item) => item.ChiNhanh === branch && item.MaHD === invoiceId)
    .map(ctd => ({
      MaHD: ctd.MaHD,
      MaSP: ctd.MaSP,
      TenHang: ctd.TenHang,
      SoLuong: ctd.SoLuong,
      DonGia: ctd.DonGia,
      ThanhTien: Number(ctd.SoLuong || 0) * Number(ctd.DonGia || 0)
    }));
}

function updateInvoiceLocal(branch, invoiceId, payload) {
  const index = invoices.findIndex(
    (item) => item.MaHD === invoiceId && item.ChiNhanh === branch,
  );
  if (index < 0) {
    throw new Error(`Invoice ${invoiceId} not found in ${branch}`);
  }
  const currentRows = invoices.filter(
    (item) => item.MaHD === invoiceId && item.ChiNhanh === branch,
  );
  const baseRow = currentRows[0];
  const nextMaSP = payload.productCode || baseRow.MaSP;
  const prevProduct = products[nextMaSP] || { TenHang: nextMaSP, Gia: 0 };
  const nextUnitPrice =
    payload.unitPrice > 0
      ? Number(payload.unitPrice)
      : payload.totalAmount > 0 && payload.quantity > 0
        ? Number(payload.totalAmount) / Number(payload.quantity)
        : Number(prevProduct.Gia || 0);

  products[nextMaSP] = {
    TenHang: payload.productName || prevProduct.TenHang,
    Gia: nextUnitPrice,
  };

  const next = {
    ...baseRow,
    MaNV: payload.employeeId || baseRow.MaNV || null,
    MaSP: nextMaSP,
    SoLuong: payload.quantity > 0 ? payload.quantity : baseRow.SoLuong,
    TongTien:
      payload.totalAmount > 0 ? payload.totalAmount : baseRow.TongTien,
    GhiChu: payload.note ?? baseRow.GhiChu,
    TenHang: products[nextMaSP].TenHang,
    DonGia: Number(products[nextMaSP].Gia || 0),
  };
  for (let i = invoices.length - 1; i >= 0; i -= 1) {
    if (invoices[i].MaHD === invoiceId && invoices[i].ChiNhanh === branch) {
      invoices.splice(i, 1);
    }
  }
  invoices.push(next);
  return next;
}

function deleteInvoiceLocal(branch, invoiceId) {
  const index = invoices.findIndex(
    (item) => item.MaHD === invoiceId && item.ChiNhanh === branch,
  );
  if (index < 0) {
    throw new Error(`Invoice ${invoiceId} not found in ${branch}`);
  }
  const deleted = invoices[index];
  for (let i = invoices.length - 1; i >= 0; i -= 1) {
    if (invoices[i].MaHD === invoiceId && invoices[i].ChiNhanh === branch) {
      invoices.splice(i, 1);
    }
  }
  return deleted;
}

function revenueReport() {
  const byBranch = ["HUE", "SAIGON", "HANOI"].map((branch) => {
    const total = invoices
      .filter((item) => item.ChiNhanh === branch)
      .reduce((sum, item) => sum + Number(item.TongTien || 0), 0);
    return { branch, revenue: total };
  });

  const nationalRevenue = byBranch.reduce((sum, item) => sum + item.revenue, 0);
  return {
    mode: "MOCK",
    generatedAt: new Date().toISOString(),
    byBranch,
    nationalRevenue,
    invoiceCount: invoices.length,
  };
}

function getInventory(branch, productCode) {
  const value = Number((inventory[branch] || {})[productCode] || 0);
  return {
    branch,
    productCode,
    quantity: value,
  };
}

function listInventory(branch) {
  const bucket = inventory[branch] || {};
  return Object.keys(bucket).map((productCode) => ({
    branch,
    productCode,
    quantity: Number(bucket[productCode] || 0),
  }));
}

function getProductByCode(branch, productCode) {
  const code = String(productCode || "").trim();
  const product = products[code];
  if (!product) {
    return null;
  }
  return {
    branch,
    productCode: code,
    productName: product.TenHang,
    unitPrice: Number(product.Gia || 0),
  };
}

function createInventoryItem(branch, payload) {
  const bucket = inventory[branch] || (inventory[branch] = {});
  if (bucket[payload.productCode] !== undefined) {
    throw new Error(
      `Product ${payload.productCode} already exists in ${branch}`,
    );
  }
  bucket[payload.productCode] = payload.quantity;
  return {
    branch,
    productCode: payload.productCode,
    quantity: payload.quantity,
  };
}

function updateInventoryItem(branch, productCode, quantity) {
  const bucket = inventory[branch] || (inventory[branch] = {});
  if (bucket[productCode] === undefined) {
    throw new Error(`Product ${productCode} not found in ${branch}`);
  }
  bucket[productCode] = quantity;
  return {
    branch,
    productCode,
    quantity,
  };
}

function deleteInventoryItem(branch, productCode) {
  const bucket = inventory[branch] || {};
  if (bucket[productCode] === undefined) {
    throw new Error(`Product ${productCode} not found in ${branch}`);
  }
  const deleted = {
    branch,
    productCode,
    quantity: Number(bucket[productCode] || 0),
  };
  delete bucket[productCode];
  return deleted;
}

function branchDashboard(branch) {
  const branchEmployees = listEmployeesByBranch(branch);
  const branchInvoices = listInvoicesByBranch(branch);
  const branchInventory = listInventory(branch);
  const revenue = branchInvoices.reduce(
    (sum, item) => sum + Number(item.TongTien || 0),
    0,
  );
  const totalStockUnits = branchInventory.reduce(
    (sum, item) => sum + Number(item.quantity || 0),
    0,
  );
  const lowStockProducts = branchInventory.filter(
    (item) => Number(item.quantity || 0) < 50,
  ).length;
  return {
    mode: "MOCK",
    branch,
    employeeCount: branchEmployees.length,
    invoiceCount: branchInvoices.length,
    revenue,
    totalStockUnits,
    lowStockProducts,
    generatedAt: new Date().toISOString(),
  };
}

function transferStock(payload) {
  const fromQty = Number(
    (inventory[payload.fromBranch] || {})[payload.productCode] || 0,
  );
  const toQty = Number(
    (inventory[payload.toBranch] || {})[payload.productCode] || 0,
  );

  if (fromQty < payload.quantity) {
    throw new Error(
      `Insufficient stock in ${payload.fromBranch}: current ${fromQty}, requested ${payload.quantity}`,
    );
  }

  const before = {
    from: fromQty,
    to: toQty,
  };

  // Simulate 2-phase update in memory as a distributed transaction demo.
  inventory[payload.fromBranch][payload.productCode] =
    fromQty - payload.quantity;
  inventory[payload.toBranch][payload.productCode] = toQty + payload.quantity;

  const after = {
    from: inventory[payload.fromBranch][payload.productCode],
    to: inventory[payload.toBranch][payload.productCode],
  };

  return {
    mode: "MOCK",
    status: "COMMITTED",
    transactionType: "BEGIN DISTRIBUTED TRANSACTION (simulated)",
    productCode: payload.productCode,
    quantity: payload.quantity,
    fromBranch: payload.fromBranch,
    toBranch: payload.toBranch,
    before,
    after,
    committedAt: new Date().toISOString(),
  };
}

module.exports = {
  listEmployeesByBranch,
  createEmployee,
  updateEmployee,
  deleteEmployee,
  listAllEmployees,
  listInvoicesByBranch,
  getInvoiceDetailsLocal,
  createInvoiceLocal,
  updateInvoiceLocal,
  deleteInvoiceLocal,
  revenueReport,
  getInventory,
  listInventory,
  getProductByCode,
  createInventoryItem,
  updateInventoryItem,
  deleteInventoryItem,
  branchDashboard,
  transferStock,
};
