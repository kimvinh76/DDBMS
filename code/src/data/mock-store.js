const employees = {
  HUE: [
    {
      MaNV: "H001",
      HoTen: "Nguyen Van A",
      ChiNhanh: "HUE",
      ChucVu: "Thu ngan",
    },
    { MaNV: "H002", HoTen: "Le Van B", ChiNhanh: "HUE", ChucVu: "Quan ly kho" },
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
  ],
  HANOI: [
    { MaNV: "N001", HoTen: "Do Thi E", ChiNhanh: "HANOI", ChucVu: "Ban hang" },
    {
      MaNV: "N002",
      HoTen: "Hoang Van F",
      ChiNhanh: "HANOI",
      ChucVu: "Quan ly cua hang",
    },
  ],
};

const inventory = {
  HUE: {
    MI_GOI: 220,
    SUA_HOP: 80,
  },
  SAIGON: {
    MI_GOI: 110,
    SUA_HOP: 120,
  },
  HANOI: {
    MI_GOI: 170,
    SUA_HOP: 60,
  },
};

const invoices = [
  {
    MaHD: "HD_INIT_HUE_01",
    ChiNhanh: "HUE",
    MaSP: "MI_GOI",
    SoLuong: 10,
    TongTien: 120000,
    GhiChu: "Hoa don khoi tao",
    NgayTao: new Date().toISOString(),
  },
  {
    MaHD: "HD_INIT_SG_01",
    ChiNhanh: "SAIGON",
    MaSP: "SUA_HOP",
    SoLuong: 8,
    TongTien: 96000,
    GhiChu: "Hoa don khoi tao",
    NgayTao: new Date().toISOString(),
  },
];

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

function createInvoiceLocal(payload) {
  const invoice = {
    MaHD: `HD_${Date.now()}`,
    ChiNhanh: payload.branch,
    MaSP: payload.productCode,
    SoLuong: payload.quantity,
    TongTien: payload.totalAmount,
    GhiChu: payload.note,
    NgayTao: new Date().toISOString(),
  };
  invoices.push(invoice);
  return invoice;
}

function listInvoicesByBranch(branch) {
  return invoices.filter((item) => item.ChiNhanh === branch);
}

function updateInvoiceLocal(branch, invoiceId, payload) {
  const index = invoices.findIndex(
    (item) => item.MaHD === invoiceId && item.ChiNhanh === branch,
  );
  if (index < 0) {
    throw new Error(`Invoice ${invoiceId} not found in ${branch}`);
  }
  const next = {
    ...invoices[index],
    MaSP: payload.productCode || invoices[index].MaSP,
    SoLuong: payload.quantity > 0 ? payload.quantity : invoices[index].SoLuong,
    TongTien:
      payload.totalAmount > 0 ? payload.totalAmount : invoices[index].TongTien,
    GhiChu: payload.note ?? invoices[index].GhiChu,
  };
  invoices[index] = next;
  return next;
}

function deleteInvoiceLocal(branch, invoiceId) {
  const index = invoices.findIndex(
    (item) => item.MaHD === invoiceId && item.ChiNhanh === branch,
  );
  if (index < 0) {
    throw new Error(`Invoice ${invoiceId} not found in ${branch}`);
  }
  const [deleted] = invoices.splice(index, 1);
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
  createInvoiceLocal,
  updateInvoiceLocal,
  deleteInvoiceLocal,
  revenueReport,
  getInventory,
  listInventory,
  createInventoryItem,
  updateInventoryItem,
  deleteInventoryItem,
  branchDashboard,
  transferStock,
};
