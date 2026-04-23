const path = require("path");
const express = require("express");
const cors = require("cors");
require("dotenv").config();

const {
  normalizeBranch,
  isCentralBranch,
  supportedBranches,
} = require("./src/config/branches");
const {
  listEmployeesByBranch,
  createEmployee,
  updateEmployee,
  deleteEmployee,
  listInvoicesByBranch,
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
  updateInventoryItem,
  getBranchDashboard,
} = require("./src/services/store-service");

const app = express();
const port = Number(process.env.PORT || 3000);

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

app.use((req, _res, next) => {
  const readMode = String(process.env.READONLY_USE_SP || "0") === "1" ? "SP" : "DIRECT";
  const employeeWriteMode = String(process.env.EMPLOYEE_WRITE_USE_SP || "0") === "1" ? "SP" : "DIRECT";
  const inventoryMode = String(process.env.INVENTORY_IMPORT_USE_SP || "0") === "1" ? "SP" : "DIRECT";
  const invoiceMode = String(process.env.INVOICE_CREATE_USE_SP || "0") === "1" ? "SP" : "DIRECT";
  const invoiceBranches = String(process.env.INVOICE_CREATE_SP_BRANCHES || "")
    .split(",")
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean)
    .join(",");



  next();
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "distributed-store-demo",
    mode: process.env.MOCK_MODE !== "false" ? "MOCK" : "SQL_SERVER",
    branches: supportedBranches(),
  });
});

app.get("/api/employees", async (req, res) => {
  try {
    const branch = normalizeBranch(req.query.branch);
    if (!branch || isCentralBranch(branch)) {
      return res.status(400).json({
        message: "branch is required and must be HUE, SAIGON, or HANOI",
      });
    }

    const rows = await listEmployeesByBranch(branch);
    return res.json({
      branch,
      count: rows.length,
      data: rows,
    });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.post("/api/employees", async (req, res) => {
  try {
    const branch = normalizeBranch(req.body.branch);
    if (!branch || isCentralBranch(branch)) {
      return res
        .status(400)
        .json({
          message: "branch is required and must be HUE, SAIGON, or HANOI",
        });
    }
    const payload = {
      MaNV: String(req.body.MaNV || "").trim() || null,
      HoTen: String(req.body.HoTen || "").trim(),
      ChucVu: String(req.body.ChucVu || "").trim(),
    };
    if (!payload.HoTen || !payload.ChucVu) {
      return res.status(400).json({ message: "HoTen and ChucVu are required" });
    }
    const data = await createEmployee(branch, payload);
    return res.status(201).json({ message: "Employee created", data });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.put("/api/employees/:employeeId", async (req, res) => {
  try {
    const branch = normalizeBranch(req.query.branch);
    const employeeId = String(req.params.employeeId || "").trim();
    if (!branch || isCentralBranch(branch)) {
      return res
        .status(400)
        .json({
          message: "branch is required and must be HUE, SAIGON, or HANOI",
        });
    }
    if (!employeeId) {
      return res.status(400).json({ message: "employeeId is required" });
    }

    const payload = {
      HoTen: req.body.HoTen ? String(req.body.HoTen).trim() : null,
      ChucVu: req.body.ChucVu ? String(req.body.ChucVu).trim() : null,
    };
    if (!payload.HoTen && !payload.ChucVu) {
      return res
        .status(400)
        .json({ message: "at least HoTen or ChucVu is required to update" });
    }

    const data = await updateEmployee(branch, employeeId, payload);
    return res.json({ message: "Employee updated", data });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.delete("/api/employees/:employeeId", async (req, res) => {
  try {
    const branch = normalizeBranch(req.query.branch);
    const employeeId = String(req.params.employeeId || "").trim();
    if (!branch || isCentralBranch(branch)) {
      return res
        .status(400)
        .json({
          message: "branch is required and must be HUE, SAIGON, or HANOI",
        });
    }
    if (!employeeId) {
      return res.status(400).json({ message: "employeeId is required" });
    }
    const data = await deleteEmployee(branch, employeeId);
    return res.json({ message: "Employee deleted", data });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.get("/api/invoices", async (req, res) => {
  try {
    const branch = normalizeBranch(req.query.branch);
    if (!branch || isCentralBranch(branch)) {
      return res.status(400).json({
        message: "branch is required and must be HUE, SAIGON, or HANOI",
      });
    }
    const rows = await listInvoicesByBranch(branch);
    return res.json({
      branch,
      count: rows.length,
      data: rows,
    });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.get("/api/invoices/:invoiceId/details", async (req, res) => {
  try {
    const { invoiceId } = req.params;
    const branch = normalizeBranch(req.query.branch);
    if (!branch || isCentralBranch(branch)) {
      return res.status(400).json({
        message: "branch is required and must be HUE, SAIGON, or HANOI",
      });
    }
    const { getInvoiceDetails } = require("./src/services/store-service");
    const rows = await getInvoiceDetails(branch, invoiceId);
    return res.json({
      branch,
      invoiceId,
      data: rows,
    });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.post("/api/invoices", async (req, res) => {
  try {
    const branch = normalizeBranch(req.body.branch);
    if (!branch || isCentralBranch(branch)) {
      return res.status(400).json({
        message: "branch is required and must be HUE, SAIGON, or HANOI",
      });
    }

    const rawItems = Array.isArray(req.body.items) ? req.body.items : [];
    const parsedItems = rawItems
      .map((item) => ({
        productCode: String(item?.productCode || "").trim(),
        productName: item?.productName ? String(item.productName).trim() : "",
        unitPrice: Number(item?.unitPrice || 0),
        quantity: Number(item?.quantity || 0),
        totalAmount: Number(item?.totalAmount || 0),
      }))
      .filter((item) => item.productCode || item.quantity || item.unitPrice);

    const fallbackSingleItem = {
      productCode: String(req.body.productCode || "").trim(),
      productName: req.body.productName
        ? String(req.body.productName).trim()
        : "",
      unitPrice: Number(req.body.unitPrice || 0),
      quantity: Number(req.body.quantity || 0),
      totalAmount: Number(req.body.totalAmount || 0),
    };

    const items = parsedItems.length
      ? parsedItems
      : fallbackSingleItem.productCode
        ? [fallbackSingleItem]
        : [];

    if (!items.length) {
      return res.status(400).json({
        message: "items is required and must contain at least one line",
      });
    }

    if (items.some((item) => !item.productCode || item.quantity <= 0)) {
      return res.status(400).json({
        message: "Each item must include productCode and quantity > 0",
      });
    }

    const payload = {
      branch,
      employeeId: req.body.employeeId
        ? String(req.body.employeeId).trim()
        : "",
      items,
      totalAmount: Number(req.body.totalAmount || 0),
      note: String(req.body.note || "").trim(),
    };

    const created = await createInvoice(payload);
    return res.status(201).json({
      message: "Invoice created in local fragment successfully",
      data: created,
    });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.put("/api/invoices/:invoiceId", async (req, res) => {
  try {
    const branch = normalizeBranch(req.query.branch);
    const invoiceId = String(req.params.invoiceId || "").trim();
    if (!branch || isCentralBranch(branch)) {
      return res
        .status(400)
        .json({
          message: "branch is required and must be HUE, SAIGON, or HANOI",
        });
    }
    if (!invoiceId) {
      return res.status(400).json({ message: "invoiceId is required" });
    }

    const payload = {
      productCode: req.body.productCode
        ? String(req.body.productCode).trim()
        : "",
      employeeId: req.body.employeeId
        ? String(req.body.employeeId).trim()
        : "",
      quantity: Number(req.body.quantity || 0),
      totalAmount: Number(req.body.totalAmount || 0),
      productName: req.body.productName
        ? String(req.body.productName).trim()
        : "",
      unitPrice: Number(req.body.unitPrice || 0),
      note: req.body.note !== undefined ? String(req.body.note).trim() : null,
    };

    const data = await updateInvoice(branch, invoiceId, payload);
    return res.json({ message: "Invoice updated", data });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.delete("/api/invoices/:invoiceId", async (req, res) => {
  try {
    const branch = normalizeBranch(req.query.branch);
    const invoiceId = String(req.params.invoiceId || "").trim();
    if (!branch || isCentralBranch(branch)) {
      return res
        .status(400)
        .json({
          message: "branch is required and must be HUE, SAIGON, or HANOI",
        });
    }
    if (!invoiceId) {
      return res.status(400).json({ message: "invoiceId is required" });
    }

    const data = await deleteInvoice(branch, invoiceId);
    return res.json({ message: "Invoice deleted", data });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.get("/api/branch-dashboard", async (req, res) => {
  try {
    const branch = normalizeBranch(req.query.branch);
    if (!branch || isCentralBranch(branch)) {
      return res
        .status(400)
        .json({
          message: "branch is required and must be HUE, SAIGON, or HANOI",
        });
    }
    const data = await getBranchDashboard(branch);
    return res.json(data);
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.get("/api/all-employees", async (req, res) => {
  try {
    const branch = normalizeBranch(req.query.branch);
    if (!branch || !isCentralBranch(branch)) {
      return res.status(400).json({ message: "branch must be CENTRAL" });
    }

    const rows = await listAllEmployeesFromCentral();
    return res.json({
      branch,
      count: rows.length,
      data: rows,
    });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.get("/api/revenue/national", async (req, res) => {
  try {
    const branch = normalizeBranch(req.query.branch);
    if (!branch || !isCentralBranch(branch)) {
      return res.status(400).json({ message: "branch must be CENTRAL" });
    }

    const report = await getNationalRevenue();
    return res.json(report);
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.get("/api/analytics/overview", async (req, res) => {
  try {
    const branch = normalizeBranch(req.query.branch);
    if (!branch || !isCentralBranch(branch)) {
      return res.status(400).json({ message: "branch must be CENTRAL" });
    }

    const sourceBranch = req.query.sourceBranch
      ? String(req.query.sourceBranch).trim().toUpperCase()
      : undefined;

    const report = await getCentralAnalyticsOverview(sourceBranch);
    return res.json(report);
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.get("/api/inventory", async (req, res) => {
  try {
    const branch = normalizeBranch(req.query.branch);
    const productCode = req.query.productCode
      ? String(req.query.productCode).trim()
      : "";

    if (!branch || isCentralBranch(branch)) {
      return res.status(400).json({
        message: "branch is required and must be HUE, SAIGON, or HANOI",
      });
    }
    const result = await listInventory(branch, productCode);
    return res.json(result);
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.get("/api/products", async (req, res) => {
  try {
    const data = await listProducts();
    return res.json(data);
  } catch (error) {
    if (error.message.includes("Login failed")) {
      return res.status(401).json({ message: error.message });
    }
    return res.status(500).json({ message: error.message });
  }
});

app.post("/api/products", async (req, res) => {
  try {
    const branch = normalizeBranch(req.body?.branch || req.query?.branch);
    if (!branch || !isCentralBranch(branch)) {
      return res.status(403).json({ message: "Only CENTRAL can create products" });
    }
    const payload = {
      productCode: req.body.productCode,
      productName: req.body.productName,
      unitPrice: req.body.unitPrice,
    };
    if (!payload.productCode) {
      return res.status(400).json({ message: "productCode is required" });
    }
    const data = await createProduct(payload);
    return res.status(201).json({ message: "Product created", data });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.put("/api/products/:productCode", async (req, res) => {
  try {
    const branch = normalizeBranch(req.body?.branch || req.query?.branch);
    if (!branch || !isCentralBranch(branch)) {
      return res.status(403).json({ message: "Only CENTRAL can update products" });
    }
    const code = req.params.productCode;
    const payload = {
      productName: req.body.productName,
      unitPrice: req.body.unitPrice,
    };
    const data = await updateProduct(code, payload);
    return res.json({ message: "Product updated", data });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.delete("/api/products/:productCode", async (req, res) => {
  try {
    const branch = normalizeBranch(req.body?.branch || req.query?.branch);
    if (!branch || !isCentralBranch(branch)) {
      return res.status(403).json({ message: "Only CENTRAL can delete products" });
    }
    const code = req.params.productCode;
    const data = await deleteProduct(code);
    return res.json({ message: "Product deleted", data });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.get("/api/products/:productCode", async (req, res) => {
  try {
    const branch = normalizeBranch(req.query.branch);
    const productCode = String(req.params.productCode || "").trim();
    if (!branch || isCentralBranch(branch)) {
      return res.status(400).json({
        message: "branch is required and must be HUE, SAIGON, or HANOI",
      });
    }
    if (!productCode) {
      return res.status(400).json({ message: "productCode is required" });
    }

    const product = await getProductByCode(branch, productCode);
    if (!product) {
      return res.status(404).json({ message: `Product ${productCode} not found in ${branch}` });
    }

    return res.json(product);
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.post("/api/inventory", async (req, res) => {
  return res.status(405).json({
    message:
      "Create inventory is disabled. Inventory rows are auto-created with quantity 0 when product is created at Central.",
  });
});

app.put("/api/inventory/:productCode", async (req, res) => {
  try {
    const branch = normalizeBranch(req.query.branch);
    const productCode = String(req.params.productCode || "").trim();
    const quantity = Number(req.body.quantity || 0);
    if (!branch || isCentralBranch(branch)) {
      return res
        .status(400)
        .json({
          message: "branch is required and must be HUE, SAIGON, or HANOI",
        });
    }
    if (!productCode || quantity < 0) {
      return res
        .status(400)
        .json({ message: "productCode is required and quantity must be >= 0" });
    }
    const data = await updateInventoryItem(branch, productCode, quantity);
    return res.json({ message: "Inventory updated", data });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.delete("/api/inventory/:productCode", async (req, res) => {
  return res.status(405).json({
    message: "Delete inventory is disabled. Use product management at Central instead.",
  });
});

app.post("/api/transfer-stock", async (req, res) => {
  try {
    const branch = normalizeBranch(req.body.branch);
    if (!branch || !isCentralBranch(branch)) {
      return res.status(400).json({ message: "branch must be CENTRAL" });
    }

    const payload = {
      fromBranch: normalizeBranch(req.body.fromBranch),
      toBranch: normalizeBranch(req.body.toBranch),
      productCode: String(req.body.productCode || "").trim(),
      quantity: Number(req.body.quantity || 0),
    };

    if (
      !payload.fromBranch ||
      !payload.toBranch ||
      isCentralBranch(payload.fromBranch) ||
      isCentralBranch(payload.toBranch)
    ) {
      return res.status(400).json({
        message:
          "fromBranch and toBranch are required and must be HUE/SAIGON/HANOI",
      });
    }
    if (payload.fromBranch === payload.toBranch) {
      return res
        .status(400)
        .json({ message: "fromBranch and toBranch must be different" });
    }
    if (!payload.productCode || payload.quantity <= 0) {
      return res
        .status(400)
        .json({ message: "productCode and positive quantity are required" });
    }

    const result = await transferStockDistributed(payload);
    return res.json(result);
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
});

app.use((_req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.listen(port, () => {
  console.log(`Distributed store demo running at http://localhost:${port}`);
});
