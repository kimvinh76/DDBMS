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
  transferStockDistributed,
  listInventory,
  createInventoryItem,
  updateInventoryItem,
  deleteInventoryItem,
  getBranchDashboard,
} = require("./src/services/store-service");

const app = express();
const port = Number(process.env.PORT || 3000);

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

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

app.post("/api/invoices", async (req, res) => {
  try {
    const branch = normalizeBranch(req.body.branch);
    if (!branch || isCentralBranch(branch)) {
      return res.status(400).json({
        message: "branch is required and must be HUE, SAIGON, or HANOI",
      });
    }

    const payload = {
      branch,
      productCode: String(req.body.productCode || "").trim(),
      quantity: Number(req.body.quantity || 0),
      totalAmount: Number(req.body.totalAmount || 0),
      note: String(req.body.note || "").trim(),
    };

    if (
      !payload.productCode ||
      payload.quantity <= 0 ||
      payload.totalAmount <= 0
    ) {
      return res.status(400).json({
        message:
          "productCode, quantity and totalAmount are required and must be positive",
      });
    }

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
      quantity: Number(req.body.quantity || 0),
      totalAmount: Number(req.body.totalAmount || 0),
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

app.post("/api/inventory", async (req, res) => {
  try {
    const branch = normalizeBranch(req.body.branch);
    const payload = {
      productCode: String(req.body.productCode || "").trim(),
      quantity: Number(req.body.quantity || 0),
    };
    if (!branch || isCentralBranch(branch)) {
      return res
        .status(400)
        .json({
          message: "branch is required and must be HUE, SAIGON, or HANOI",
        });
    }
    if (!payload.productCode || payload.quantity < 0) {
      return res
        .status(400)
        .json({ message: "productCode is required and quantity must be >= 0" });
    }
    const data = await createInventoryItem(branch, payload);
    return res.status(201).json({ message: "Inventory item created", data });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
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
  try {
    const branch = normalizeBranch(req.query.branch);
    const productCode = String(req.params.productCode || "").trim();
    if (!branch || isCentralBranch(branch)) {
      return res
        .status(400)
        .json({
          message: "branch is required and must be HUE, SAIGON, or HANOI",
        });
    }
    if (!productCode) {
      return res.status(400).json({ message: "productCode is required" });
    }
    const data = await deleteInventoryItem(branch, productCode);
    return res.json({ message: "Inventory deleted", data });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
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
