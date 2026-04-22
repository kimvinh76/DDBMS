const BRANCH_LABELS = {
  HUE: "Chi nhánh Huế (Port 1401)",
  SAIGON: "Chi nhánh Sài Gòn (Port 1402)",
  HANOI: "Chi nhánh Hà Nội (Port 1403)",
  CENTRAL: "Tổng công ty (Port 1404)",
};

const chartRegistry = {};
const page = document.body.dataset.page;

function getCurrentBranch() {
  return localStorage.getItem("current_branch");
}

function setCurrentBranch(branch) {
  localStorage.setItem("current_branch", branch);
}

function logout() {
  localStorage.removeItem("current_branch");
  window.location.href = "/";
}

function navigateByBranch(branch) {
  window.location.href =
    branch === "CENTRAL" ? "/central-overview.html" : "/branch-dashboard.html";
}

function ensureLocalBranch() {
  const branch = getCurrentBranch();
  if (!branch || branch === "CENTRAL") {
    window.location.href = "/";
    return null;
  }
  return branch;
}

function ensureCentralBranch() {
  const branch = getCurrentBranch();
  if (branch !== "CENTRAL") {
    window.location.href = "/";
    return false;
  }
  return true;
}

function setupBranchShell(branch) {
  const branchTitle = document.getElementById("branchTitle");
  if (branchTitle) {
    branchTitle.textContent = BRANCH_LABELS[branch] || branch;
  }
  const logoutBtn = document.getElementById("logoutBtn");
  if (logoutBtn) {
    logoutBtn.addEventListener("click", logout);
  }
}

function setupCentralShell() {
  const logoutBtn = document.getElementById("logoutBtn");
  if (logoutBtn) {
    logoutBtn.addEventListener("click", logout);
  }
}

function setupCrudSwitcher(switchId) {
  const switcher = document.getElementById(switchId);
  if (!switcher) {
    return;
  }

  const card = switcher.closest(".card") || document;
  const tabs = Array.from(switcher.querySelectorAll("[data-crud-target]"));
  const panels = Array.from(card.querySelectorAll("[data-crud-panel]"));

  function activate(target) {
    tabs.forEach((tab) => {
      tab.classList.toggle("active", tab.dataset.crudTarget === target);
    });
    panels.forEach((panel) => {
      panel.classList.toggle("active", panel.dataset.crudPanel === target);
    });
  }

  tabs.forEach((tab) => {
    tab.addEventListener("click", () => activate(tab.dataset.crudTarget));
  });

  if (tabs[0]) {
    activate(tabs[0].dataset.crudTarget);
  }
}

function resetChart(chartId) {
  if (chartRegistry[chartId]) {
    chartRegistry[chartId].destroy();
    delete chartRegistry[chartId];
  }
}

function renderChart(chartId, config) {
  if (!window.Chart) {
    return;
  }
  const canvas = document.getElementById(chartId);
  if (!canvas) {
    return;
  }
  resetChart(chartId);
  chartRegistry[chartId] = new window.Chart(canvas.getContext("2d"), config);
}

function formatTableCellValue(columnName, value) {
  if (value === null || value === undefined) {
    return "";
  }

  const normalized = String(columnName || "").toLowerCase();
  const shouldFormatDate = normalized === "ngaytao";
  if (!shouldFormatDate) {
    return value;
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  const pad2 = (num) => String(num).padStart(2, "0");

  // SQL DATETIME/DATETIME2 has no timezone. Avoid applying timezone conversion twice.
  const day = pad2(date.getUTCDate());
  const month = pad2(date.getUTCMonth() + 1);
  const year = date.getUTCFullYear();
  const hour = pad2(date.getUTCHours());
  const minute = pad2(date.getUTCMinutes());
  const second = pad2(date.getUTCSeconds());

  return `${day}/${month}/${year} ${hour}:${minute}:${second}`;
}

function renderTable(wrapper, rows, options = {}) {
  if (!rows || rows.length === 0) {
    wrapper.innerHTML = `<p class="subtitle">${options.emptyMessage || "Không có dữ liệu."}</p>`;
    return;
  }

  const tableId =
    options.tableId || `tb_${Math.random().toString(36).slice(2)}`;
  const headers = Object.keys(rows[0]);
  const thead = `<thead><tr>${headers.map((h) => `<th>${h}</th>`).join("")}</tr></thead>`;
  const tbody = `<tbody>${rows
    .map(
      (row, index) =>
        `<tr data-table-id="${tableId}" data-row-index="${index}">${headers.map((h) => `<td>${formatTableCellValue(h, row[h])}</td>`).join("")}</tr>`,
    )
    .join("")}</tbody>`;

  wrapper.innerHTML = `<table class="selectable-table">${thead}${tbody}</table>`;

  if (typeof options.onRowSelect === "function") {
    const renderedRows = wrapper.querySelectorAll(
      `tr[data-table-id="${tableId}"]`,
    );
    renderedRows.forEach((rowEl) => {
      rowEl.addEventListener("click", () => {
        renderedRows.forEach((item) => item.classList.remove("is-selected"));
        rowEl.classList.add("is-selected");
        const index = Number(rowEl.dataset.rowIndex || 0);
        options.onRowSelect(rows[index], index);
      });
    });
  }
}

function hideColumns(rows, columnNames = []) {
  if (!Array.isArray(rows) || rows.length === 0 || columnNames.length === 0) {
    return rows;
  }

  const hidden = new Set(columnNames.map((name) => String(name).toLowerCase()));
  return rows.map((row) =>
    Object.fromEntries(
      Object.entries(row).filter(
        ([column]) => !hidden.has(String(column).toLowerCase()),
      ),
    ),
  );
}

async function fetchJSON(url, options) {
  const response = await fetch(url, options);
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.message || "Request failed");
  }
  return data;
}

function formatNumber(value) {
  return new Intl.NumberFormat("vi-VN").format(Number(value || 0));
}

function attachAutoTotal(form) {
  if (!form || !form.elements.unitPrice || !form.elements.quantity || !form.elements.totalAmount) {
    return;
  }

  const recompute = () => {
    const unitPrice = Number(form.elements.unitPrice.value || 0);
    const quantity = Number(form.elements.quantity.value || 0);
    form.elements.totalAmount.value =
      unitPrice > 0 && quantity > 0 ? String(unitPrice * quantity) : "";
  };

  form.elements.unitPrice.addEventListener("input", recompute);
  form.elements.quantity.addEventListener("input", recompute);
  recompute();
}

function attachInvoiceLineItems(form, resolveBranch, resultBox) {
  if (!form) {
    return {
      collectItems: () => [],
      resetItems: () => {},
      recomputeTotal: () => {},
    };
  }

  const itemsWrap = form.querySelector("[data-invoice-items]");
  const addBtn = form.querySelector("[data-add-invoice-item]");
  const totalInput = form.elements.totalAmount;
  if (!itemsWrap || !addBtn || !totalInput) {
    return {
      collectItems: () => [],
      resetItems: () => {},
      recomputeTotal: () => {},
    };
  }

  const padRowHtml = () => `
    <div class="invoice-item-row compact-top" data-invoice-item-row>
      <label class="invoice-item-field">
        Mã sản phẩm
        <input name="itemProductCode" placeholder="VD: MI_GOI" required />
      </label>
      <label class="invoice-item-field">
        Tên hàng
        <input name="itemProductName" placeholder="Tự động theo MaSP" readonly />
      </label>
      <label class="invoice-item-field">
        Đơn giá
        <input type="number" name="itemUnitPrice" min="0" />
      </label>
      <label class="invoice-item-field">
        Số lượng
        <input type="number" name="itemQuantity" min="1" required />
      </label>
      <div class="invoice-item-actions">
        <button type="button" class="danger" data-remove-invoice-item>Xóa dòng</button>
      </div>
    </div>
  `;

  const recomputeTotal = () => {
    const rows = Array.from(itemsWrap.querySelectorAll("[data-invoice-item-row]"));
    const total = rows.reduce((sum, row) => {
      const unitPrice = Number(row.querySelector('[name="itemUnitPrice"]')?.value || 0);
      const quantity = Number(row.querySelector('[name="itemQuantity"]')?.value || 0);
      return sum + (unitPrice > 0 && quantity > 0 ? unitPrice * quantity : 0);
    }, 0);
    totalInput.value = total > 0 ? String(total) : "";
  };

  const fillRowProduct = async (row) => {
    const productCode = String(row.querySelector('[name="itemProductCode"]')?.value || "").trim();
    const branch = String(resolveBranch() || "").trim();
    if (!productCode || !branch) {
      return;
    }

    try {
      const product = await fetchJSON(
        `/api/products/${encodeURIComponent(productCode)}?branch=${branch}`,
      );
      const nameInput = row.querySelector('[name="itemProductName"]');
      const priceInput = row.querySelector('[name="itemUnitPrice"]');
      if (nameInput) {
        nameInput.value = String(product.productName || "");
      }
      if (priceInput) {
        priceInput.value = String(product.unitPrice ?? "");
      }
      recomputeTotal();
    } catch (error) {
      if (String(error.message || "").toLowerCase().includes("not found")) {
        const nameInput = row.querySelector('[name="itemProductName"]');
        const priceInput = row.querySelector('[name="itemUnitPrice"]');
        if (nameInput) {
          nameInput.value = "";
        }
        if (priceInput) {
          priceInput.value = "";
        }
        recomputeTotal();
      }
      if (resultBox) {
        resultBox.textContent = `Lỗi: ${error.message}`;
      }
    }
  };

  const bindRowEvents = (row) => {
    const codeInput = row.querySelector('[name="itemProductCode"]');
    const priceInput = row.querySelector('[name="itemUnitPrice"]');
    const qtyInput = row.querySelector('[name="itemQuantity"]');
    const removeBtn = row.querySelector("[data-remove-invoice-item]");
    let lookupTimer = null;

    if (codeInput) {
      const lookupWithDebounce = () => {
        if (lookupTimer) {
          clearTimeout(lookupTimer);
        }
        lookupTimer = setTimeout(() => {
          fillRowProduct(row).catch(() => {});
        }, 250);
      };

      codeInput.addEventListener("input", lookupWithDebounce);
      codeInput.addEventListener("blur", () => {
        fillRowProduct(row).catch(() => {});
      });
      codeInput.addEventListener("change", () => {
        fillRowProduct(row).catch(() => {});
      });
    }

    if (priceInput) {
      priceInput.addEventListener("input", recomputeTotal);
    }
    if (qtyInput) {
      qtyInput.addEventListener("input", recomputeTotal);
    }

    if (removeBtn) {
      removeBtn.addEventListener("click", () => {
        const rows = itemsWrap.querySelectorAll("[data-invoice-item-row]");
        if (rows.length <= 1) {
          return;
        }
        row.remove();
        recomputeTotal();
      });
    }
  };

  const addRow = () => {
    const holder = document.createElement("div");
    holder.innerHTML = padRowHtml().trim();
    const row = holder.firstElementChild;
    itemsWrap.appendChild(row);
    bindRowEvents(row);
    recomputeTotal();
    return row;
  };

  const collectItems = () => {
    return Array.from(itemsWrap.querySelectorAll("[data-invoice-item-row]"))
      .map((row) => {
        const productCode = String(row.querySelector('[name="itemProductCode"]')?.value || "").trim();
        const productName = String(row.querySelector('[name="itemProductName"]')?.value || "").trim();
        const unitPrice = Number(row.querySelector('[name="itemUnitPrice"]')?.value || 0);
        const quantity = Number(row.querySelector('[name="itemQuantity"]')?.value || 0);
        return {
          productCode,
          productName,
          unitPrice,
          quantity,
          totalAmount: unitPrice * quantity,
        };
      })
      .filter((item) => item.productCode || item.quantity || item.unitPrice);
  };

  const resetItems = () => {
    itemsWrap.innerHTML = "";
    addRow();
    recomputeTotal();
  };

  addBtn.addEventListener("click", () => {
    addRow();
  });

  resetItems();

  return {
    collectItems,
    resetItems,
    recomputeTotal,
  };
}

function attachInvoiceProductAutoFill(form, resolveBranch, resultBox) {
  if (!form || !form.elements.productCode || !form.elements.unitPrice) {
    return;
  }

  const fillFromProductCode = async () => {
    const productCode = String(form.elements.productCode.value || "").trim();
    const branch = String(resolveBranch() || "").trim();
    if (!productCode || !branch) {
      return;
    }

    try {
      const product = await fetchJSON(
        `/api/products/${encodeURIComponent(productCode)}?branch=${branch}`,
      );
      form.elements.unitPrice.value = String(product.unitPrice ?? "");
      if (form.elements.productName) {
        form.elements.productName.value = String(product.productName || "");
      }
      form.elements.unitPrice.dispatchEvent(new Event("input"));
    } catch (error) {
      if (String(error.message || "").toLowerCase().includes("not found")) {
        form.elements.unitPrice.value = "";
        if (form.elements.productName) {
          form.elements.productName.value = "";
        }
        form.elements.unitPrice.dispatchEvent(new Event("input"));
      }
      if (resultBox) {
        resultBox.textContent = `Lỗi: ${error.message}`;
      }
    }
  };

  form.elements.productCode.addEventListener("blur", () => {
    fillFromProductCode().catch(() => {});
  });
  form.elements.productCode.addEventListener("change", () => {
    fillFromProductCode().catch(() => {});
  });
}

function renderStatsGrid(wrapper, stats) {
  const cards = [
    { label: "Nhân viên", value: stats.employeeCount },
    { label: "Hóa đơn", value: stats.invoiceCount },
    { label: "Doanh thu", value: `${formatNumber(stats.revenue)} VND` },
    { label: "Tổng tồn kho", value: formatNumber(stats.totalStockUnits) },
    { label: "Sản phẩm sắp hết", value: formatNumber(stats.lowStockProducts) },
  ];
  wrapper.innerHTML = cards
    .map(
      (item) =>
        `<article class="stat-card"><p>${item.label}</p><h3>${item.value}</h3></article>`,
    )
    .join("");
}

function setupLandingPage() {
  const buttons = document.querySelectorAll("[data-branch]");
  buttons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const branch = btn.dataset.branch;
      setCurrentBranch(branch);
      navigateByBranch(branch);
    });
  });
}

async function setupBranchDashboardPage() {
  const branch = ensureLocalBranch();
  if (!branch) {
    return;
  }
  setupBranchShell(branch);

  const dashboardApiLine = document.getElementById("dashboardApiLine");
  const branchStatsGrid = document.getElementById("branchStatsGrid");
  const refreshBranchDashboard = document.getElementById(
    "refreshBranchDashboard",
  );

  dashboardApiLine.textContent = `GET /api/branch-dashboard?branch=${branch}`;

  async function loadBranchDashboard() {
    const [result, invoicesResp, inventoryRows] = await Promise.all([
      fetchJSON(`/api/branch-dashboard?branch=${branch}`),
      fetchJSON(`/api/invoices?branch=${branch}`),
      fetchJSON(`/api/inventory?branch=${branch}`),
    ]);

    renderStatsGrid(branchStatsGrid, result);

    const invoices = Array.isArray(invoicesResp.data) ? invoicesResp.data : [];
    const inventory = Array.isArray(inventoryRows) ? inventoryRows : [];

    const daily = {};
    for (let i = 6; i >= 0; i -= 1) {
      const d = new Date();
      d.setDate(d.getDate() - i);
      const key = d.toISOString().slice(0, 10);
      daily[key] = 0;
    }

    invoices.forEach((item) => {
      const key = new Date(item.NgayTao || Date.now())
        .toISOString()
        .slice(0, 10);
      if (daily[key] !== undefined) {
        daily[key] += Number(item.TongTien || 0);
      }
    });

    renderChart("branchRevenueTrendChart", {
      type: "line",
      data: {
        labels: Object.keys(daily).map((d) => d.slice(5)),
        datasets: [
          {
            label: "Doanh thu VND",
            data: Object.values(daily),
            borderColor: "#1c7ca0",
            backgroundColor: "rgba(28,124,160,0.2)",
            tension: 0.35,
            fill: true,
          },
        ],
      },
      options: { responsive: true, maintainAspectRatio: false },
    });

    const sortedInventory = [...inventory]
      .sort((a, b) => Number(b.quantity || 0) - Number(a.quantity || 0))
      .slice(0, 8);

    renderChart("branchInventoryChart", {
      type: "bar",
      data: {
        labels: sortedInventory.map((item) => item.productCode),
        datasets: [
          {
            label: "Số lượng tồn",
            data: sortedInventory.map((item) => Number(item.quantity || 0)),
            backgroundColor: [
              "#0c8d8a",
              "#1c7ca0",
              "#2e6ad1",
              "#ca5b2d",
              "#0a6f6d",
              "#1f8ab2",
              "#5d7ce2",
              "#d77744",
            ],
          },
        ],
      },
      options: { responsive: true, maintainAspectRatio: false },
    });

  }

  refreshBranchDashboard.addEventListener("click", () => {
    loadBranchDashboard().catch((error) => {
      branchStatsGrid.innerHTML = `<p class="subtitle">Lỗi: ${error.message}</p>`;
    });
  });

  await loadBranchDashboard();
}

async function setupBranchEmployeesPage() {
  const branch = ensureLocalBranch();
  if (!branch) {
    return;
  }
  setupBranchShell(branch);
  setupCrudSwitcher("employeesCrudSwitch");

  const employeesApiLine = document.getElementById("employeesApiLine");
  const employeesTableWrap = document.getElementById("employeesTableWrap");
  const employeeResult = document.getElementById("employeeResult");
  const employeeUpdateForm = document.getElementById("employeeUpdateForm");
  const employeeDeleteForm = document.getElementById("employeeDeleteForm");

  employeesApiLine.textContent = `GET /api/employees?branch=${branch}`;

  async function loadEmployees() {
    employeesTableWrap.innerHTML =
      '<p class="subtitle">Đang tải dữ liệu...</p>';
    const result = await fetchJSON(`/api/employees?branch=${branch}`);
    renderTable(employeesTableWrap, hideColumns(result.data, ["rowguid", "rowid"]), {
      tableId: "employees",
      emptyMessage: "Chưa có nhân viên nào trong chi nhánh.",
      onRowSelect: (row) => {
        employeeUpdateForm.elements.employeeId.value = row.MaNV || "";
        employeeDeleteForm.elements.employeeId.value = row.MaNV || "";
        employeeUpdateForm.elements.HoTen.value = row.HoTen || "";
        employeeUpdateForm.elements.ChucVu.value = row.ChucVu || "";
      },
    });
  }

  document.getElementById("reloadEmployees").addEventListener("click", () => {
    loadEmployees().catch((error) => {
      employeesTableWrap.innerHTML = `<p class="subtitle">Lỗi: ${error.message}</p>`;
    });
  });

  document
    .getElementById("employeeCreateForm")
    .addEventListener("submit", async (event) => {
      event.preventDefault();
      const form = new FormData(event.target);
      const payload = {
        branch,
        MaNV: form.get("MaNV"),
        HoTen: form.get("HoTen"),
        ChucVu: form.get("ChucVu"),
      };
      try {
        const result = await fetchJSON("/api/employees", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        employeeResult.textContent = JSON.stringify(result, null, 2);
        event.target.reset();
        await loadEmployees();
      } catch (error) {
        employeeResult.textContent = `Lỗi: ${error.message}`;
      }
    });

  employeeUpdateForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(event.target);
    const employeeId = String(form.get("employeeId") || "").trim();
    const payload = {};
    if (String(form.get("HoTen") || "").trim()) {
      payload.HoTen = form.get("HoTen");
    }
    if (String(form.get("ChucVu") || "").trim()) {
      payload.ChucVu = form.get("ChucVu");
    }
    try {
      const result = await fetchJSON(
        `/api/employees/${encodeURIComponent(employeeId)}?branch=${branch}`,
        {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        },
      );
      employeeResult.textContent = JSON.stringify(result, null, 2);
      await loadEmployees();
    } catch (error) {
      employeeResult.textContent = `Lỗi: ${error.message}`;
    }
  });

  employeeDeleteForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(event.target);
    const employeeId = String(form.get("employeeId") || "").trim();
    try {
      const result = await fetchJSON(
        `/api/employees/${encodeURIComponent(employeeId)}?branch=${branch}`,
        {
          method: "DELETE",
        },
      );
      employeeResult.textContent = JSON.stringify(result, null, 2);
      event.target.reset();
      await loadEmployees();
    } catch (error) {
      employeeResult.textContent = `Lỗi: ${error.message}`;
    }
  });

  await loadEmployees();
}

async function setupBranchInvoicesPage() {
  const branch = ensureLocalBranch();
  if (!branch) {
    return;
  }
  setupBranchShell(branch);

  const invoicesApiLine = document.getElementById("invoicesApiLine");
  const invoicesTableWrap = document.getElementById("invoicesTableWrap");
  const invoiceResult = document.getElementById("invoiceResult");
  const invoiceCreateForm = document.getElementById("invoiceCreateForm");
  
  const invoiceDetailsModal = document.getElementById("invoiceDetailsModal");
  const closeInvoiceDetailsModal = document.getElementById("closeInvoiceDetailsModal");
  const invoiceDetailsTitle = document.getElementById("invoiceDetailsTitle");
  const invoiceDetailsTableWrap = document.getElementById("invoiceDetailsTableWrap");

  if (closeInvoiceDetailsModal) {
    closeInvoiceDetailsModal.addEventListener("click", () => {
      invoiceDetailsModal.close();
    });
  }


  if (invoiceDetailsModal) {
    invoiceDetailsModal.addEventListener("click", (event) => {
      const rect = invoiceDetailsModal.getBoundingClientRect();
      const inDialog = event.clientX >= rect.left && event.clientX <= rect.right &&
                     event.clientY >= rect.top && event.clientY <= rect.bottom;
      if (!inDialog) {
        invoiceDetailsModal.close();
      }
    });
  }

  const branchInvoiceItems = attachInvoiceLineItems(
    invoiceCreateForm,
    () => branch,
    invoiceResult,
  );
  attachInvoiceProductAutoFill(invoiceCreateForm, () => branch, invoiceResult);

  invoicesApiLine.textContent = `GET /api/invoices?branch=${branch}`;

  async function loadInvoices() {
    invoicesTableWrap.innerHTML = '<p class="subtitle">Đang tải hóa đơn...</p>';
    
    const result = await fetchJSON(`/api/invoices?branch=${branch}`);
    renderTable(invoicesTableWrap, result.data, {
      tableId: "invoices",
      emptyMessage: "Chưa có hóa đơn nào trong chi nhánh.",
      onRowSelect: async (row) => {
        if (!row.MaHD) return;
        if (invoiceDetailsModal) {
          invoiceDetailsTitle.textContent = row.MaHD;
          invoiceDetailsModal.showModal();
          invoiceDetailsTableWrap.innerHTML = '<p class="subtitle">Đang tải chi tiết...</p>';
          try {
            const detailRes = await fetchJSON(`/api/invoices/${row.MaHD}/details?branch=${branch}`);
            renderTable(invoiceDetailsTableWrap, detailRes.data, {
              tableId: "invoice_details",
              emptyMessage: "Không tìm thấy chi tiết hóa đơn.",
            });
          } catch (err) {
            invoiceDetailsTableWrap.innerHTML = `<p class="subtitle">Lỗi tải chi tiết: ${err.message}</p>`;
          }
        }
      }
    });
  }

  document.getElementById("reloadInvoices").addEventListener("click", () => {
    loadInvoices().catch((error) => {
      invoicesTableWrap.innerHTML = `<p class="subtitle">Lỗi: ${error.message}</p>`;
    });
  });

  invoiceCreateForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      const form = new FormData(event.target);
      const items = branchInvoiceItems.collectItems();
      const totalAmount = items.reduce(
        (sum, item) => sum + Number(item.totalAmount || 0),
        0,
      );
      const payload = {
        branch,
        employeeId: String(form.get("employeeId") || "").trim(),
        items,
        totalAmount,
        note: form.get("note"),
      };

      try {
        if (!items.length) {
          throw new Error("Cần ít nhất 1 chi tiết hóa đơn");
        }
        const result = await fetchJSON("/api/invoices", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        invoiceResult.textContent = JSON.stringify(result, null, 2);
        event.target.reset();
        branchInvoiceItems.resetItems();
        await loadInvoices();
      } catch (error) {
        invoiceResult.textContent = `Lỗi: ${error.message}`;
      }
    });

  await loadInvoices();
}

async function setupBranchInventoryPage() {
  const branch = ensureLocalBranch();
  if (!branch) {
    return;
  }
  setupBranchShell(branch);
  setupCrudSwitcher("inventoryCrudSwitch");

  const inventoryApiLine = document.getElementById("inventoryApiLine");
  const inventoryTableWrap = document.getElementById("inventoryTableWrap");
  const inventoryResult = document.getElementById("inventoryResult");
  const inventoryUpdateForm = document.getElementById("inventoryUpdateForm");

  inventoryApiLine.textContent = `GET /api/inventory?branch=${branch}, PUT /api/inventory/:productCode?branch=${branch}`;

  async function loadInventory() {
    inventoryTableWrap.innerHTML =
      '<p class="subtitle">Đang tải tồn kho...</p>';
    const [result, products] = await Promise.all([
      fetchJSON(`/api/inventory?branch=${branch}`),
      fetchJSON("/api/products"),
    ]);
    const productMap = new Map(
      (Array.isArray(products) ? products : []).map((item) => [
        item.productCode,
        item,
      ]),
    );
    const inventoryRows = (Array.isArray(result) ? result : []).map((row) => {
      const product = productMap.get(row.productCode) || {};
      return {
        productCode: row.productCode,
        productName: product.productName || "",
        unitPrice: product.unitPrice ?? "",
        quantity: row.quantity,
        branch: row.branch,
      };
    });

    renderTable(inventoryTableWrap, inventoryRows, {
      tableId: "inventory",
      emptyMessage: "Chưa có sản phẩm tồn kho nào.",
      onRowSelect: (row) => {
        inventoryUpdateForm.elements.productCode.value = row.productCode || "";
        inventoryUpdateForm.elements.quantity.value = row.quantity || "";
      },
    });
  }

  document.getElementById("reloadInventory").addEventListener("click", () => {
    loadInventory().catch((error) => {
      inventoryTableWrap.innerHTML = `<p class="subtitle">Lỗi: ${error.message}</p>`;
    });
  });

  inventoryUpdateForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(event.target);
    const productCode = String(form.get("productCode") || "").trim();
    const payload = { quantity: Number(form.get("quantity") || 0) };
    try {
      const result = await fetchJSON(
        `/api/inventory/${encodeURIComponent(productCode)}?branch=${branch}`,
        {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        },
      );
      inventoryResult.textContent = JSON.stringify(result, null, 2);
      await loadInventory();
    } catch (error) {
      inventoryResult.textContent = `Lỗi: ${error.message}`;
    }
  });

  await loadInventory();
}

async function setupBranchProductsPage() {
  const branch = ensureLocalBranch();
  if (!branch) {
    return;
  }
  setupBranchShell(branch);

  const apiLine = document.getElementById("branchProductsApiLine");
  const tableWrap = document.getElementById("branchProductsTableWrap");
  const reloadBtn = document.getElementById("reloadBranchProducts");

  apiLine.textContent = "GET /api/products";

  async function loadBranchProducts() {
    tableWrap.innerHTML =
      '<p class="subtitle">Đang tải danh sách sản phẩm...</p>';
    const products = await fetchJSON("/api/products");
    const rows = (Array.isArray(products) ? products : []).map((item) => ({
      productCode: item.productCode,
      productName: item.productName,
      unitPrice: item.unitPrice,
    }));

    renderTable(tableWrap, rows, {
      tableId: "branch-products",
      emptyMessage: "Chưa có sản phẩm.",
    });
  }

  reloadBtn.addEventListener("click", () => {
    loadBranchProducts().catch((error) => {
      tableWrap.innerHTML = `<p class="subtitle">Lỗi: ${error.message}</p>`;
    });
  });

  await loadBranchProducts();
}

async function setupCentralOverviewPage() {
  if (!ensureCentralBranch()) {
    return;
  }
  setupCentralShell();

  const centralStatsGrid = document.getElementById("centralStatsGrid");
  const centralDailyTableWrap = document.getElementById("centralDailyTableWrap");
  const centralWeeklyTableWrap = document.getElementById("centralWeeklyTableWrap");
  const toggleCentralDailyRows = document.getElementById("toggleCentralDailyRows");
  const toggleCentralWeeklyRows = document.getElementById("toggleCentralWeeklyRows");
  const centralTopEmployeesWrap = document.getElementById("centralTopEmployeesWrap");
  const centralTopProductsWrap = document.getElementById("centralTopProductsWrap");
  const toggleCentralTopEmployeesRows = document.getElementById("toggleCentralTopEmployeesRows");
  const toggleCentralTopProductsRows = document.getElementById("toggleCentralTopProductsRows");
  const detailsModal = document.getElementById("centralAnalyticsDetailsModal");
  const openDetailsBtn = document.getElementById("openCentralAnalyticsDetails");
  const closeDetailsBtn = document.getElementById("closeCentralAnalyticsDetailsModal");
  const performanceModal = document.getElementById("centralPerformanceDetailsModal");
  const openPerformanceBtn = document.getElementById("openCentralPerformanceDetails");
  const closePerformanceBtn = document.getElementById("closeCentralPerformanceDetailsModal");
  const initialDetailRowLimit = 8;

  let dailyRowsForModal = [];
  let weeklyRowsForModal = [];
  let showAllDailyRows = false;
  let showAllWeeklyRows = false;
  let topEmployeesForModal = [];
  let topProductsForModal = [];
  let showAllTopEmployeesRows = false;
  let showAllTopProductsRows = false;

  if (openDetailsBtn && detailsModal) {
    openDetailsBtn.addEventListener("click", () => {
      detailsModal.showModal();
    });
  }

  if (closeDetailsBtn && detailsModal) {
    closeDetailsBtn.addEventListener("click", () => {
      detailsModal.close();
    });
  }

  if (detailsModal) {
    detailsModal.addEventListener("click", (event) => {
      const rect = detailsModal.getBoundingClientRect();
      const inDialog =
        event.clientX >= rect.left &&
        event.clientX <= rect.right &&
        event.clientY >= rect.top &&
        event.clientY <= rect.bottom;
      if (!inDialog) {
        detailsModal.close();
      }
    });
  }

  function aggregateDailyForChart(dailyRows) {
    const map = new Map();
    dailyRows.forEach((row) => {
      const key = String(row.date || "").slice(0, 10);
      const current = map.get(key) || 0;
      map.set(key, current + Number(row.totalRevenue || 0));
    });
    return Array.from(map.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .slice(-7);
  }

  function aggregateWeeklyForChart(weeklyRows) {
    const map = new Map();
    weeklyRows.forEach((row) => {
      const key = `${row.year}-W${String(row.week).padStart(2, "0")}`;
      const current = map.get(key) || 0;
      map.set(key, current + Number(row.totalRevenue || 0));
    });
    return Array.from(map.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .slice(-8);
  }

  function renderExpandableAnalyticsTable(
    target,
    rows,
    toggleBtn,
    expanded,
    options,
  ) {
    if (!target) {
      return;
    }

    const totalRows = rows.length;
    const visibleRows = expanded ? rows : rows.slice(0, initialDetailRowLimit);
    renderTable(target, visibleRows, options);

    if (!toggleBtn) {
      return;
    }

    const shouldShowToggle = totalRows > initialDetailRowLimit;
    toggleBtn.style.display = shouldShowToggle ? "inline-flex" : "none";
    if (shouldShowToggle) {
      toggleBtn.textContent = expanded ? "Thu gọn" : "Xem thêm";
    }
  }

  function renderModalTables() {
    renderExpandableAnalyticsTable(
      centralDailyTableWrap,
      dailyRowsForModal,
      toggleCentralDailyRows,
      showAllDailyRows,
      {
        tableId: "central-daily-analytics",
        emptyMessage: "Chưa có dữ liệu theo ngày.",
      },
    );

    renderExpandableAnalyticsTable(
      centralWeeklyTableWrap,
      weeklyRowsForModal,
      toggleCentralWeeklyRows,
      showAllWeeklyRows,
      {
        tableId: "central-weekly-analytics",
        emptyMessage: "Chưa có dữ liệu theo tuần.",
      },
    );
  }

  if (toggleCentralDailyRows) {
    toggleCentralDailyRows.addEventListener("click", () => {
      showAllDailyRows = !showAllDailyRows;
      renderModalTables();
    });
  }

  if (toggleCentralWeeklyRows) {
    toggleCentralWeeklyRows.addEventListener("click", () => {
      showAllWeeklyRows = !showAllWeeklyRows;
      renderModalTables();
    });
  }

  if (toggleCentralTopEmployeesRows) {
    toggleCentralTopEmployeesRows.addEventListener("click", () => {
      showAllTopEmployeesRows = !showAllTopEmployeesRows;
      renderPerformanceTables();
    });
  }

  if (toggleCentralTopProductsRows) {
    toggleCentralTopProductsRows.addEventListener("click", () => {
      showAllTopProductsRows = !showAllTopProductsRows;
      renderPerformanceTables();
    });
  }

  if (openPerformanceBtn && performanceModal) {
    openPerformanceBtn.addEventListener("click", () => {
      performanceModal.showModal();
    });
  }

  if (closePerformanceBtn && performanceModal) {
    closePerformanceBtn.addEventListener("click", () => {
      performanceModal.close();
    });
  }

  if (performanceModal) {
    performanceModal.addEventListener("click", (event) => {
      const rect = performanceModal.getBoundingClientRect();
      const inDialog =
        event.clientX >= rect.left &&
        event.clientX <= rect.right &&
        event.clientY >= rect.top &&
        event.clientY <= rect.bottom;
      if (!inDialog) {
        performanceModal.close();
      }
    });
  }

  function renderPerformanceTables() {
    renderExpandableAnalyticsTable(
      centralTopEmployeesWrap,
      topEmployeesForModal,
      toggleCentralTopEmployeesRows,
      showAllTopEmployeesRows,
      {
        tableId: "central-top-employees",
        emptyMessage: "Chưa có dữ liệu top nhân viên.",
      },
    );

    renderExpandableAnalyticsTable(
      centralTopProductsWrap,
      topProductsForModal,
      toggleCentralTopProductsRows,
      showAllTopProductsRows,
      {
        tableId: "central-top-products",
        emptyMessage: "Chưa có dữ liệu top sản phẩm.",
      },
    );
  }

  async function loadRevenue() {
    const [result, analytics] = await Promise.all([
      fetchJSON("/api/revenue/national?branch=CENTRAL"),
      fetchJSON("/api/analytics/overview?branch=CENTRAL"),
    ]);

    const byBranch = Array.isArray(result.byBranch) ? result.byBranch : [];
    const daily = Array.isArray(analytics.daily) ? analytics.daily : [];
    const weekly = Array.isArray(analytics.weekly) ? analytics.weekly : [];
    const topEmployees = Array.isArray(analytics.topEmployees) ? analytics.topEmployees : [];
    const topProducts = Array.isArray(analytics.topProducts) ? analytics.topProducts : [];
    const weekCompare = Array.isArray(analytics.weekCompare)
      ? analytics.weekCompare
      : [];

    const best = byBranch.reduce((acc, item) => {
      if (!acc || Number(item.revenue || 0) > Number(acc.revenue || 0)) {
        return item;
      }
      return acc;
    }, null);

    const cards = [
      {
        label: "Tổng doanh thu",
        value: `${formatNumber(result.nationalRevenue)} VND`,
      },
      { label: "Số chi nhánh", value: byBranch.length },
      {
        label: "Chi nhánh dẫn đầu",
        value: best
          ? `${best.branch} (${formatNumber(best.revenue)} VND)`
          : "N/A",
      },
      { label: "Chế độ", value: result.mode || "N/A" },
      {
        label: "Cập nhật",
        value: new Date(result.generatedAt).toLocaleString("vi-VN"),
      },
    ];

    centralStatsGrid.innerHTML = cards
      .map(
        (item) =>
          `<article class="stat-card"><p>${item.label}</p><h3>${item.value}</h3></article>`,
      )
      .join("");

    renderChart("centralRevenueBarChart", {
      type: "bar",
      data: {
        labels: byBranch.map((item) => item.branch),
        datasets: [
          {
            label: "Doanh thu VND",
            data: byBranch.map((item) => Number(item.revenue || 0)),
            backgroundColor: ["#0c8d8a", "#ca5b2d", "#2e6ad1"],
          },
        ],
      },
      options: { responsive: true, maintainAspectRatio: false },
    });

    renderChart("centralRevenueShareChart", {
      type: "doughnut",
      data: {
        labels: byBranch.map((item) => item.branch),
        datasets: [
          {
            data: byBranch.map((item) => Number(item.revenue || 0)),
            backgroundColor: ["#0c8d8a", "#ca5b2d", "#2e6ad1"],
            borderWidth: 0,
          },
        ],
      },
      options: { responsive: true, maintainAspectRatio: false },
    });

    const dailyForChart = aggregateDailyForChart(daily);
    renderChart("centralDailyRevenueChart", {
      type: "line",
      data: {
        labels: dailyForChart.map(([date]) => date.slice(5)),
        datasets: [
          {
            label: "Doanh thu toàn hệ thống",
            data: dailyForChart.map(([, revenue]) => revenue),
            borderColor: "#1c7ca0",
            backgroundColor: "rgba(28,124,160,0.2)",
            tension: 0.3,
            fill: true,
          },
        ],
      },
      options: { responsive: true, maintainAspectRatio: false },
    });

    const weeklyForChart = aggregateWeeklyForChart(weekly);
    renderChart("centralWeeklyRevenueChart", {
      type: "bar",
      data: {
        labels: weeklyForChart.map(([label]) => label),
        datasets: [
          {
            label: "Doanh thu theo tuần",
            data: weeklyForChart.map(([, revenue]) => revenue),
            backgroundColor: "#0c8d8a",
          },
        ],
      },
      options: { responsive: true, maintainAspectRatio: false },
    });

    renderChart("centralWeekCompareChart", {
      type: "bar",
      data: {
        labels: weekCompare.map((row) => row.branch),
        datasets: [
          {
            label: "Tuần này",
            data: weekCompare.map((row) => Number(row.thisWeekRevenue || 0)),
            backgroundColor: "#2e6ad1",
          },
          {
            label: "Tuần trước",
            data: weekCompare.map((row) => Number(row.lastWeekRevenue || 0)),
            backgroundColor: "#ca5b2d",
          },
        ],
      },
      options: { responsive: true, maintainAspectRatio: false },
    });

    dailyRowsForModal = daily.map((row) => ({
        ChiNhanh: row.branch,
        Ngay: String(row.date || "").slice(0, 10),
        TongSoDonHang: row.totalOrders,
        TongDoanhThu: row.totalRevenue,
      }));

    weeklyRowsForModal = weekly.map((row) => ({
        ChiNhanh: row.branch,
        Nam: row.year,
        TuanTrongNam: row.week,
        TongSoDonHang: row.totalOrders,
        TongDoanhThu: row.totalRevenue,
      }));

    topEmployeesForModal = topEmployees.map((row) => ({
      ChiNhanh: row.branch,
      MaNV: row.employeeId,
      HoTen: row.employeeName,
      TongDoanhThu: row.totalRevenue,
    }));

    topProductsForModal = topProducts.map((row) => ({
      ChiNhanh: row.branch,
      MaSP: row.productCode,
      TenHang: row.productName,
      TongSoLuongBan: row.totalSold,
    }));

    showAllDailyRows = false;
    showAllWeeklyRows = false;
    showAllTopEmployeesRows = false;
    showAllTopProductsRows = false;
    renderModalTables();
    renderPerformanceTables();

  }

  document.getElementById("loadRevenue").addEventListener("click", () => {
    loadRevenue().catch((error) => {
      centralStatsGrid.innerHTML = `<p class="subtitle">Lỗi: ${error.message}</p>`;
    });
  });

  await loadRevenue();
}

async function setupCentralEmployeesPage() {
  if (!ensureCentralBranch()) {
    return;
  }
  setupCentralShell();
  setupCrudSwitcher("centralEmployeesCrudSwitch");

  const tableWrap = document.getElementById("centralEmployeesTableWrap");
  const resultBox = document.getElementById("centralEmployeeResult");
  const readBranch = document.getElementById("centralEmployeesReadBranch");
  const createForm = document.getElementById("centralEmployeeCreateForm");
  const updateForm = document.getElementById("centralEmployeeUpdateForm");
  const deleteForm = document.getElementById("centralEmployeeDeleteForm");

  function syncFormsBranch(branch) {
    createForm.elements.branch.value = branch;
    updateForm.elements.branch.value = branch;
    deleteForm.elements.branch.value = branch;
  }

  async function loadEmployees() {
    const branch = readBranch.value;
    tableWrap.innerHTML = '<p class="subtitle">Đang tải nhân viên...</p>';
    const result = await fetchJSON(`/api/employees?branch=${branch}`);
    renderTable(tableWrap, hideColumns(result.data, ["rowguid", "rowid"]), {
      tableId: "central-local-employees",
      emptyMessage: "Chưa có nhân viên nào ở chi nhánh này.",
      onRowSelect: (row) => {
        syncFormsBranch(row.ChiNhanh || branch);
        updateForm.elements.employeeId.value = row.MaNV || "";
        deleteForm.elements.employeeId.value = row.MaNV || "";
        updateForm.elements.HoTen.value = row.HoTen || "";
        updateForm.elements.ChucVu.value = row.ChucVu || "";
      },
    });
  }

  readBranch.addEventListener("change", () => {
    syncFormsBranch(readBranch.value);
    loadEmployees().catch((error) => {
      tableWrap.innerHTML = `<p class="subtitle">Lỗi: ${error.message}</p>`;
    });
  });

  document
    .getElementById("loadCentralEmployees")
    .addEventListener("click", (event) => {
      event.preventDefault();
      loadEmployees().catch((error) => {
        tableWrap.innerHTML = `<p class="subtitle">Lỗi: ${error.message}</p>`;
      });
    });

  createForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(createForm);
    const payload = {
      branch: form.get("branch"),
      MaNV: form.get("MaNV"),
      HoTen: form.get("HoTen"),
      ChucVu: form.get("ChucVu"),
    };

    try {
      const result = await fetchJSON("/api/employees", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      resultBox.textContent = JSON.stringify(result, null, 2);
      createForm.reset();
      readBranch.value = payload.branch;
      syncFormsBranch(payload.branch);
      await loadEmployees();
    } catch (error) {
      resultBox.textContent = `Lỗi: ${error.message}`;
    }
  });

  updateForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(updateForm);
    const branch = String(form.get("branch"));
    const employeeId = String(form.get("employeeId") || "").trim();
    const payload = {};
    if (String(form.get("HoTen") || "").trim()) {
      payload.HoTen = form.get("HoTen");
    }
    if (String(form.get("ChucVu") || "").trim()) {
      payload.ChucVu = form.get("ChucVu");
    }

    try {
      const result = await fetchJSON(
        `/api/employees/${encodeURIComponent(employeeId)}?branch=${branch}`,
        {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        },
      );
      resultBox.textContent = JSON.stringify(result, null, 2);
      readBranch.value = branch;
      syncFormsBranch(branch);
      await loadEmployees();
    } catch (error) {
      resultBox.textContent = `Lỗi: ${error.message}`;
    }
  });

  deleteForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(deleteForm);
    const branch = String(form.get("branch"));
    const employeeId = String(form.get("employeeId") || "").trim();

    try {
      const result = await fetchJSON(
        `/api/employees/${encodeURIComponent(employeeId)}?branch=${branch}`,
        {
          method: "DELETE",
        },
      );
      resultBox.textContent = JSON.stringify(result, null, 2);
      deleteForm.reset();
      readBranch.value = branch;
      syncFormsBranch(branch);
      await loadEmployees();
    } catch (error) {
      resultBox.textContent = `Lỗi: ${error.message}`;
    }
  });

  syncFormsBranch(readBranch.value);
  await loadEmployees();
}

async function setupCentralInvoicesPage() {
  if (!ensureCentralBranch()) {
    return;
  }
  setupCentralShell();

  const tableWrap = document.getElementById("centralInvoicesTableWrap");
  const resultBox = document.getElementById("centralInvoiceResult");
  const readBranch = document.getElementById("centralInvoicesReadBranch");
  const createForm = document.getElementById("centralInvoiceCreateForm");

  const centralInvoiceDetailsModal = document.getElementById("centralInvoiceDetailsModal");
  const closeCentralInvoiceDetailsModal = document.getElementById("closeCentralInvoiceDetailsModal");
  const centralInvoiceDetailsTitle = document.getElementById("centralInvoiceDetailsTitle");
  const centralInvoiceDetailsTableWrap = document.getElementById("centralInvoiceDetailsTableWrap");

  if (closeCentralInvoiceDetailsModal) {
    closeCentralInvoiceDetailsModal.addEventListener("click", () => {
      centralInvoiceDetailsModal.close();
    });
  }


  if (centralInvoiceDetailsModal) {
    centralInvoiceDetailsModal.addEventListener("click", (event) => {
      const rect = centralInvoiceDetailsModal.getBoundingClientRect();
      const inDialog = event.clientX >= rect.left && event.clientX <= rect.right &&
                     event.clientY >= rect.top && event.clientY <= rect.bottom;
      if (!inDialog) {
        centralInvoiceDetailsModal.close();
      }
    });
  }

  const centralInvoiceItems = attachInvoiceLineItems(
    createForm,
    () => String(createForm.elements.branch.value || ""),
    resultBox,
  );
  attachInvoiceProductAutoFill(
    createForm,
    () => String(createForm.elements.branch.value || ""),
    resultBox,
  );

  function syncFormsBranch(branch) {
    createForm.elements.branch.value = branch;
  }

  async function loadInvoices() {
    const branch = readBranch.value;
    tableWrap.innerHTML = '<p class="subtitle">Đang tải hóa đơn...</p>';

    const result = await fetchJSON(`/api/invoices?branch=${branch}`);
    renderTable(tableWrap, result.data, {
      tableId: "central-local-invoices",
      emptyMessage: "Chưa có hóa đơn nào ở chi nhánh này.",
      onRowSelect: async (row) => {
        if (!row.MaHD) return;
        if (centralInvoiceDetailsModal) {
          centralInvoiceDetailsTitle.textContent = row.MaHD;
          centralInvoiceDetailsModal.showModal();
          centralInvoiceDetailsTableWrap.innerHTML = '<p class="subtitle">Đang tải chi tiết...</p>';
          try {
            const detailRes = await fetchJSON(`/api/invoices/${row.MaHD}/details?branch=${branch}`);
            renderTable(centralInvoiceDetailsTableWrap, detailRes.data, {
              tableId: "central_invoice_details",
              emptyMessage: "Không tìm thấy chi tiết hóa đơn.",
            });
          } catch (err) {
            centralInvoiceDetailsTableWrap.innerHTML = `<p class="subtitle">Lỗi tải chi tiết: ${err.message}</p>`;
          }
        }
      }
    });
  }

  readBranch.addEventListener("change", () => {
    syncFormsBranch(readBranch.value);
    loadInvoices().catch((error) => {
      tableWrap.innerHTML = `<p class="subtitle">Lỗi: ${error.message}</p>`;
    });
  });

  document
    .getElementById("loadCentralInvoices")
    .addEventListener("click", (event) => {
      event.preventDefault();
      loadInvoices().catch((error) => {
        tableWrap.innerHTML = `<p class="subtitle">Lỗi: ${error.message}</p>`;
      });
    });

  createForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(createForm);
    const items = centralInvoiceItems.collectItems();
    const totalAmount = items.reduce(
      (sum, item) => sum + Number(item.totalAmount || 0),
      0,
    );
    const payload = {
      branch: form.get("branch"),
      employeeId: String(form.get("employeeId") || "").trim(),
      items,
      totalAmount,
      note: form.get("note"),
    };
    try {
      if (!items.length) {
        throw new Error("Cần ít nhất 1 chi tiết hóa đơn");
      }
      const result = await fetchJSON("/api/invoices", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      resultBox.textContent = JSON.stringify(result, null, 2);
      createForm.reset();
      centralInvoiceItems.resetItems();
      readBranch.value = payload.branch;
      syncFormsBranch(payload.branch);
      await loadInvoices();
    } catch (error) {
      resultBox.textContent = `Lỗi: ${error.message}`;
    }
  });

  syncFormsBranch(readBranch.value);
  await loadInvoices();
}

async function setupCentralInventoryPage() {
  if (!ensureCentralBranch()) {
    return;
  }
  setupCentralShell();
  setupCrudSwitcher("centralInventoryCrudSwitch");

  const tableWrap = document.getElementById("centralInventoryTableWrap");
  const resultBox = document.getElementById("centralInventoryResult");
  const readBranch = document.getElementById("centralInventoryReadBranch");
  const updateForm = document.getElementById("centralInventoryUpdateForm");

  function syncFormsBranch(branch) {
    updateForm.elements.branch.value = branch;
  }

  async function loadInventory() {
    const branch = readBranch.value;
    tableWrap.innerHTML = '<p class="subtitle">Đang tải tồn kho...</p>';
    const [result, products] = await Promise.all([
      fetchJSON(`/api/inventory?branch=${branch}`),
      fetchJSON("/api/products"),
    ]);
    const productMap = new Map(
      (Array.isArray(products) ? products : []).map((item) => [
        item.productCode,
        item,
      ]),
    );
    const inventoryRows = (Array.isArray(result) ? result : []).map((row) => {
      const product = productMap.get(row.productCode) || {};
      return {
        productCode: row.productCode,
        productName: product.productName || "",
        unitPrice: product.unitPrice ?? "",
        quantity: row.quantity,
        branch: row.branch,
      };
    });

    renderTable(tableWrap, inventoryRows, {
      tableId: "central-local-inventory",
      emptyMessage: "Chưa có sản phẩm tồn kho ở chi nhánh này.",
      onRowSelect: (row) => {
        syncFormsBranch(row.branch || branch);
        updateForm.elements.productCode.value = row.productCode || "";
        updateForm.elements.quantity.value = row.quantity || "";
      },
    });
  }

  readBranch.addEventListener("change", () => {
    syncFormsBranch(readBranch.value);
    loadInventory().catch((error) => {
      tableWrap.innerHTML = `<p class="subtitle">Lỗi: ${error.message}</p>`;
    });
  });

  document
    .getElementById("loadCentralInventory")
    .addEventListener("click", (event) => {
      event.preventDefault();
      loadInventory().catch((error) => {
        tableWrap.innerHTML = `<p class="subtitle">Lỗi: ${error.message}</p>`;
      });
    });

  updateForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(updateForm);
    const branch = String(form.get("branch"));
    const productCode = String(form.get("productCode") || "").trim();
    const payload = { quantity: Number(form.get("quantity") || 0) };
    try {
      const result = await fetchJSON(
        `/api/inventory/${encodeURIComponent(productCode)}?branch=${branch}`,
        {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        },
      );
      resultBox.textContent = JSON.stringify(result, null, 2);
      readBranch.value = branch;
      syncFormsBranch(branch);
      await loadInventory();
    } catch (error) {
      resultBox.textContent = `Lỗi: ${error.message}`;
    }
  });

  syncFormsBranch(readBranch.value);
  await loadInventory();
}

async function setupCentralProductsPage() {
  if (!ensureCentralBranch()) {
    return;
  }
  setupCentralShell();
  setupCrudSwitcher("centralProductCrudSwitch");

  const tableWrap = document.getElementById("centralProductTableWrap");
  const resultBox = document.getElementById("centralProductResult");
  const createForm = document.getElementById("centralProductCreateForm");
  const updateForm = document.getElementById("centralProductUpdateForm");
  const deleteForm = document.getElementById("centralProductDeleteForm");

  async function loadProducts() {
    tableWrap.innerHTML = '<p class="subtitle">Đang tải danh sách sản phẩm...</p>';
    const result = await fetchJSON("/api/products");
    renderTable(tableWrap, result, {
      tableId: "central-products",
      emptyMessage: "Chưa có sản phẩm.",
      onRowSelect: (row) => {
        updateForm.elements.productCode.value = row.productCode || "";
        updateForm.elements.productName.value = row.productName || "";
        updateForm.elements.unitPrice.value = row.unitPrice || "";
        deleteForm.elements.productCode.value = row.productCode || "";
      },
    });
  }

  document.getElementById("loadCentralProducts").addEventListener("click", (event) => {
    event.preventDefault();
    loadProducts().catch((error) => {
      tableWrap.innerHTML = `<p class="subtitle">Lỗi: ${error.message}</p>`;
    });
  });

  createForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(createForm);
    const payload = {
      branch: "CENTRAL",
      productCode: String(form.get("productCode") || "").trim(),
      productName: String(form.get("productName") || "").trim(),
      unitPrice: Number(form.get("unitPrice") || 0),
    };
    try {
      const result = await fetchJSON("/api/products", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      resultBox.textContent = JSON.stringify(result, null, 2);
      createForm.reset();
      await loadProducts();
    } catch (error) {
      resultBox.textContent = `Lỗi: ${error.message}`;
    }
  });

  updateForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(updateForm);
    const productCode = String(form.get("productCode") || "").trim();
    const payload = {
      branch: "CENTRAL",
      productName: form.get("productName"),
      unitPrice: form.get("unitPrice") === "" ? undefined : Number(form.get("unitPrice")),
    };
    try {
      const result = await fetchJSON(`/api/products/${encodeURIComponent(productCode)}?branch=CENTRAL`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      resultBox.textContent = JSON.stringify(result, null, 2);
      await loadProducts();
    } catch (error) {
      resultBox.textContent = `Lỗi: ${error.message}`;
    }
  });

  deleteForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(deleteForm);
    const productCode = String(form.get("productCode") || "").trim();
    try {
      const result = await fetchJSON(`/api/products/${encodeURIComponent(productCode)}?branch=CENTRAL`, {
        method: "DELETE",
      });
      resultBox.textContent = JSON.stringify(result, null, 2);
      deleteForm.reset();
      await loadProducts();
    } catch (error) {
      resultBox.textContent = `Lỗi: ${error.message}`;
    }
  });

  await loadProducts();
}

async function setupCentralTransferPage() {
  if (!ensureCentralBranch()) {
    return;
  }
  setupCentralShell();

  const transferResult = document.getElementById("transferResult");
  document
    .getElementById("transferForm")
    .addEventListener("submit", async (event) => {
      event.preventDefault();
      const form = new FormData(event.target);
      const payload = {
        branch: "CENTRAL",
        fromBranch: form.get("fromBranch"),
        toBranch: form.get("toBranch"),
        productCode: form.get("productCode"),
        quantity: Number(form.get("quantity")),
      };

      try {
        const result = await fetchJSON("/api/transfer-stock", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });

        const fromInv = await fetchJSON(
          `/api/inventory?branch=${payload.fromBranch}&productCode=${encodeURIComponent(payload.productCode)}`,
        );
        const toInv = await fetchJSON(
          `/api/inventory?branch=${payload.toBranch}&productCode=${encodeURIComponent(payload.productCode)}`,
        );

        transferResult.textContent = JSON.stringify(
          {
            transfer: result,
            inventoryAfterCheck: {
              [payload.fromBranch]: fromInv,
              [payload.toBranch]: toInv,
            },
          },
          null,
          2,
        );
      } catch (error) {
        transferResult.textContent = `Lỗi: ${error.message}`;
      }
    });
}

(async function bootstrap() {
  try {
    if (page === "landing") {
      setupLandingPage();
      return;
    }

    if (page === "branch") {
      window.location.href = "/branch-dashboard.html";
      return;
    }

    if (page === "central") {
      window.location.href = "/central-overview.html";
      return;
    }

    if (page === "branch-dashboard") {
      await setupBranchDashboardPage();
      return;
    }

    if (page === "branch-employees") {
      await setupBranchEmployeesPage();
      return;
    }

    if (page === "branch-invoices") {
      await setupBranchInvoicesPage();
      return;
    }

    if (page === "branch-inventory") {
      await setupBranchInventoryPage();
      return;
    }

    if (page === "branch-products") {
      await setupBranchProductsPage();
      return;
    }

    if (page === "central-overview") {
      await setupCentralOverviewPage();
      return;
    }

    if (page === "central-employees") {
      await setupCentralEmployeesPage();
      return;
    }

    if (page === "central-invoices") {
      await setupCentralInvoicesPage();
      return;
    }

    if (page === "central-inventory") {
      await setupCentralInventoryPage();
      return;
    }

    if (page === "central-products") {
      await setupCentralProductsPage();
      return;
    }

    if (page === "central-transfer") {
      await setupCentralTransferPage();
      return;
    }
  } catch (error) {
    console.error(error);
    alert(`Lỗi khởi tạo giao diện: ${error.message}`);
  }
})();



