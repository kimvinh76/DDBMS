const BRANCH_LABELS = {
  HUE: "Chi nhánh Hue (Port 1401)",
  SAIGON: "Chi nhánh Sai Gon (Port 1402)",
  HANOI: "Chi nhánh Ha Noi (Port 1403)",
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

function renderTable(wrapper, rows, options = {}) {
  if (!rows || rows.length === 0) {
    wrapper.innerHTML = `<p class="subtitle">${options.emptyMessage || "Không co du lieu."}</p>`;
    return;
  }

  const tableId =
    options.tableId || `tb_${Math.random().toString(36).slice(2)}`;
  const headers = Object.keys(rows[0]);
  const thead = `<thead><tr>${headers.map((h) => `<th>${h}</th>`).join("")}</tr></thead>`;
  const tbody = `<tbody>${rows
    .map(
      (row, index) =>
        `<tr data-table-id="${tableId}" data-row-index="${index}">${headers.map((h) => `<td>${row[h] ?? ""}</td>`).join("")}</tr>`,
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

function renderStatsGrid(wrapper, stats) {
  const cards = [
    { label: "Nhân viên", value: stats.employeeCount },
    { label: "Hóa đơn", value: stats.invoiceCount },
    { label: "Doanh thu", value: `${formatNumber(stats.revenue)} VND` },
    { label: "Tong ton kho", value: formatNumber(stats.totalStockUnits) },
    { label: "San pham sap het", value: formatNumber(stats.lowStockProducts) },
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
  const branchDashboardDetail = document.getElementById(
    "branchDashboardDetail",
  );
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
            label: "So luong ton",
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

    const maxInventory = sortedInventory[0] || null;
    const lowStock = inventory
      .filter((item) => Number(item.quantity || 0) < 50)
      .map((item) => item.productCode);

    branchDashboardDetail.textContent = JSON.stringify(
      {
        branch,
        generatedAt: result.generatedAt,
        strongestProductByStock: maxInventory,
        lowStockProducts: lowStock,
        sevenDayRevenue: daily,
      },
      null,
      2,
    );
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
      '<p class="subtitle">Đang tải du lieu...</p>';
    const result = await fetchJSON(`/api/employees?branch=${branch}`);
    renderTable(employeesTableWrap, result.data, {
      tableId: "employees",
      emptyMessage: "Chưa co nhan vien nao trong chi nhánh.",
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
  setupCrudSwitcher("invoicesCrudSwitch");

  const invoicesApiLine = document.getElementById("invoicesApiLine");
  const invoicesTableWrap = document.getElementById("invoicesTableWrap");
  const invoiceResult = document.getElementById("invoiceResult");
  const invoiceUpdateForm = document.getElementById("invoiceUpdateForm");
  const invoiceDeleteForm = document.getElementById("invoiceDeleteForm");

  invoicesApiLine.textContent = `GET /api/invoices?branch=${branch}`;

  async function loadInvoices() {
    invoicesTableWrap.innerHTML = '<p class="subtitle">Đang tải hoa don...</p>';
    const result = await fetchJSON(`/api/invoices?branch=${branch}`);
    renderTable(invoicesTableWrap, result.data, {
      tableId: "invoices",
      emptyMessage: "Chưa co hoa don nao trong chi nhánh.",
      onRowSelect: (row) => {
        invoiceUpdateForm.elements.invoiceId.value = row.MaHD || "";
        invoiceDeleteForm.elements.invoiceId.value = row.MaHD || "";
        invoiceUpdateForm.elements.productCode.value = row.MaSP || "";
        invoiceUpdateForm.elements.quantity.value = row.SoLuong || "";
        invoiceUpdateForm.elements.totalAmount.value = row.TongTien || "";
        invoiceUpdateForm.elements.note.value = row.GhiChu || "";
      },
    });
  }

  document.getElementById("reloadInvoices").addEventListener("click", () => {
    loadInvoices().catch((error) => {
      invoicesTableWrap.innerHTML = `<p class="subtitle">Lỗi: ${error.message}</p>`;
    });
  });

  document
    .getElementById("invoiceCreateForm")
    .addEventListener("submit", async (event) => {
      event.preventDefault();
      const form = new FormData(event.target);
      const payload = {
        branch,
        productCode: form.get("productCode"),
        quantity: Number(form.get("quantity")),
        totalAmount: Number(form.get("totalAmount")),
        note: form.get("note"),
      };

      try {
        const result = await fetchJSON("/api/invoices", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        invoiceResult.textContent = JSON.stringify(result, null, 2);
        event.target.reset();
        await loadInvoices();
      } catch (error) {
        invoiceResult.textContent = `Lỗi: ${error.message}`;
      }
    });

  invoiceUpdateForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(event.target);
    const invoiceId = String(form.get("invoiceId") || "").trim();
    const payload = {
      productCode: String(form.get("productCode") || "").trim(),
      quantity: Number(form.get("quantity") || 0),
      totalAmount: Number(form.get("totalAmount") || 0),
      note: String(form.get("note") || ""),
    };
    try {
      const result = await fetchJSON(
        `/api/invoices/${encodeURIComponent(invoiceId)}?branch=${branch}`,
        {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        },
      );
      invoiceResult.textContent = JSON.stringify(result, null, 2);
      await loadInvoices();
    } catch (error) {
      invoiceResult.textContent = `Lỗi: ${error.message}`;
    }
  });

  invoiceDeleteForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(event.target);
    const invoiceId = String(form.get("invoiceId") || "").trim();
    try {
      const result = await fetchJSON(
        `/api/invoices/${encodeURIComponent(invoiceId)}?branch=${branch}`,
        {
          method: "DELETE",
        },
      );
      invoiceResult.textContent = JSON.stringify(result, null, 2);
      event.target.reset();
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
  const inventoryDeleteForm = document.getElementById("inventoryDeleteForm");

  inventoryApiLine.textContent = `GET /api/inventory?branch=${branch}`;

  async function loadInventory() {
    inventoryTableWrap.innerHTML =
      '<p class="subtitle">Đang tải ton kho...</p>';
    const result = await fetchJSON(`/api/inventory?branch=${branch}`);
    renderTable(inventoryTableWrap, result, {
      tableId: "inventory",
      emptyMessage: "Chưa co san pham ton kho nao.",
      onRowSelect: (row) => {
        inventoryUpdateForm.elements.productCode.value = row.productCode || "";
        inventoryDeleteForm.elements.productCode.value = row.productCode || "";
        inventoryUpdateForm.elements.quantity.value = row.quantity || "";
      },
    });
  }

  document.getElementById("reloadInventory").addEventListener("click", () => {
    loadInventory().catch((error) => {
      inventoryTableWrap.innerHTML = `<p class="subtitle">Lỗi: ${error.message}</p>`;
    });
  });

  document
    .getElementById("inventoryCreateForm")
    .addEventListener("submit", async (event) => {
      event.preventDefault();
      const form = new FormData(event.target);
      const payload = {
        branch,
        productCode: form.get("productCode"),
        quantity: Number(form.get("quantity") || 0),
      };
      try {
        const result = await fetchJSON("/api/inventory", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        inventoryResult.textContent = JSON.stringify(result, null, 2);
        event.target.reset();
        await loadInventory();
      } catch (error) {
        inventoryResult.textContent = `Lỗi: ${error.message}`;
      }
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

  inventoryDeleteForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(event.target);
    const productCode = String(form.get("productCode") || "").trim();
    try {
      const result = await fetchJSON(
        `/api/inventory/${encodeURIComponent(productCode)}?branch=${branch}`,
        {
          method: "DELETE",
        },
      );
      inventoryResult.textContent = JSON.stringify(result, null, 2);
      event.target.reset();
      await loadInventory();
    } catch (error) {
      inventoryResult.textContent = `Lỗi: ${error.message}`;
    }
  });

  await loadInventory();
}

async function setupCentralOverviewPage() {
  if (!ensureCentralBranch()) {
    return;
  }
  setupCentralShell();

  const centralStatsGrid = document.getElementById("centralStatsGrid");
  const centralInsightResult = document.getElementById("centralInsightResult");
  const revenueResult = document.getElementById("revenueResult");

  async function loadRevenue() {
    const result = await fetchJSON("/api/revenue/national?branch=CENTRAL");
    const byBranch = Array.isArray(result.byBranch) ? result.byBranch : [];
    const best = byBranch.reduce((acc, item) => {
      if (!acc || Number(item.revenue || 0) > Number(acc.revenue || 0)) {
        return item;
      }
      return acc;
    }, null);

    const cards = [
      {
        label: "Tong doanh thu",
        value: `${formatNumber(result.nationalRevenue)} VND`,
      },
      { label: "So chi nhánh", value: byBranch.length },
      {
        label: "Chi nhánh dan dau",
        value: best
          ? `${best.branch} (${formatNumber(best.revenue)} VND)`
          : "N/A",
      },
      { label: "Che do", value: result.mode || "N/A" },
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

    centralInsightResult.textContent = JSON.stringify(
      {
        bestBranch: best || null,
        branchRanking: [...byBranch].sort(
          (a, b) => Number(b.revenue || 0) - Number(a.revenue || 0),
        ),
        nationalRevenue: result.nationalRevenue,
      },
      null,
      2,
    );

    revenueResult.textContent = JSON.stringify(result, null, 2);
  }

  document.getElementById("loadRevenue").addEventListener("click", () => {
    loadRevenue().catch((error) => {
      revenueResult.textContent = `Lỗi: ${error.message}`;
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
    tableWrap.innerHTML = '<p class="subtitle">Đang tải nhan vien...</p>';
    const result = await fetchJSON(`/api/employees?branch=${branch}`);
    renderTable(tableWrap, result.data, {
      tableId: "central-local-employees",
      emptyMessage: "Chưa co nhan vien nao o chi nhánh nay.",
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
  setupCrudSwitcher("centralInvoicesCrudSwitch");

  const tableWrap = document.getElementById("centralInvoicesTableWrap");
  const resultBox = document.getElementById("centralInvoiceResult");
  const readBranch = document.getElementById("centralInvoicesReadBranch");
  const createForm = document.getElementById("centralInvoiceCreateForm");
  const updateForm = document.getElementById("centralInvoiceUpdateForm");
  const deleteForm = document.getElementById("centralInvoiceDeleteForm");

  function syncFormsBranch(branch) {
    createForm.elements.branch.value = branch;
    updateForm.elements.branch.value = branch;
    deleteForm.elements.branch.value = branch;
  }

  async function loadInvoices() {
    const branch = readBranch.value;
    tableWrap.innerHTML = '<p class="subtitle">Đang tải hoa don...</p>';
    const result = await fetchJSON(`/api/invoices?branch=${branch}`);
    renderTable(tableWrap, result.data, {
      tableId: "central-local-invoices",
      emptyMessage: "Chưa co hoa don nao o chi nhánh nay.",
      onRowSelect: (row) => {
        syncFormsBranch(row.ChiNhanh || branch);
        updateForm.elements.invoiceId.value = row.MaHD || "";
        deleteForm.elements.invoiceId.value = row.MaHD || "";
        updateForm.elements.productCode.value = row.MaSP || "";
        updateForm.elements.quantity.value = row.SoLuong || "";
        updateForm.elements.totalAmount.value = row.TongTien || "";
        updateForm.elements.note.value = row.GhiChu || "";
      },
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
    const payload = {
      branch: form.get("branch"),
      productCode: form.get("productCode"),
      quantity: Number(form.get("quantity") || 0),
      totalAmount: Number(form.get("totalAmount") || 0),
      note: form.get("note"),
    };
    try {
      const result = await fetchJSON("/api/invoices", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      resultBox.textContent = JSON.stringify(result, null, 2);
      createForm.reset();
      readBranch.value = payload.branch;
      syncFormsBranch(payload.branch);
      await loadInvoices();
    } catch (error) {
      resultBox.textContent = `Lỗi: ${error.message}`;
    }
  });

  updateForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(updateForm);
    const branch = String(form.get("branch"));
    const invoiceId = String(form.get("invoiceId") || "").trim();
    const payload = {
      productCode: String(form.get("productCode") || "").trim(),
      quantity: Number(form.get("quantity") || 0),
      totalAmount: Number(form.get("totalAmount") || 0),
      note: String(form.get("note") || ""),
    };
    try {
      const result = await fetchJSON(
        `/api/invoices/${encodeURIComponent(invoiceId)}?branch=${branch}`,
        {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        },
      );
      resultBox.textContent = JSON.stringify(result, null, 2);
      readBranch.value = branch;
      syncFormsBranch(branch);
      await loadInvoices();
    } catch (error) {
      resultBox.textContent = `Lỗi: ${error.message}`;
    }
  });

  deleteForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(deleteForm);
    const branch = String(form.get("branch"));
    const invoiceId = String(form.get("invoiceId") || "").trim();
    try {
      const result = await fetchJSON(
        `/api/invoices/${encodeURIComponent(invoiceId)}?branch=${branch}`,
        {
          method: "DELETE",
        },
      );
      resultBox.textContent = JSON.stringify(result, null, 2);
      deleteForm.reset();
      readBranch.value = branch;
      syncFormsBranch(branch);
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
  const createForm = document.getElementById("centralInventoryCreateForm");
  const updateForm = document.getElementById("centralInventoryUpdateForm");
  const deleteForm = document.getElementById("centralInventoryDeleteForm");

  function syncFormsBranch(branch) {
    createForm.elements.branch.value = branch;
    updateForm.elements.branch.value = branch;
    deleteForm.elements.branch.value = branch;
  }

  async function loadInventory() {
    const branch = readBranch.value;
    tableWrap.innerHTML = '<p class="subtitle">Đang tải ton kho...</p>';
    const result = await fetchJSON(`/api/inventory?branch=${branch}`);
    renderTable(tableWrap, result, {
      tableId: "central-local-inventory",
      emptyMessage: "Chưa co san pham ton kho o chi nhánh nay.",
      onRowSelect: (row) => {
        syncFormsBranch(row.branch || branch);
        updateForm.elements.productCode.value = row.productCode || "";
        updateForm.elements.quantity.value = row.quantity || "";
        deleteForm.elements.productCode.value = row.productCode || "";
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

  createForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(createForm);
    const payload = {
      branch: form.get("branch"),
      productCode: form.get("productCode"),
      quantity: Number(form.get("quantity") || 0),
    };
    try {
      const result = await fetchJSON("/api/inventory", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      resultBox.textContent = JSON.stringify(result, null, 2);
      createForm.reset();
      readBranch.value = payload.branch;
      syncFormsBranch(payload.branch);
      await loadInventory();
    } catch (error) {
      resultBox.textContent = `Lỗi: ${error.message}`;
    }
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

  deleteForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(deleteForm);
    const branch = String(form.get("branch"));
    const productCode = String(form.get("productCode") || "").trim();
    try {
      const result = await fetchJSON(
        `/api/inventory/${encodeURIComponent(productCode)}?branch=${branch}`,
        {
          method: "DELETE",
        },
      );
      resultBox.textContent = JSON.stringify(result, null, 2);
      deleteForm.reset();
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

    if (page === "central-transfer") {
      await setupCentralTransferPage();
      return;
    }
  } catch (error) {
    console.error(error);
    alert(`Lỗi khoi tao giao dien: ${error.message}`);
  }
})();



