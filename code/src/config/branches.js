/**
 * CẤU HÌNH CÁC CHI NHÁNH VÀ TRUNG TÂM
 * 
 * Mỗi chi nhánh có:
 * - code: Mã chi nhánh (HUE, SAIGON, HANOI, CENTRAL)
 * - label: Tên hiển thị trên giao diện
 * - envPrefix: Tiền tố để tìm biến môi trường trong .env
 *   VD: envPrefix="HUE" → tìm HUE_DB_HOST, HUE_DB_PORT, HUE_DB_NAME, ...
 * - defaultPort: Cổng mặc định nếu không có trong .env
 * - defaultDatabase: Tên database mặc định nếu không có trong .env
 */
const BRANCHES = Object.freeze({
  // ========================================
  // CHI NHÁNH HUẾ
  // ========================================
  HUE: {
    code: "HUE",
    label: "Chi nhanh Hue",
    envPrefix: "HUE",  // → Tìm HUE_DB_HOST, HUE_DB_PORT, HUE_DB_USER, HUE_DB_PASSWORD, HUE_DB_NAME trong .env
    defaultPort: 1401,  // Cổng mặc định nếu không set HUE_DB_PORT
    defaultDatabase: "Store_H",  // Database mặc định nếu không set HUE_DB_NAME
  },
  
  // ========================================
  // CHI NHÁNH SÀI GÒN
  // ========================================
  SAIGON: {
    code: "SAIGON",
    label: "Chi nhanh Sai Gon",
    envPrefix: "SAIGON",  // → Tìm SAIGON_DB_HOST, SAIGON_DB_PORT, ...
    defaultPort: 1402,
    defaultDatabase: "Store_SG",
  },
  
  // ========================================
  // CHI NHÁNH HÀ NỘI
  // ========================================
  HANOI: {
    code: "HANOI",
    label: "Chi nhanh Ha Noi",
    envPrefix: "HANOI",  // → Tìm HANOI_DB_HOST, HANOI_DB_PORT, ...
    defaultPort: 1403,
    defaultDatabase: "Store_HN",
  },
  
  // ========================================
  // TRUNG TÂM (CENTRAL)
  // ========================================
  CENTRAL: {
    code: "CENTRAL",
    label: "Tong cong ty",
    envPrefix: "CENTRAL",  // → Tìm CENTRAL_DB_HOST, CENTRAL_DB_PORT, CENTRAL_DB_NAME, ...
    defaultPort: 1404,
    defaultDatabase: "CentralDB",
  },
});

/**
 * Chuẩn hóa mã chi nhánh từ input người dùng
 * VD: "hue" hoặc "HUE" hoặc " hue " → trả về "HUE"
 * VD: "invalid" → trả về null
 * 
 * @param {string} value - Mã chi nhánh từ URL, form, ...
 * @returns {string|null} Mã chi nhánh chuẩn hóa hoặc null nếu không hợp lệ
 */
function normalizeBranch(value) {
  if (!value) {
    return null;
  }
  const normalized = String(value).trim().toUpperCase();
  return BRANCHES[normalized] ? normalized : null;
}

/**
 * Kiểm tra xem mã chi nhánh có phải CENTRAL không
 * @param {string} branchCode - Mã chi nhánh
 * @returns {boolean} true nếu là CENTRAL, false nếu là chi nhánh địa phương
 */
function isCentralBranch(branchCode) {
  return normalizeBranch(branchCode) === "CENTRAL";
}

/**
 * Lấy toàn bộ cấu hình của một chi nhánh
 * @param {string} branchCode - Mã chi nhánh
 * @returns {object|null} Object chứa code, label, envPrefix, defaultPort, defaultDatabase
 */
function getBranchConfig(branchCode) {
  const normalized = normalizeBranch(branchCode);
  return normalized ? BRANCHES[normalized] : null;
}

/**
 * Danh sách tất cả các chi nhánh được hỗ trợ
 * @returns {array} ["HUE", "SAIGON", "HANOI", "CENTRAL"]
 */
function supportedBranches() {
  return Object.keys(BRANCHES);
}

module.exports = {
  BRANCHES,
  normalizeBranch,
  isCentralBranch,
  getBranchConfig,
  supportedBranches,
};
