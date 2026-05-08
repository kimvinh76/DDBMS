const sql = require("mssql");
const { getBranchConfig } = require("../config/branches");

// Lưu trữ các connection pool đã tạo sẵn (tránh tạo lại nhiều lần)
const poolCache = new Map();

// Kiểm tra xem có đang chạy ở chế độ MOCK (dữ liệu giả) không
function isMockMode() {
  return process.env.MOCK_MODE !== "false";
}

/**
 * Chuyển thành cấu hình kết nối SQL Server cho một chi nhánh
 * @param {string} branchCode - Mã chi nhánh (HUE, SAIGON, HANOI, CENTRAL)
 * @returns {object} Cấu hình kết nối chứa: server, port, user, password, database
 */
function toConnectionConfig(branchCode) {
  const branch = getBranchConfig(branchCode);
  if (!branch) {
    throw new Error(`Unsupported branch: ${branchCode}`);
  }

  // prefix = "HUE", "SG", "HN", "CENTRAL" → lấy biến môi trường tương ứng từ .env
  const prefix = branch.envPrefix;
  return {
    // Đọc từ .env: HUE_DB_HOST, SAIGON_DB_HOST, ... (mặc định: localhost)
    server: process.env[`${prefix}_DB_HOST`] || "localhost",
    // Đọc từ .env: HUE_DB_PORT, SAIGON_DB_PORT, ...  (mặc định: 1401, 1402,1403 )
    port: Number(process.env[`${prefix}_DB_PORT`] || branch.defaultPort),
    // Đọc từ .env: HUE_DB_USER, SAIGON_DB_USER, ... (mặc định: sa)
    user: process.env[`${prefix}_DB_USER`] || "sa",
    // Đọc từ .env: HUE_DB_PASSWORD, SAIGON_DB_PASSWORD, ...
    password: process.env[`${prefix}_DB_PASSWORD`] || "YourStrong(!)Password",
    // Đọc từ .env: HUE_DB_NAME (Store_H), SAIGON_DB_NAME (Store_SG), ...
    database: process.env[`${prefix}_DB_NAME`] || branch.defaultDatabase,
    // Tùy chọn kết nối
    options: {
      encrypt: false,  // Không mã hóa kết nối (cho local testing)
      trustServerCertificate: true,  // Tin tưởng chứng chỉ self-signed
    },
    // Cấu hình connection pool (có tối đa 10 kết nối đồng thời)
    pool: {
      max: 10,        // Tối đa 10 kết nối
      min: 0,         // Tối thiểu 0 kết nối
      idleTimeoutMillis: 30000,  // Đóng kết nối nếu không dùng trong 30 giây
    },
  };
}

/**
 * Lấy connection pool cho một chi nhánh (tạo hoặc dùng cái cũ từ cache)
 * @param {string} branchCode - Mã chi nhánh (HUE, SAIGON, HANOI, CENTRAL)
 * @returns {object} Connection pool sẵn sàng dùng
 * 
 * VD: 
 *   getPool("HUE") → kết nối đến Server localhost:1401, DB: Store_H
 *   getPool("CENTRAL") → kết nối đến Server localhost:1404, DB: CentralDB
 */
async function getPool(branchCode) {
  const branch = getBranchConfig(branchCode);
  if (!branch) {
    throw new Error(`Unsupported branch: ${branchCode}`);
  }

  // Nếu chưa tạo pool cho chi nhánh này, tạo mới
  if (!poolCache.has(branch.code)) {
    const config = toConnectionConfig(branch.code);
    const pool = new sql.ConnectionPool(config);
    // Kết nối async đến server
    const poolConnect = pool.connect();
    // Lưu vào cache để lần sau không cần tạo lại
    poolCache.set(branch.code, poolConnect);
  }

  // Trả về pool từ cache
  return poolCache.get(branch.code);
}

async function closeAllPools() {
  const entries = Array.from(poolCache.entries());
  for (const [key, poolPromise] of entries) {
    const pool = await poolPromise;
    await pool.close();
    poolCache.delete(key);
  }
}

module.exports = {
  sql,
  isMockMode,
  getPool,
  closeAllPools,
};
