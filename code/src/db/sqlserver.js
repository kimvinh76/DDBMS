const sql = require("mssql");
const { getBranchConfig } = require("../config/branches");

const poolCache = new Map();

function isMockMode() {
  return process.env.MOCK_MODE !== "false";
}

function toConnectionConfig(branchCode) {
  const branch = getBranchConfig(branchCode);
  if (!branch) {
    throw new Error(`Unsupported branch: ${branchCode}`);
  }

  const prefix = branch.envPrefix;
  return {
    server: process.env[`${prefix}_DB_HOST`] || "localhost",
    port: Number(process.env[`${prefix}_DB_PORT`] || branch.defaultPort),
    user: process.env[`${prefix}_DB_USER`] || "sa",
    password: process.env[`${prefix}_DB_PASSWORD`] || "YourStrong(!)Password",
    database: process.env[`${prefix}_DB_NAME`] || branch.defaultDatabase,
    options: {
      encrypt: false,
      trustServerCertificate: true,
    },
    pool: {
      max: 10,
      min: 0,
      idleTimeoutMillis: 30000,
    },
  };
}

async function getPool(branchCode) {
  const branch = getBranchConfig(branchCode);
  if (!branch) {
    throw new Error(`Unsupported branch: ${branchCode}`);
  }

  if (!poolCache.has(branch.code)) {
    const config = toConnectionConfig(branch.code);
    const pool = new sql.ConnectionPool(config);
    const poolConnect = pool.connect();
    poolCache.set(branch.code, poolConnect);
  }

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
