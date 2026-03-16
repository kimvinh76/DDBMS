const BRANCHES = Object.freeze({
  HUE: {
    code: "HUE",
    label: "Chi nhanh Hue",
    envPrefix: "HUE",
    defaultPort: 1401,
    defaultDatabase: "Store_H",
  },
  SAIGON: {
    code: "SAIGON",
    label: "Chi nhanh Sai Gon",
    envPrefix: "SAIGON",
    defaultPort: 1402,
    defaultDatabase: "Store_SG",
  },
  HANOI: {
    code: "HANOI",
    label: "Chi nhanh Ha Noi",
    envPrefix: "HANOI",
    defaultPort: 1403,
    defaultDatabase: "Store_HN",
  },
  CENTRAL: {
    code: "CENTRAL",
    label: "Tong cong ty",
    envPrefix: "CENTRAL",
    defaultPort: 1404,
    defaultDatabase: "CentralDB",
  },
});

function normalizeBranch(value) {
  if (!value) {
    return null;
  }
  const normalized = String(value).trim().toUpperCase();
  return BRANCHES[normalized] ? normalized : null;
}

function isCentralBranch(branchCode) {
  return normalizeBranch(branchCode) === "CENTRAL";
}

function getBranchConfig(branchCode) {
  const normalized = normalizeBranch(branchCode);
  return normalized ? BRANCHES[normalized] : null;
}

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
