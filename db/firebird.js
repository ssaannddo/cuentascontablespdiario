const Firebird = require('node-firebird');

function normalizeConfig(cfg = {}) {
  return {
    host: cfg.host || '127.0.0.1',
    port: Number(cfg.port || 3050),
    database: cfg.database || '',
    user: cfg.user || 'SYSDBA',
    password: cfg.password || 'masterkey',
    lowercase_keys: false,
    role: null,
    pageSize: 4096
  };
}

function attachDatabase(config) {
  const options = normalizeConfig(config);

  return new Promise((resolve, reject) => {
    Firebird.attach(options, (err, db) => {
      if (err) return reject(err);
      resolve(db);
    });
  });
}

function detachDatabase(db) {
  return new Promise((resolve) => {
    if (!db) return resolve();
    db.detach(() => resolve());
  });
}

async function testConnection(config) {
  let db;
  try {
    db = await attachDatabase(config);
    return { ok: true, message: 'Conexión exitosa' };
  } catch (error) {
    return { ok: false, message: error.message || String(error) };
  } finally {
    await detachDatabase(db);
  }
}

async function query(config, sql, params = []) {
  let db;
  try {
    db = await attachDatabase(config);

    const rows = await new Promise((resolve, reject) => {
      db.query(sql, params, (err, result) => {
        if (err) return reject(err);
        resolve(result || []);
      });
    });

    return rows;
  } finally {
    await detachDatabase(db);
  }
}

async function execute(config, sql, params = []) {
  let db;
  try {
    db = await attachDatabase(config);

    const result = await new Promise((resolve, reject) => {
      db.query(sql, params, (err, res) => {
        if (err) return reject(err);
        resolve(res);
      });
    });

    return result;
  } finally {
    await detachDatabase(db);
  }
}

module.exports = {
  testConnection,
  query,
  execute
};