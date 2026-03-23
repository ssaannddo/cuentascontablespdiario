const express = require('express');
const path = require('path');
const { testConnection } = require('./db/firebird');
const { generarVistaPrevia, actualizarRegistros } = require('./services/conciliador');

const app = express();
const PORT = 3000;

app.use(express.json({ limit: '5mb' }));
app.use(express.static(path.join(__dirname, 'public')));

const runtimeConfig = {
  coi: null,
  sae: null
};

app.post('/api/test-coi', async (req, res) => {
  try {
    const config = req.body;
    const result = await testConnection(config);

    if (result.ok) runtimeConfig.coi = config;

    res.json(result);
  } catch (error) {
    res.status(500).json({
      ok: false,
      message: error.message || String(error)
    });
  }
});

app.post('/api/test-sae', async (req, res) => {
  try {
    const config = req.body;
    const result = await testConnection(config);

    if (result.ok) runtimeConfig.sae = config;

    res.json(result);
  } catch (error) {
    res.status(500).json({
      ok: false,
      message: error.message || String(error)
    });
  }
});

app.post('/api/preview', async (req, res) => {
  try {
    const { anioTabla } = req.body;

    if (!runtimeConfig.coi) {
      return res.status(400).json({ ok: false, message: 'COI no está conectado.' });
    }

    if (!runtimeConfig.sae) {
      return res.status(400).json({ ok: false, message: 'SAE no está conectado.' });
    }

    const rows = await generarVistaPrevia(runtimeConfig.coi, runtimeConfig.sae, anioTabla);

    res.json({
      ok: true,
      total: rows.length,
      rows
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      message: error.message || String(error)
    });
  }
});

app.post('/api/update', async (req, res) => {
  try {
    const { anioTabla, items } = req.body;

    if (!runtimeConfig.coi) {
      return res.status(400).json({ ok: false, message: 'COI no está conectado.' });
    }

    const result = await actualizarRegistros(runtimeConfig.coi, anioTabla, items || []);

    res.json({
      ok: true,
      total: result.length,
      rows: result
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      message: error.message || String(error)
    });
  }
});

app.listen(PORT, () => {
  console.log(`Servidor activo en http://localhost:${PORT}`);
});