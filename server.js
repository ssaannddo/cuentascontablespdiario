const express = require('express');
const fs = require('fs');
const path = require('path');
const { execFile } = require('child_process');
const { testConnection, query } = require('./db/firebird');
const {
  generarVistaPrevia,
  actualizarRegistros
} = require('./services/conciliador');

const app = express();
const PORT = 3000;

app.use(express.json({ limit: '5mb' }));
app.use(express.static(path.join(__dirname, 'public')));

const runtimeConfig = {
  coi: null,
  sae: null
};

const dialogState = {
  coi: null,
  sae: null
};

function obtenerDirectoriosBaseSugeridos() {
  const userProfile = process.env.USERPROFILE || '';

  return [
    path.join(userProfile, 'OneDrive', 'Escritorio', 'bases'),
    path.join(userProfile, 'OneDrive', 'Desktop', 'bases'),
    path.join(userProfile, 'Desktop', 'bases'),
    process.cwd()
  ];
}

function resolverDirectorioInicial(currentPath, kind) {
  const valor = String(currentPath || '').trim();

  if (valor) {
    try {
      if (fs.existsSync(valor) && fs.statSync(valor).isDirectory()) {
        return valor;
      }

      const directorio = path.dirname(valor);
      if (fs.existsSync(directorio) && fs.statSync(directorio).isDirectory()) {
        return directorio;
      }
    } catch {
      // Ignorar y seguir con otros candidatos.
    }
  }

  const ultimoDirectorio = kind && dialogState[kind];
  if (ultimoDirectorio && fs.existsSync(ultimoDirectorio) && fs.statSync(ultimoDirectorio).isDirectory()) {
    return ultimoDirectorio;
  }

  const runtimeDatabase = kind && runtimeConfig[kind] && runtimeConfig[kind].database;
  if (runtimeDatabase) {
    try {
      const runtimeDirectory = path.dirname(runtimeDatabase);
      if (fs.existsSync(runtimeDirectory) && fs.statSync(runtimeDirectory).isDirectory()) {
        return runtimeDirectory;
      }
    } catch {
      // Ignorar y seguir con otros candidatos.
    }
  }

  for (const candidate of obtenerDirectoriosBaseSugeridos()) {
    try {
      if (fs.existsSync(candidate) && fs.statSync(candidate).isDirectory()) {
        return candidate;
      }
    } catch {
      // Ignorar y probar el siguiente.
    }
  }

  return process.cwd();
}

function seleccionarArchivoBase({ currentPath, title, kind } = {}) {
  if (process.platform !== 'win32') {
    return Promise.reject(new Error('El selector de archivos solo esta disponible en Windows.'));
  }

  const initialDirectory = resolverDirectorioInicial(currentPath, kind);
  const dialogTitle = String(title || 'Seleccionar base de datos Firebird');
  const script = [
    'Add-Type -AssemblyName System.Windows.Forms',
    'Add-Type -AssemblyName System.Drawing',
    '[System.Windows.Forms.Application]::EnableVisualStyles()',
    '$owner = New-Object System.Windows.Forms.Form',
    '$owner.StartPosition = [System.Windows.Forms.FormStartPosition]::CenterScreen',
    '$owner.Size = New-Object System.Drawing.Size(1, 1)',
    '$owner.Opacity = 0',
    '$owner.ShowInTaskbar = $false',
    '$owner.TopMost = $true',
    '$owner.FormBorderStyle = [System.Windows.Forms.FormBorderStyle]::FixedToolWindow',
    '$owner.Show()',
    '$owner.Activate()',
    '$dialog = New-Object System.Windows.Forms.OpenFileDialog',
    "$dialog.Filter = 'Bases de datos Firebird (*.fdb)|*.fdb|Todos los archivos (*.*)|*.*'",
    '$dialog.CheckFileExists = $true',
    '$dialog.Multiselect = $false',
    '$dialog.FilterIndex = 1',
    '$dialog.RestoreDirectory = $false',
    '$dialog.AutoUpgradeEnabled = $true',
    '$dialog.Title = $env:CODEX_DIALOG_TITLE',
    'if ($env:CODEX_INITIAL_DIR -and (Test-Path $env:CODEX_INITIAL_DIR)) {',
    '  $dialog.InitialDirectory = $env:CODEX_INITIAL_DIR',
    '}',
    '$result = $dialog.ShowDialog($owner)',
    'if ($result -eq [System.Windows.Forms.DialogResult]::OK) {',
    '  [Console]::OutputEncoding = [System.Text.Encoding]::UTF8',
    '  Write-Output $dialog.FileName',
    '}',
    '$dialog.Dispose()',
    '$owner.Close()',
    '$owner.Dispose()'
  ].join('; ');

  return new Promise((resolve, reject) => {
    execFile(
      'powershell',
      ['-NoProfile', '-STA', '-Command', script],
      {
        windowsHide: true,
        encoding: 'utf8',
        maxBuffer: 1024 * 1024,
        env: {
          ...process.env,
          CODEX_INITIAL_DIR: initialDirectory,
          CODEX_DIALOG_TITLE: dialogTitle
        }
      },
      (error, stdout, stderr) => {
        if (error) {
          return reject(new Error((stderr || error.message || String(error)).trim()));
        }

        const selectedPath = String(stdout || '').trim() || null;

        if (selectedPath && kind) {
          try {
            dialogState[kind] = path.dirname(selectedPath);
          } catch {
            dialogState[kind] = null;
          }
        }

        resolve(selectedPath);
      }
    );
  });
}

async function obtenerAniosCoiDisponibles(config) {
  const sql = `
    SELECT TRIM(RDB$RELATION_NAME) AS RELATION_NAME
    FROM RDB$RELATIONS
    WHERE COALESCE(RDB$SYSTEM_FLAG, 0) = 0
      AND RDB$VIEW_BLR IS NULL
      AND TRIM(RDB$RELATION_NAME) STARTING WITH 'AUXILIAR'
    ORDER BY TRIM(RDB$RELATION_NAME)
  `;

  const rows = await query(config, sql);

  return rows
    .map(row => String(row.RELATION_NAME || row['RDB$RELATION_NAME'] || '').trim().toUpperCase())
    .map(tabla => {
      const match = tabla.match(/^AUXILIAR(\d{2})$/);
      if (!match) return null;

      return {
        anioTabla: match[1],
        tabla
      };
    })
    .filter(Boolean)
    .sort((a, b) => Number(b.anioTabla) - Number(a.anioTabla));
}

app.post('/api/test-coi', async (req, res) => {
  try {
    const config = req.body;
    const result = await testConnection(config);

    if (result.ok) {
      runtimeConfig.coi = config;
    } else {
      runtimeConfig.coi = null;
    }

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

    if (result.ok) {
      runtimeConfig.sae = config;
    } else {
      runtimeConfig.sae = null;
    }

    res.json(result);
  } catch (error) {
    res.status(500).json({
      ok: false,
      message: error.message || String(error)
    });
  }
});

app.get('/api/coi-years', async (req, res) => {
  try {
    if (!runtimeConfig.coi) {
      return res.status(400).json({
        ok: false,
        message: 'COI no esta conectado.'
      });
    }

    const years = await obtenerAniosCoiDisponibles(runtimeConfig.coi);

    res.json({
      ok: true,
      total: years.length,
      years
    });
  } catch (error) {
    console.error('Error en /api/coi-years:', error);
    res.status(500).json({
      ok: false,
      message: error.message || String(error)
    });
  }
});

app.post('/api/pick-database', async (req, res) => {
  try {
    const { currentPath, title, kind } = req.body || {};
    const selectedPath = await seleccionarArchivoBase({ currentPath, title, kind });

    res.json({
      ok: true,
      cancelled: !selectedPath,
      path: selectedPath
    });
  } catch (error) {
    console.error('Error en /api/pick-database:', error);
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
      return res.status(400).json({ ok: false, message: 'COI no esta conectado.' });
    }

    if (!runtimeConfig.sae) {
      return res.status(400).json({ ok: false, message: 'SAE no esta conectado.' });
    }

    const { resultados, errores } = await generarVistaPrevia(
      runtimeConfig.coi,
      runtimeConfig.sae,
      anioTabla
    );

    res.json({
      ok: true,
      total: resultados.length,
      erroresTotal: errores.length,
      rows: resultados,
      errores
    });
  } catch (error) {
    console.error('Error en /api/preview:', error);
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
      return res.status(400).json({ ok: false, message: 'COI no esta conectado.' });
    }

    const rows = await actualizarRegistros(
      runtimeConfig.coi,
      anioTabla,
      items || []
    );

    res.json({
      ok: true,
      total: rows.length,
      rows
    });
  } catch (error) {
    console.error('Error en /api/update:', error);
    res.status(500).json({
      ok: false,
      message: error.message || String(error)
    });
  }
});

app.listen(PORT, () => {
  console.log(`Servidor activo en http://localhost:${PORT}`);
});
