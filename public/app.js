let previewRows = [];

function detectarEmpresaDesdeRuta(databasePath) {
  const valor = String(databasePath || '').trim();
  const match = valor.match(/EMPRE0*(\d{1,2})(?:\D|$)/i);

  if (!match) {
    return null;
  }

  return String(Number(match[1])).padStart(2, '0');
}

function sincronizarEmpresaDetectada() {
  const empresaInput = document.getElementById('sae-empresa');
  const saeDatabase = document.getElementById('sae-database').value.trim();
  const coiDatabase = document.getElementById('coi-database').value.trim();

  const detectada = detectarEmpresaDesdeRuta(saeDatabase) || detectarEmpresaDesdeRuta(coiDatabase);
  if (!empresaInput || !detectada) {
    return null;
  }

  empresaInput.value = detectada;
  return detectada;
}

function getConfig(prefix) {
  if (prefix === 'sae') {
    sincronizarEmpresaDetectada();
  }

  const config = {
    host: document.getElementById(`${prefix}-host`).value.trim(),
    port: document.getElementById(`${prefix}-port`).value.trim(),
    database: document.getElementById(`${prefix}-database`).value.trim(),
    user: document.getElementById(`${prefix}-user`).value.trim(),
    password: document.getElementById(`${prefix}-password`).value
  };

  const empresaEl = document.getElementById(`${prefix}-empresa`);
  if (empresaEl) {
    config.empresa = empresaEl.value.trim();
  }

  return config;
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  const text = await response.text();

  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`HTTP ${response.status} en ${url}. Reinicia el servidor para cargar los cambios nuevos.`);
  }
}

function setStatus(tipo, message, state) {
  const statusEl = document.getElementById(`${tipo}-status`);
  statusEl.textContent = message;
  statusEl.className = `status ${state}`;
}

async function seleccionarBase(tipo) {
  const input = document.getElementById(`${tipo}-database`);
  const button = document.getElementById(`${tipo}-pick-button`);
  const originalText = button.textContent;

  button.disabled = true;
  button.textContent = 'Abriendo explorador...';

  try {
    const data = await fetchJson('/api/pick-database', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        kind: tipo,
        currentPath: input.value.trim(),
        title: tipo === 'coi' ? 'Seleccionar base COI' : 'Seleccionar base SAE'
      })
    });

    if (!data.ok) {
      throw new Error(data.message || 'No se pudo seleccionar el archivo');
    }

    if (data.cancelled) {
      return;
    }

    input.value = data.path || input.value;
    const empresaDetectada = sincronizarEmpresaDetectada();

    if (tipo === 'coi') {
      resetAniosCoi('Prueba la conexion COI para cargar los anios');
      previewRows = [];
    }

    let message = `Ruta seleccionada. Prueba la conexion ${tipo.toUpperCase()}.`;
    if (empresaDetectada) {
      message += ` Empresa detectada: ${empresaDetectada}.`;
    }

    setStatus(tipo, message, 'pending');
  } catch (error) {
    setStatus(tipo, error.message || String(error), 'error');
  } finally {
    button.disabled = false;
    button.textContent = originalText;
  }
}

function renderAniosCoi(anios = []) {
  const select = document.getElementById('anioTabla');
  select.innerHTML = '';

  if (!anios.length) {
    select.disabled = true;

    const option = document.createElement('option');
    option.value = '';
    option.textContent = 'No se encontraron tablas AUXILIARxx';
    select.appendChild(option);
    return;
  }

  anios.forEach(item => {
    const option = document.createElement('option');
    option.value = item.anioTabla;
    option.textContent = `${item.anioTabla} (${item.tabla})`;
    select.appendChild(option);
  });

  select.disabled = false;
}

function resetAniosCoi(message = 'Primero conecta COI') {
  const select = document.getElementById('anioTabla');
  select.innerHTML = '';
  select.disabled = true;

  const option = document.createElement('option');
  option.value = '';
  option.textContent = message;
  select.appendChild(option);
}

async function cargarAniosCoi() {
  const data = await fetchJson('/api/coi-years');

  if (!data.ok) {
    throw new Error(data.message || 'No se pudieron cargar los anios de COI');
  }

  renderAniosCoi(data.years || []);
  return data;
}

function obtenerAnioSeleccionado() {
  const anioTabla = document.getElementById('anioTabla').value.trim();

  if (!anioTabla) {
    throw new Error('Selecciona un anio disponible de COI');
  }

  return anioTabla;
}

async function testConexion(tipo) {
  const statusEl = document.getElementById(`${tipo}-status`);
  statusEl.textContent = 'Probando conexion...';
  statusEl.className = 'status pending';

  try {
    const endpoint = tipo === 'coi' ? '/api/test-coi' : '/api/test-sae';

    const data = await fetchJson(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(getConfig(tipo))
    });

    if (!data.ok) {
      if (tipo === 'coi') {
        resetAniosCoi();
      }

      statusEl.textContent = data.message;
      statusEl.className = 'status error';
      return;
    }

    if (tipo === 'coi') {
      try {
        const yearsData = await cargarAniosCoi();
        statusEl.textContent = `${data.message}. Anios detectados: ${yearsData.total}`;
      } catch (error) {
        resetAniosCoi('No se pudieron cargar los anios');
        statusEl.textContent = `${data.message}. ${error.message || String(error)}`;
        statusEl.className = 'status error';
        return;
      }
    } else {
      statusEl.textContent = data.message;
    }

    statusEl.className = 'status ok';
  } catch (error) {
    if (tipo === 'coi') {
      resetAniosCoi();
    }

    statusEl.textContent = error.message || String(error);
    statusEl.className = 'status error';
  }
}

async function generarPreview() {
  const anioTabla = obtenerAnioSeleccionado();
  const meta = document.getElementById('preview-meta');
  const raw = document.getElementById('raw-output');
  const container = document.getElementById('tabla-container');
  const errorContainer = document.getElementById('errores-container');

  meta.textContent = 'Procesando vista previa...';
  raw.textContent = '';
  container.innerHTML = '';
  errorContainer.innerHTML = '';

  try {
    const data = await fetchJson('/api/preview', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ anioTabla })
    });

    if (!data.ok) {
      meta.textContent = data.message;
      raw.textContent = JSON.stringify(data, null, 2);
      return;
    }

    previewRows = data.rows || [];
    const errores = data.errores || [];
    meta.textContent = `Registros procesados: ${data.total} | Errores detectados: ${data.erroresTotal}`;

    renderTable(previewRows, container);
    if (errores.length > 0) {
      renderErrorTable(errores, errorContainer);
    }
    raw.textContent = JSON.stringify(data, null, 2);
  } catch (error) {
    meta.textContent = error.message || String(error);
  }
}

function renderErrorTable(errores, container) {
  if (!errores.length) {
    container.innerHTML = '';
    return;
  }

  container.innerHTML = '<h3>Errores detectados (para rastrear manualmente)</h3>';

  const table = document.createElement('table');
  const thead = document.createElement('thead');
  const tbody = document.createElement('tbody');

  const columns = [
    'TIPO_ERROR',
    'POLIZA',
    'PERIODO',
    'EJERCICIO',
    'NUM_PART',
    'CLIENTE_BUSCADO',
    'CONCEPTO',
    'MENSAJE'
  ];

  const trHead = document.createElement('tr');
  columns.forEach(col => {
    const th = document.createElement('th');
    th.textContent = col;
    trHead.appendChild(th);
  });
  thead.appendChild(trHead);

  errores.forEach(error => {
    const tr = document.createElement('tr');

    const celdas = {
      'TIPO_ERROR': error.tipo,
      'POLIZA': error.NUM_POLIZ,
      'PERIODO': error.PERIODO,
      'EJERCICIO': error.EJERCICIO,
      'NUM_PART': error.NUM_PART,
      'CLIENTE_BUSCADO': error.NUMERO_CLIENTE || error.CVE_CLPV_BUSCADA || error.CUENTA_SAE || '-',
      'CONCEPTO': error.CONCEP_PO,
      'MENSAJE': error.mensaje
    };

    columns.forEach(col => {
      const td = document.createElement('td');
      td.textContent = celdas[col] || '';
      tr.appendChild(td);
    });

    tbody.appendChild(tr);
  });

  table.appendChild(thead);
  table.appendChild(tbody);
  container.appendChild(table);
}

function renderTable(rows, container) {
  if (!rows.length) {
    container.innerHTML = '<p>Sin resultados.</p>';
    return;
  }

  const table = document.createElement('table');
  const thead = document.createElement('thead');
  const tbody = document.createElement('tbody');

  const columns = [
    'SEL',
    'TIPO_POLI',
    'NUM_POLIZ',
    'PERIODO',
    'EJERCICIO',
    'NUM_PART',
    'NUMERO_CLIENTE',
    'CLIENTE_SAE_CLAVE',
    'CLIENTE_SAE_NOMBRE',
    'NUM_CTA',
    'CUENTA_SAE',
    'CUENTA_FORMATEADA',
    'ESTATUS',
    'CONCEP_PO'
  ];

  const trHead = document.createElement('tr');
  columns.forEach(col => {
    const th = document.createElement('th');
    th.textContent = col;
    trHead.appendChild(th);
  });
  thead.appendChild(trHead);

  rows.forEach((row, index) => {
    const tr = document.createElement('tr');

    columns.forEach(col => {
      const td = document.createElement('td');

      if (col === 'SEL') {
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.checked = !!row.REQUIERE_ACTUALIZACION;
        checkbox.disabled = !row.REQUIERE_ACTUALIZACION;
        checkbox.dataset.index = index;
        td.appendChild(checkbox);
      } else {
        td.textContent = row[col] ?? '';
      }

      tr.appendChild(td);
    });

    tbody.appendChild(tr);
  });

  table.appendChild(thead);
  table.appendChild(tbody);
  container.innerHTML = '';
  container.appendChild(table);
}

async function actualizarSeleccionados() {
  const anioTabla = obtenerAnioSeleccionado();
  const raw = document.getElementById('raw-output');
  const meta = document.getElementById('preview-meta');

  const checkboxes = [...document.querySelectorAll('input[type="checkbox"][data-index]')];
  const seleccionados = checkboxes
    .filter(cb => cb.checked)
    .map(cb => previewRows[Number(cb.dataset.index)])
    .filter(row => row.REQUIERE_ACTUALIZACION === true && row.CUENTA_FORMATEADA);

  if (!seleccionados.length) {
    meta.textContent = 'No hay registros seleccionados para actualizar.';
    return;
  }

  meta.textContent = 'Actualizando registros seleccionados...';

  try {
    const data = await fetchJson('/api/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ anioTabla, items: seleccionados })
    });

    if (!data.ok) {
      meta.textContent = data.message;
      raw.textContent = JSON.stringify(data, null, 2);
      return;
    }

    meta.textContent = `Actualizacion finalizada. Registros procesados: ${data.total}`;
    raw.textContent = JSON.stringify(data.rows, null, 2);
  } catch (error) {
    meta.textContent = error.message || String(error);
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  const coiDatabaseInput = document.getElementById('coi-database');
  const saeDatabaseInput = document.getElementById('sae-database');

  coiDatabaseInput.addEventListener('change', sincronizarEmpresaDetectada);
  coiDatabaseInput.addEventListener('blur', sincronizarEmpresaDetectada);
  saeDatabaseInput.addEventListener('change', sincronizarEmpresaDetectada);
  saeDatabaseInput.addEventListener('blur', sincronizarEmpresaDetectada);
  sincronizarEmpresaDetectada();

  try {
    await cargarAniosCoi();
  } catch {
    resetAniosCoi();
  }
});
