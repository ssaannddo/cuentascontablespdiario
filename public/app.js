let previewRows = [];

function getConfig(prefix) {
  return {
    host: document.getElementById(`${prefix}-host`).value.trim(),
    port: document.getElementById(`${prefix}-port`).value.trim(),
    database: document.getElementById(`${prefix}-database`).value.trim(),
    user: document.getElementById(`${prefix}-user`).value.trim(),
    password: document.getElementById(`${prefix}-password`).value
  };
}

async function testConexion(tipo) {
  const statusEl = document.getElementById(`${tipo}-status`);
  statusEl.textContent = 'Probando conexión...';
  statusEl.className = 'status pending';

  try {
    const endpoint = tipo === 'coi' ? '/api/test-coi' : '/api/test-sae';

    const response = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(getConfig(tipo))
    });

    const data = await response.json();

    if (data.ok) {
      statusEl.textContent = data.message;
      statusEl.className = 'status ok';
    } else {
      statusEl.textContent = data.message;
      statusEl.className = 'status error';
    }
  } catch (error) {
    statusEl.textContent = error.message || String(error);
    statusEl.className = 'status error';
  }
}

async function generarPreview() {
  const anioTabla = document.getElementById('anioTabla').value.trim();
  const meta = document.getElementById('preview-meta');
  const raw = document.getElementById('raw-output');
  const container = document.getElementById('tabla-container');

  meta.textContent = 'Procesando vista previa...';
  raw.textContent = '';
  container.innerHTML = '';

  try {
    const response = await fetch('/api/preview', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ anioTabla })
    });

    const data = await response.json();

    if (!data.ok) {
      meta.textContent = data.message;
      raw.textContent = JSON.stringify(data, null, 2);
      return;
    }

    previewRows = data.rows || [];
    meta.textContent = `Registros procesados: ${data.total}`;
    renderTable(previewRows, container);
    raw.textContent = JSON.stringify(previewRows, null, 2);
  } catch (error) {
    meta.textContent = error.message || String(error);
  }
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
  const anioTabla = document.getElementById('anioTabla').value.trim();
  const raw = document.getElementById('raw-output');
  const meta = document.getElementById('preview-meta');

  const checkboxes = [...document.querySelectorAll('input[type="checkbox"][data-index]')];
  const seleccionados = checkboxes
    .filter(cb => cb.checked)
    .map(cb => previewRows[Number(cb.dataset.index)]);

  if (!seleccionados.length) {
    meta.textContent = 'No hay registros seleccionados para actualizar.';
    return;
  }

  meta.textContent = 'Actualizando registros seleccionados...';

  try {
    const response = await fetch('/api/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ anioTabla, items: seleccionados })
    });

    const data = await response.json();

    if (!data.ok) {
      meta.textContent = data.message;
      raw.textContent = JSON.stringify(data, null, 2);
      return;
    }

    meta.textContent = `Actualización finalizada. Registros procesados: ${data.total}`;
    raw.textContent = JSON.stringify(data.rows, null, 2);
  } catch (error) {
    meta.textContent = error.message || String(error);
  }
}