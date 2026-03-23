const { query, execute } = require('../db/firebird');

function formatearCuentaContable(cuenta) {
  if (!cuenta) return null;

  const sinGuiones = String(cuenta).replace(/-/g, '').trim();

  if (!sinGuiones) return null;
  if (!/^\d+$/.test(sinGuiones)) return null;
  if (sinGuiones.length > 20) return null;

  const cuenta20 = sinGuiones.padEnd(20, '0');
  return cuenta20 + '4';
}

function obtenerNombreTablaAuxiliar(anioCorto) {
  const valor = String(anioCorto).trim();

  if (!/^\d{2}$/.test(valor)) {
    throw new Error('El año corto debe tener 2 dígitos. Ejemplo: 25');
  }

  return `AUXILIAR${valor}`;
}

async function obtenerMovimientosCoi(configCoi, anioTabla) {
  const tablaAux = obtenerNombreTablaAuxiliar(anioTabla);

  const sql = `
    SELECT
      TIPO_POLI,
      NUM_POLIZ,
      PERIODO,
      EJERCICIO,
      FECHA_POL,
      NUM_PART,
      NUM_CTA,
      DEBE_HABER,
      CONCEP_PO,
      TRIM(
        SUBSTRING(
          CONCEP_PO FROM
          POSITION('Cliente ' IN CONCEP_PO) + 8
          FOR
          POSITION(',' IN CONCEP_PO) - (POSITION('Cliente ' IN CONCEP_PO) + 8)
        )
      ) AS NUMERO_CLIENTE
    FROM ${tablaAux}
    WHERE TIPO_POLI = 'Dr'
      AND DEBE_HABER = 'D'
      AND NUM_CTA STARTING WITH '105'
      AND CONCEP_PO LIKE '%Cliente %,%'
    ORDER BY EJERCICIO, PERIODO, NUM_POLIZ, NUM_PART
  `;

  return query(configCoi, sql);
}

async function buscarClienteSae(configSae, numeroCliente) {
  const cliente = String(numeroCliente || '').trim();

  const clienteSinCeros = cliente.replace(/^0+/, '') || '0';
  const cliente6 = cliente.padStart(6, '0');
  const cliente10 = cliente.padStart(10, '0');

  const sql = `
    SELECT
      CLAVE,
      NOMBRE,
      CUENTA_CONTABLE
    FROM CLIE03
    WHERE TRIM(CLAVE) = ?
       OR TRIM(CLAVE) = ?
       OR TRIM(CLAVE) = ?
       OR TRIM(CLAVE) = ?
  `;

  const rows = await query(configSae, sql, [
    cliente,
    clienteSinCeros,
    cliente6,
    cliente10
  ]);

  if (!rows.length) {
    return null;
  }

  return rows[0];
}

async function generarVistaPrevia(configCoi, configSae, anioTabla) {
  const movimientos = await obtenerMovimientosCoi(configCoi, anioTabla);
  const resultados = [];

  for (const mov of movimientos) {
    const numeroCliente = String(mov.NUMERO_CLIENTE || '').trim();

    if (!numeroCliente) {
      resultados.push({
        ...mov,
        ESTATUS: 'No se pudo extraer cliente',
        CUENTA_SAE: null,
        CUENTA_FORMATEADA: null,
        REQUIERE_ACTUALIZACION: false
      });
      continue;
    }

    const cliente = await buscarClienteSae(configSae, numeroCliente);

    if (!cliente) {
      resultados.push({
        ...mov,
        NUMERO_CLIENTE: numeroCliente,
        ESTATUS: 'Cliente no encontrado en SAE',
        CUENTA_SAE: null,
        CUENTA_FORMATEADA: null,
        REQUIERE_ACTUALIZACION: false
      });
      continue;
    }

    const cuentaFormateada = formatearCuentaContable(cliente.CUENTA_CONTABLE);

    if (!cuentaFormateada) {
      resultados.push({
        ...mov,
        NUMERO_CLIENTE: numeroCliente,
        CUENTA_SAE: cliente.CUENTA_CONTABLE,
        CUENTA_FORMATEADA: null,
        ESTATUS: 'Cuenta SAE inválida',
        REQUIERE_ACTUALIZACION: false
      });
      continue;
    }

    const cuentaActual = String(mov.NUM_CTA || '').trim();
    const requiereActualizacion = cuentaActual !== cuentaFormateada;

    resultados.push({
        ...mov,
        NUMERO_CLIENTE: numeroCliente,
        CLIENTE_SAE_CLAVE: cliente.CLAVE,
        CLIENTE_SAE_NOMBRE: cliente.NOMBRE,
        CUENTA_SAE: cliente.CUENTA_CONTABLE,
        CUENTA_FORMATEADA: cuentaFormateada,
        ESTATUS: requiereActualizacion ? 'Requiere actualización' : 'Correcta',
        REQUIERE_ACTUALIZACION: requiereActualizacion
    });
  }

  return resultados;
}

async function actualizarRegistros(configCoi, anioTabla, items = []) {
  const tablaAux = obtenerNombreTablaAuxiliar(anioTabla);

  const sqlUpdate = `
    UPDATE ${tablaAux}
    SET NUM_CTA = ?
    WHERE TIPO_POLI = ?
      AND NUM_POLIZ = ?
      AND PERIODO = ?
      AND EJERCICIO = ?
      AND NUM_PART = ?
  `;

  const resultados = [];

  for (const item of items) {
    if (!item.REQUIERE_ACTUALIZACION) {
      resultados.push({
        ...item,
        UPDATE_RESULT: 'Omitido'
      });
      continue;
    }

    try {
      await execute(configCoi, sqlUpdate, [
        item.CUENTA_FORMATEADA,
        item.TIPO_POLI,
        item.NUM_POLIZ,
        item.PERIODO,
        item.EJERCICIO,
        item.NUM_PART
      ]);

      resultados.push({
        ...item,
        UPDATE_RESULT: 'Actualizado'
      });
    } catch (error) {
      resultados.push({
        ...item,
        UPDATE_RESULT: `Error: ${error.message || String(error)}`
      });
    }
  }

  return resultados;
}

module.exports = {
  formatearCuentaContable,
  generarVistaPrevia,
  actualizarRegistros
};