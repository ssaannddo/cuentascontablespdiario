const { query, execute } = require('../db/firebird');

function formatearCuentaContable(cuenta, maxLength = 20) {
  if (!cuenta) return null;

  const sinGuiones = String(cuenta).replace(/-/g, '').trim();

  if (!sinGuiones) return null;
  if (!/^\d+$/.test(sinGuiones)) return null;

  const length = Number(maxLength) || 20;
  if (length <= 0) return null;

  const base = sinGuiones.padEnd(length, '0');
  if (length === 1) return '4';
  return base.slice(0, length - 1) + '4';
}

function detectarEmpresaDesdeRuta(databasePath) {
  const valor = String(databasePath || '').trim();
  const match = valor.match(/EMPRE0*(\d{1,2})(?:\D|$)/i);

  if (!match) return null;

  return String(Number(match[1])).padStart(2, '0');
}

function normalizarEmpresaSae(empresa, databasePath) {
  const detectada = detectarEmpresaDesdeRuta(databasePath);
  const valor = String(empresa ?? '').trim();

  if (detectada) return detectada;
  if (!valor) return '03';

  if (!/^\d{1,2}$/.test(valor)) {
    throw new Error('La empresa SAE debe ser numerica de 1 o 2 digitos. Ejemplo: 3 o 03');
  }

  return valor.padStart(2, '0');
}

function obtenerTablasSae(configSae) {
  const empresa = normalizarEmpresaSae(
    configSae && configSae.empresa,
    configSae && configSae.database
  );

  return {
    empresa,
    tablaClientes: `CLIE${empresa}`
  };
}

async function obtenerLongitudCampo(config, tabla, campo) {
  try {
    const sql = `
      SELECT f.RDB$FIELD_LENGTH AS LENGTH
      FROM RDB$RELATION_FIELDS rf
      JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
      WHERE rf.RDB$RELATION_NAME = ?
        AND rf.RDB$FIELD_NAME = ?
    `;

    const rows = await query(config, sql, [String(tabla).toUpperCase(), String(campo).toUpperCase()]);
    if (!rows || !rows.length) return null;

    const raw = rows[0].LENGTH ?? rows[0]['RDB$FIELD_LENGTH'];
    const length = Number(raw);
    return Number.isFinite(length) ? length : null;
  } catch {
    return null;
  }
}

function obtenerNombreTablaAuxiliar(anioCorto) {
  let valor = String(anioCorto).trim();

  if (/^\d{4}$/.test(valor)) {
    valor = valor.slice(-2);
  }

  if (!/^\d{2}$/.test(valor)) {
    throw new Error('El anio corto debe tener 2 digitos. Ejemplo: 25');
  }

  return `AUXILIAR${valor}`;
}

function normalizarNumeroCliente(numeroCliente) {
  return String(numeroCliente || '').trim();
}

function limitarTexto(valor, maxLength) {
  if (!valor || !maxLength || maxLength <= 0) return String(valor || '');
  const texto = String(valor);
  return texto.length > maxLength ? texto.slice(0, maxLength) : texto;
}

function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

function generarVariantesCliente(numeroCliente, maxLength = 10) {
  const cliente = normalizarNumeroCliente(numeroCliente);
  if (!cliente) return [];

  const clienteSinCeros = cliente.replace(/^0+/, '') || '0';
  const lengths = new Set([cliente.length, 6, 10, maxLength].filter(length => Number(length) > 0));
  const variantes = new Set([cliente, clienteSinCeros]);

  for (const length of lengths) {
    variantes.add(cliente.padStart(length, '0'));
    variantes.add(clienteSinCeros.padStart(length, '0'));
  }

  return [...variantes]
    .map(valor => limitarTexto(valor, maxLength))
    .filter(Boolean);
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

async function buscarClientesSaeEnBloque(configSae, tablaClientes, numerosCliente) {
  if (!numerosCliente.length) {
    return {
      mapaClientes: new Map(),
      variantesPorNumero: new Map()
    };
  }

  const mapaClientes = new Map();
  const variantesPorNumero = new Map();
  const maxLenClave = (await obtenerLongitudCampo(configSae, tablaClientes, 'CLAVE')) || 10;
  const variantesUnicas = new Set();

  for (const numeroCliente of numerosCliente) {
    const variantes = generarVariantesCliente(numeroCliente, maxLenClave);
    variantesPorNumero.set(numeroCliente, variantes);

    for (const variante of variantes) {
      variantesUnicas.add(variante);
    }
  }

  const chunks = chunkArray([...variantesUnicas], 300);

  for (const chunk of chunks) {
    const placeholders = chunk.map(() => '?').join(', ');

    const sql = `
      SELECT
        TRIM(CLAVE) AS CLAVE,
        TRIM(NOMBRE) AS NOMBRE,
        TRIM(CUENTA_CONTABLE) AS CUENTA_CONTABLE
      FROM ${tablaClientes}
      WHERE TRIM(CLAVE) IN (${placeholders})
    `;

    const rows = await query(configSae, sql, chunk);

    for (const row of rows) {
      const key = normalizarNumeroCliente(row.CLAVE);
      if (!mapaClientes.has(key)) {
        mapaClientes.set(key, row);
      }
    }
  }

  return {
    mapaClientes,
    variantesPorNumero
  };
}

async function generarVistaPrevia(configCoi, configSae, anioTabla) {
  const tablaAux = obtenerNombreTablaAuxiliar(anioTabla);
  const { empresa, tablaClientes } = obtenerTablasSae(configSae);
  const maxLenNumCta = (await obtenerLongitudCampo(configCoi, tablaAux, 'NUM_CTA')) || 20;
  const movimientos = await obtenerMovimientosCoi(configCoi, anioTabla);

  console.log(`COI: ${tablaAux}.NUM_CTA max length = ${maxLenNumCta}`);
  console.log(`SAE: empresa seleccionada = ${empresa}`);

  const numerosClienteUnicos = [...new Set(
    movimientos
      .map(mov => normalizarNumeroCliente(mov.NUMERO_CLIENTE))
      .filter(Boolean)
  )];

  let mapaClientes = new Map();
  let variantesPorNumero = new Map();
  let errorClientes = null;

  try {
    const resultadoClientes = await buscarClientesSaeEnBloque(configSae, tablaClientes, numerosClienteUnicos);
    mapaClientes = resultadoClientes.mapaClientes;
    variantesPorNumero = resultadoClientes.variantesPorNumero;
  } catch (error) {
    errorClientes = error.message || String(error);
    console.error('Error buscando clientes SAE:', error);
  }

  const resultados = [];
  const errores = [];

  for (const mov of movimientos) {
    const numeroCliente = normalizarNumeroCliente(mov.NUMERO_CLIENTE);

    if (!numeroCliente) {
      resultados.push({
        ...mov,
        NUMERO_CLIENTE: null,
        CLIENTE_SAE_CLAVE: null,
        CLIENTE_SAE_NOMBRE: null,
        CUENTA_SAE: null,
        CUENTA_FORMATEADA: null,
        ESTATUS: 'No se pudo extraer cliente',
        REQUIERE_ACTUALIZACION: false
      });

      errores.push({
        tipo: 'NO_CLIENTE_EXTRAIDO',
        NUM_POLIZ: mov.NUM_POLIZ,
        PERIODO: mov.PERIODO,
        EJERCICIO: mov.EJERCICIO,
        NUM_PART: mov.NUM_PART,
        CONCEP_PO: mov.CONCEP_PO,
        mensaje: 'No se pudo extraer el numero de cliente del concepto'
      });
      continue;
    }

    if (errorClientes) {
      resultados.push({
        ...mov,
        NUMERO_CLIENTE: numeroCliente,
        CLIENTE_SAE_CLAVE: null,
        CLIENTE_SAE_NOMBRE: null,
        CUENTA_SAE: null,
        CUENTA_FORMATEADA: null,
        ESTATUS: `Error al buscar clientes SAE: ${errorClientes}`,
        REQUIERE_ACTUALIZACION: false
      });
      continue;
    }

    const variantes = variantesPorNumero.get(numeroCliente) || [];
    const cliente = variantes
      .map(variante => mapaClientes.get(variante))
      .find(Boolean);

    if (!cliente) {
      resultados.push({
        ...mov,
        NUMERO_CLIENTE: numeroCliente,
        CLIENTE_SAE_CLAVE: null,
        CLIENTE_SAE_NOMBRE: null,
        CUENTA_SAE: null,
        CUENTA_FORMATEADA: null,
        ESTATUS: 'Cliente no encontrado en SAE',
        REQUIERE_ACTUALIZACION: false
      });

      errores.push({
        tipo: 'CLIENTE_NO_ENCONTRADO',
        NUM_POLIZ: mov.NUM_POLIZ,
        PERIODO: mov.PERIODO,
        EJERCICIO: mov.EJERCICIO,
        NUM_PART: mov.NUM_PART,
        NUMERO_CLIENTE: numeroCliente,
        CVE_CLPV_BUSCADA: variantes.join(', '),
        CONCEP_PO: mov.CONCEP_PO,
        mensaje: `Cliente ${numeroCliente} no encontrado en SAE`
      });
      continue;
    }

    const cuentaFormateada = formatearCuentaContable(cliente.CUENTA_CONTABLE, maxLenNumCta);

    if (!cuentaFormateada) {
      resultados.push({
        ...mov,
        NUMERO_CLIENTE: numeroCliente,
        CLIENTE_SAE_CLAVE: cliente.CLAVE,
        CLIENTE_SAE_NOMBRE: cliente.NOMBRE,
        CUENTA_SAE: cliente.CUENTA_CONTABLE,
        CUENTA_FORMATEADA: null,
        ESTATUS: 'Cuenta SAE invalida',
        REQUIERE_ACTUALIZACION: false
      });

      errores.push({
        tipo: 'CUENTA_INVALIDA',
        NUM_POLIZ: mov.NUM_POLIZ,
        PERIODO: mov.PERIODO,
        EJERCICIO: mov.EJERCICIO,
        NUM_PART: mov.NUM_PART,
        NUMERO_CLIENTE: numeroCliente,
        CUENTA_SAE: cliente.CUENTA_CONTABLE,
        CONCEP_PO: mov.CONCEP_PO,
        mensaje: `Cuenta SAE ${cliente.CUENTA_CONTABLE} no valida para formatear`
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
      ESTATUS: requiereActualizacion ? 'Requiere actualizacion' : 'Correcta',
      REQUIERE_ACTUALIZACION: requiereActualizacion
    });
  }

  return {
    resultados,
    errores
  };
}

async function actualizarRegistros(configCoi, anioTabla, items = []) {
  const tablaAux = obtenerNombreTablaAuxiliar(anioTabla);
  const maxLenNumCta = (await obtenerLongitudCampo(configCoi, tablaAux, 'NUM_CTA')) || 20;

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
    if (!item.REQUIERE_ACTUALIZACION || !item.CUENTA_FORMATEADA) {
      resultados.push({
        ...item,
        UPDATE_RESULT: 'Omitido'
      });
      continue;
    }

    try {
      const cuentaParaActualizar = formatearCuentaContable(item.CUENTA_SAE, maxLenNumCta) ||
        String(item.CUENTA_FORMATEADA || '').slice(0, maxLenNumCta);

      await execute(configCoi, sqlUpdate, [
        cuentaParaActualizar,
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
  generarVistaPrevia,
  actualizarRegistros
};
