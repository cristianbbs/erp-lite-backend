function normalizeText(t) {
  return (t || "")
    .replace(/\r/g, "\n")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{2,}/g, "\n")
    .trim();
}

/**
 * Muchos PDFs vienen con 2 páginas (y la 2 repite todo).
 * Como tú dijiste "leer solo hoja 1", hacemos un corte simple:
 * si encontramos una 2da ocurrencia de "SEÑOR(ES):", cortamos ahí.
 */
function keepFirstPageOnly(text) {
  const key = "SEÑOR(ES):";
  const first = text.indexOf(key);
  if (first === -1) return text;

  const second = text.indexOf(key, first + key.length);
  if (second === -1) return text;

  return text.slice(0, second).trim();
}

function pickAfterLabel(text, label) {
  // Ej: label="R.U.T.:" => captura hasta fin de línea
  const re = new RegExp(label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "\\s*(.+)", "i");
  const m = text.match(re);
  return m ? m[1].trim() : null;
}

function pickDocType(text) {
  // En tu ejemplo aparece como línea completa "FACTURA ELECTRONICA"
  const m = text.match(/\b(FACTURA ELECTRONICA|NOTA DE CREDITO|NOTA DE D[ÉE]BITO|GUIA DE DESPACHO(?: ELECTR[ÓO]NICA)?)\b/i);
  return m ? m[1].trim() : null;
}

function pickDocNumber(text) {
  // "Nº10245" o "N° 10245"
  const m = text.match(/\bN[º°]\s*([0-9]{1,10})\b/i);
  return m ? m[1] : null;
}

function pickFechaEmision(text) {
  // "Fecha Emision: 23 de Febrero del 2026"
  const m = text.match(/Fecha\s+Emision:\s*(.+)/i);
  return m ? m[1].trim() : null;
}

function parseMoneyLine(text, label) {
  // "MONTO NETO $ 399.000"
  const re = new RegExp(label + "\\s*\\$\\s*([\\d\\.]+)", "i");
  const m = text.match(re);
  return m ? m[1] : null;
}

/**
 * Parse ítems:
 * - Encuentra el bloque entre el header "Codigo Descripcion..." y "Referencias:"
 * - Cada ítem comienza con un "Codigo" tipo HP-02
 * - La última línea del ítem suele ser: "<cantidad> <unidad> <precio> <valor>"
 */
function parseItems(text) {
  const start = text.search(/Codigo\s+Descripcion/i);
  if (start === -1) return [];

  const afterStart = text.slice(start);
  const endIdx = afterStart.search(/\nReferencias:\s*/i);
  const block = endIdx === -1 ? afterStart : afterStart.slice(0, endIdx);

  const lines = block.split("\n").map(l => l.trim()).filter(Boolean);

  // Quita las 1-2 líneas de encabezado
  // (en tu ejemplo: "Codigo Descripcion..." y "Adic.* %Desc. Valor")
  const cleaned = lines.filter(l => !/^Codigo\s+Descripcion/i.test(l) && !/^Adic\.\*/i.test(l));

  const items = [];
  let current = null;

  const isCodeLine = (l) => /^[A-Z0-9]{1,6}-[A-Z0-9]{1,10}\b/.test(l); // HP-02
  const qtyPriceValue = (l) => {
    // Ej: "1.400 KG 285 399.000"
    const m = l.match(/^([\d\.]+)\s+([A-Za-zÁÉÍÓÚÑñ\.]+)\s+(\d+)\s+([\d\.]+)$/);
    if (!m) return null;
    return { cantidad: `${m[1]} ${m[2]}`, precio: m[3], valor: m[4] };
  };

  for (const line of cleaned) {
    if (isCodeLine(line)) {
      // Cierra el anterior si quedó abierto
      if (current) items.push(current);
      const [codigo, ...rest] = line.split(" ");
      current = {
        codigo,
        descripcion: rest.join(" ").trim(),
        cantidad: null,
        precio: null,
        valor: null,
      };
      continue;
    }

    if (!current) continue;

    const qpv = qtyPriceValue(line);
    if (qpv) {
      current.cantidad = qpv.cantidad;
      current.precio = qpv.precio;
      current.valor = qpv.valor;
    } else {
      // parte de descripción multi-línea (incluye LOTE, DESDE, HASTA, etc.)
      current.descripcion = (current.descripcion ? current.descripcion + "\n" : "") + line;
    }
  }

  if (current) items.push(current);
  return items;
}

function parseReferences(text) {
  const idx = text.search(/\nReferencias:\s*/i);
  if (idx === -1) return [];

  const after = text.slice(idx);
  // Corta al llegar a "Forma de Pago" o "MONTO NETO"
  const end = after.search(/\n(Forma de Pago:|MONTO NETO)/i);
  const block = end === -1 ? after : after.slice(0, end);

  return block
    .split("\n")
    .map(l => l.trim())
    .filter(l => l.startsWith("-"))
    .map(l => l.replace(/^-+\s*/, ""));
}

function extractFacturaKolderStyle(fullText) {
  const text = keepFirstPageOnly(normalizeText(fullText));

  // Encabezado
  const tipo_documento = pickDocType(text);
  const numero_documento = pickDocNumber(text);

  const razon_social = pickAfterLabel(text, "SEÑOR\\(ES\\):");
  const rut = pickAfterLabel(text, "R\\.U\\.T\\.:");
  const giro = pickAfterLabel(text, "GIRO:");
  const direccion = pickAfterLabel(text, "DIRECCION:");
  const contacto = pickAfterLabel(text, "CONTACTO:");
  const fecha_emision = pickFechaEmision(text);

  // COMUNA y CIUDAD vienen en la misma línea: "COMUNA QUILICURA CIUDAD: SANTIAGO"
  let comuna = null, ciudad = null;
  const mLoc = text.match(/COMUNA\s+(.+?)\s+CIUDAD:\s*(.+)/i);
  if (mLoc) {
    comuna = mLoc[1].trim();
    ciudad = mLoc[2].trim();
  }

  // Ítems + referencias
  const items = parseItems(text);
  const referencias = parseReferences(text);

  // Montos
  const monto_neto = parseMoneyLine(text, "MONTO NETO");
  const iva_19 = parseMoneyLine(text, "I\\.V\\.A\\.\\s*19%");
  const total = parseMoneyLine(text, "TOTAL");

  return {
    tipo_documento,
    numero_documento,
    razon_social,
    rut,
    giro,
    direccion,
    comuna,
    ciudad,
    contacto,
    fecha_emision,
    items,
    referencias,
    monto_neto,
    iva_19,
    total,
  };
}