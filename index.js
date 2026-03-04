import express from "express";
import cors from "cors";
import multer from "multer";
import pdf from "pdf-parse";

const app = express();

// CORS: permite tu web
app.use(
  cors({
    origin: ["https://hielokolder.cl", "https://www.hielokolder.cl"],
    methods: ["GET", "POST", "OPTIONS"],
  })
);
app.use(express.json());

// Multer en memoria (NO guarda PDF en disco)
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 }, // 15MB
  fileFilter: (req, file, cb) => {
    const ok = file.mimetype === "application/pdf";
    cb(ok ? null : new Error("Solo se permiten PDFs"), ok);
  },
});

/* =========================
   UTILIDADES
========================= */

function normalizeText(t) {
  return (t || "")
    .replace(/\r/g, "\n")
    .replace(/\u00A0/g, " ") // no-break space
    .replace(/[ \t]+/g, " ")
    .replace(/\n{2,}/g, "\n")
    .trim();
}

function escRe(s) {
  return (s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function cleanValue(v) {
  if (v == null) return null;
  return (
    String(v)
      // quita cosas raras al inicio/fin
      .replace(/^[\s.:;-]+/, "")
      .replace(/[\s.:;-]+$/, "")
      .replace(/\s{2,}/g, " ")
      .trim() || null
  );
}

// Si el valor trae etiquetas pegadas, cortamos al empezar una etiqueta conocida
function stripTrailingLabels(value) {
  if (!value) return value;
  const stopRe =
    /\b(CONTACTO|GIRO|DIRECCI[ÓO]N|COMUNA|CIUDAD|FECHA\s+EMISI[ÓO]N|MONTO\s+NETO|I\.V\.A\.|TOTAL|REFERENCIAS|CODIGO|DESCRIPCION)\b/i;
  const idx = value.search(stopRe);
  if (idx > 0) return value.slice(0, idx).trim();
  return value.trim();
}

// Toma primera página si el PDF repite encabezado
function keepFirstPageOnly(text) {
  const t = text || "";
  const re = /SEÑOR\s*\(ES\)\s*:?\s*/gi;
  const matches = [...t.matchAll(re)];
  if (matches.length <= 1) return t.trim();
  const secondIdx = matches[1].index ?? -1;
  if (secondIdx === -1) return t.trim();
  return t.slice(0, secondIdx).trim();
}

/**
 * Encuentra valor asociado a etiqueta (tolerante)
 * - ":" opcional
 * - valor misma línea o siguiente
 * - limpia ":" y corta si vienen etiquetas pegadas
 */
function pickAfterLabel(text, labelVariants) {
  const t = text || "";

  for (const label of labelVariants) {
    const L = escRe(label);

    // Caso 1: misma línea
    const reSameLine = new RegExp(`(?:^|\\n)\\s*${L}\\s*:??\\s*(.+)`, "i");
    const m1 = t.match(reSameLine);
    if (m1 && m1[1]) {
      let v = cleanValue(m1[1]);
      v = stripTrailingLabels(v);
      if (v && v !== "-" && v !== "—") return v;
    }

    // Caso 2: línea siguiente
    const reNextLine = new RegExp(
      `(?:^|\\n)\\s*${L}\\s*:??\\s*\\n\\s*([^\\n]+)`,
      "i"
    );
    const m2 = t.match(reNextLine);
    if (m2 && m2[1]) {
      let v = cleanValue(m2[1]);
      v = stripTrailingLabels(v);
      if (v && v !== "-" && v !== "—") return v;
    }
  }

  // Caso 3 fallback: chunk cercano (por si el PDF mete saltos raros)
  for (const label of labelVariants) {
    const idx = t.toUpperCase().indexOf(label.toUpperCase());
    if (idx !== -1) {
      const chunk = t.slice(idx, idx + 220);
      const after = chunk.split("\n").slice(1).join("\n").trim();
      if (after) {
        let firstLine = cleanValue(after.split("\n")[0]);
        firstLine = stripTrailingLabels(firstLine);
        if (firstLine && firstLine !== "-" && firstLine !== "—") return firstLine;
      }
    }
  }

  return null;
}

function pickDocType(text) {
  const m = (text || "").match(
    /\b(FACTURA ELECTRONICA|NOTA DE CREDITO|NOTA DE D[ÉE]BITO|GUIA DE DESPACHO(?: ELECTR[ÓO]NICA)?)\b/i
  );
  return m ? m[1].trim() : null;
}

function pickDocNumber(text) {
  const m = (text || "").match(/\bN[º°]\s*([0-9]{1,10})\b/i);
  return m ? m[1] : null;
}

function pickFechaEmision(text) {
  const m = (text || "").match(/Fecha\s+Emisi[óo]n\s*:?\s*(.+)/i);
  return m ? cleanValue(m[1]) : null;
}

function parseMoneyLine(text, labelRegex) {
  const re = new RegExp(labelRegex + "\\s*\\$\\s*([\\d\\.]+)", "i");
  const m = (text || "").match(re);
  return m ? m[1] : null;
}

function parseReferences(text) {
  const t = text || "";
  const idx = t.search(/\nReferencias:\s*/i);
  if (idx === -1) return [];

  const after = t.slice(idx);
  const end = after.search(/\n(Forma de Pago:|MONTO NETO)/i);
  const block = end === -1 ? after : after.slice(0, end);

  return block
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l.startsWith("-"))
    .map((l) => l.replace(/^-+\s*/, ""));
}

// Fallback: detecta un RUT por patrón (12.345.678-9 o 12345678-9 o con K)
function findRutByPattern(text) {
  const t = text || "";
  const m = t.match(/\b\d{1,2}\.?\d{3}\.?\d{3}-[0-9Kk]\b/);
  return m ? cleanValue(m[0].replace(/\./g, "")) : null;
}

/**
 * Extrae un bloque de texto “de ubicación”
 */
function getLocationBlock(text) {
  const t = text || "";
  const start = t.search(/DIRECCI[ÓO]N/i);
  if (start === -1) return t;

  const after = t.slice(start);
  const stop = after.search(/\n(Fecha\s+Emisi[óo]n|MONTO\s+NETO|Codigo\s+Descripcion)\b/i);
  const block = stop === -1 ? after.slice(0, 450) : after.slice(0, stop);
  return block;
}

/**
 * COMUNA/CIUDAD robusto (corta si viene pegado “CIUDAD:STGO”)
 */
function extractComunaCiudad(text) {
  const block = getLocationBlock(text);

  const cutRe =
    /(?=\b(CONTACTO|GIRO|DIRECCI[ÓO]N|FECHA\s+EMISI[ÓO]N|MONTO\s+NETO|TOTAL|I\.V\.A\.|CODIGO|DESCRIPCION)\b)/i;

  let comuna = null;
  {
    const m = block.match(/COMUNA\s*:?\s*([\s\S]{0,160})/i);
    if (m && m[1]) {
      let v = m[1].split(cutRe)[0];
      v = v.split(/CIUDAD\s*:?/i)[0]; // corta aunque venga pegado
      comuna = cleanValue(v);
    }
  }

  let ciudad = null;
  {
    const m = block.match(/CIUDAD\s*:?\s*([\s\S]{0,160})/i);
    if (m && m[1]) {
      let v = m[1].split(cutRe)[0];
      v = v.split(/\bCONTACTO\b/i)[0];
      ciudad = cleanValue(v);
    }
  }

  return { comuna, ciudad };
}

/* =========================
   ITEMS (ROBUSTO POR "FILA")
   - SOLO usa la línea donde aparece el código (HC-101 / HP-02 / etc.)
   - Repara números pegados tipo "4045018.000" => 40 / 450 / 18.000
   - cantidad => SOLO número (string), unidad aparte
========================= */

function isThousandsMoney(s) {
  // 18.000 / 399.000 / 1.234.567
  return /^[0-9]{1,3}(?:\.[0-9]{3})+$/.test(s);
}
function isPlainNumber(s) {
  // 450 / 285 / 620 / 40 / 100 / 1400
  return /^[0-9]+$/.test(s);
}
function isQtyWithDots(s) {
  // 1.400
  return /^[0-9]{1,3}(?:\.[0-9]{3})+$/.test(s) || /^[0-9]+$/.test(s);
}

function splitPackedQtyPriceValue(token) {
  // Caso típico: 4045018.000  => qty=40, price=450, value=18.000
  //            10062062.000 => qty=100, price=620, value=62.000
  // Heurística: termina con NNN.NNN (o N.NNN etc), antes vienen qty+price pegados
  const m = token.match(/^(\d+)(\d{2,4})(\d{1,3}(?:\.\d{3})+)$/);
  if (!m) return null;
  const qty = m[1];
  const price = m[2];
  const value = m[3];
  return { qty, unit: null, price, value };
}

function parseRowTail(line) {
  const raw = line.trim();

  // 1) Caso con espacios y unidad opcional al medio:
  // ... 1.400 KG 285 399.000
  // ... 40 450 18.000
  // ... 100 620 62.000
  let m = raw.match(
    /(\d[\d\.]*)\s+([A-Za-zÁÉÍÓÚÑñ\.]{1,10})\s+(\d[\d\.]*)\s+(\d[\d\.]*)\s*$/
  );
  if (m) {
    const qtyRaw = m[1];
    const unitRaw = m[2];
    const priceRaw = m[3];
    const valueRaw = m[4];

    // validación mínima
    if (isQtyWithDots(qtyRaw) && isPlainNumber(priceRaw.replace(/\./g, "")) && /[0-9]/.test(valueRaw)) {
      return {
        qty: qtyRaw,
        unit: unitRaw,
        price: priceRaw,
        value: valueRaw,
        cutLen: m[0].length,
      };
    }
  }

  // 2) Caso sin unidad:
  m = raw.match(/(\d[\d\.]*)\s+(\d[\d\.]*)\s+(\d[\d\.]*)\s*$/);
  if (m) {
    const qtyRaw = m[1];
    const priceRaw = m[2];
    const valueRaw = m[3];
    // ojo: si value no parece dinero y qty/price/value se ven raros, aún lo aceptamos,
    // pero esto suele funcionar bien.
    if (isQtyWithDots(qtyRaw)) {
      return {
        qty: qtyRaw,
        unit: null,
        price: priceRaw,
        value: valueRaw,
        cutLen: m[0].length,
      };
    }
  }

  // 3) Caso números pegados: "4045018.000"
  const lastToken = raw.split(/\s+/).slice(-1)[0];
  const packed = splitPackedQtyPriceValue(lastToken);
  if (packed) {
    return {
      qty: packed.qty,
      unit: null,
      price: packed.price,
      value: packed.value,
      cutLen: lastToken.length,
      packedOnlyLastToken: true,
    };
  }

  // 4) Caso extremo: "1.400KG285399.000" pegado (sin espacios)
  // qty (con puntos) + unidad + precio + valor(con puntos)
  const m2 = raw.match(/^(.+)\s+([0-9]{1,3}(?:\.[0-9]{3})+)([A-Za-z]{1,6})(\d{2,5})(\d{1,3}(?:\.\d{3})+)\s*$/);
  if (m2) {
    return {
      qty: m2[2],
      unit: m2[3],
      price: m2[4],
      value: m2[5],
      cutLen: (m2[2] + m2[3] + m2[4] + m2[5]).length,
    };
  }

  return null;
}

function normalizeQtyToNumberString(qtyRaw) {
  if (!qtyRaw) return null;
  // "1.400" => "1400"
  return String(qtyRaw).replace(/\./g, "");
}

function parseItems(text) {
  const t = normalizeText(text || "");

  // Tomamos una ventana “solo ítems” para evitar mezclar con referencias/montos
  const stopRe = /\b(Referencias:|MONTO NETO|I\.V\.A\.|TOTAL)\b/i;

  // Identifica comienzo por header o por primer código
  const firstCodeIdx = t.search(/\b[A-Z]{2}-\d{1,4}\b/);
  if (firstCodeIdx === -1) return [];

  let working = t.slice(firstCodeIdx);
  const stop = working.search(stopRe);
  if (stop !== -1) working = working.slice(0, stop);

  const lines = working
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);

  const items = [];

  // Solo líneas que PARTEN con código
  const codeLineRe = /^([A-Z]{2}-\d{1,4})\b\s*(.*)$/;

  for (const line of lines) {
    const m = line.match(codeLineRe);
    if (!m) continue;

    const codigo = m[1];
    let rest = (m[2] || "").trim();

    // Si esta “línea de código” es en realidad algo que no es ítem (por PDF raro), skip
    if (!rest) continue;

    // Parse cola (cantidad/precio/valor) SOLO desde esta misma línea
    const tail = parseRowTail(line);

    let cantidad = null;
    let unidad = null;
    let precio = null;
    let valor = null;

    let descripcion = rest;

    if (tail) {
      cantidad = normalizeQtyToNumberString(tail.qty);
      unidad = tail.unit ? cleanValue(tail.unit) : null;

      // precio: normalmente viene sin puntos (450/620/285). Si viene con puntos, los dejamos.
      precio = tail.price ? cleanValue(tail.price) : null;
      valor = tail.value ? cleanValue(tail.value) : null;

      // Quita del final para que la descripción no incluya números
      // (más seguro: recortar por match de tail sobre la línea original)
      // Aquí recortamos en base a "rest" para no depender de espacios exactos del inicio.
      // Buscamos el primer token de qty dentro de rest y cortamos ahí.
      const qtyToken = String(tail.qty);
      const idx = rest.lastIndexOf(qtyToken);
      if (idx > 0) {
        descripcion = rest.slice(0, idx).trim();
      }
    } else {
      // Sin cola: al menos devolvemos el código + descripción
      descripcion = rest.trim();
    }

    descripcion = cleanValue(descripcion);

    // filtros anti-basura: si “descripcion” quedó como algo muy corto y sin números, igual puede servir
    // pero evitamos cosas como "Referencias:" etc.
    if (descripcion && /\b(Referencias:|Forma de Pago:|Timbre Electr[oó]nico|SII)\b/i.test(descripcion)) {
      continue;
    }

    items.push({
      codigo,
      descripcion: descripcion || null,
      cantidad: cantidad || null, // SOLO número (string)
      unidad: unidad || null,     // extra (opcional)
      precio: precio || null,
      valor: valor || null,
    });
  }

  return items;
}

/* =========================
   EXTRACTOR PRINCIPAL
========================= */

function extractFacturaKolderStyle(fullText) {
  const text = keepFirstPageOnly(normalizeText(fullText));

  const tipo_documento = pickDocType(text);
  const numero_documento = pickDocNumber(text);

  const razon_social = pickAfterLabel(text, [
    "SEÑOR(ES)",
    "SEÑOR (ES)",
    "SEÑOR(ES):",
    "SEÑOR (ES):",
  ]);

  let rut = pickAfterLabel(text, ["RUT", "RUT:", "R.U.T", "R.U.T.", "R.U.T:", "R.U.T.:"]);
  if (!rut) rut = findRutByPattern(text);

  const giro = pickAfterLabel(text, ["GIRO", "GIRO:"]);
  const direccion = pickAfterLabel(text, ["DIRECCION", "DIRECCIÓN", "DIRECCION:", "DIRECCIÓN:"]);
  const contacto = pickAfterLabel(text, ["CONTACTO", "CONTACTO:"]);
  const fecha_emision = pickFechaEmision(text);

  const { comuna, ciudad } = extractComunaCiudad(text);

  const items = parseItems(text);
  const referencias = parseReferences(text);

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

/* =========================
   ENDPOINTS
========================= */

app.get("/health", (req, res) => res.json({ ok: true }));

app.post("/api/upload-pdf", upload.single("pdf"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Falta archivo PDF" });

    const parsed = await pdf(req.file.buffer);
    const text = normalizeText(parsed.text);

    if (!text || text.length < 30) {
      return res.status(422).json({
        ok: false,
        error: "No se pudo leer texto suficiente. ¿Seguro que es PDF nativo?",
      });
    }

    const fields = extractFacturaKolderStyle(text);
    const preview = text.slice(0, 2500);

    return res.json({
      ok: true,
      fields,
      preview,
      meta: { pages: parsed.numpages || null },
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error procesando PDF" });
  }
});

// Render: usar el puerto que te asigna la plataforma
const PORT = process.env.PORT || 5050;
app.listen(PORT, "0.0.0.0", () => console.log(`Server running on port ${PORT}`));
