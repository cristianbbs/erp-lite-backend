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
   PARSER ROBUSTO
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
    /\b(CONTACTO|GIRO|DIRECCI[ÓO]N|COMUNA|CIUDAD|FECHA\s+EMISI[ÓO]N|MONTO\s+NETO|I\.V\.A\.|TOTAL|REFERENCIAS|FORMA\s+DE\s+PAGO)\b/i;
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
 * Encuentra valor asociado a etiqueta (muy tolerante)
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

  // Caso 3 fallback: chunk cercano
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

// Fallback: detecta un RUT por patrón
function findRutByPattern(text) {
  const t = text || "";
  const m = t.match(/\b\d{1,2}\.?\d{3}\.?\d{3}-[0-9Kk]\b/);
  return m ? cleanValue(m[0].replace(/\./g, "")) : null;
}

/**
 * Extrae un bloque “de ubicación”
 */
function getLocationBlock(text) {
  const t = text || "";
  const start = t.search(/DIRECCI[ÓO]N/i);
  if (start === -1) return t;

  const after = t.slice(start);
  const stop = after.search(
    /\n(Fecha\s+Emisi[óo]n|MONTO\s+NETO|Codigo\s+Descripcion)\b/i
  );
  const block = stop === -1 ? after.slice(0, 450) : after.slice(0, stop);
  return block;
}

/**
 * COMUNA/CIUDAD robusto (corta si viene pegado “CIUDAD:STGO”)
 */
function extractComunaCiudad(text) {
  const block = getLocationBlock(text);

  const cutRe =
    /(?=\b(CONTACTO|GIRO|DIRECCI[ÓO]N|FECHA\s+EMISI[ÓO]N|MONTO\s+NETO|TOTAL|I\.V\.A\.|FORMA\s+DE\s+PAGO)\b)/i;

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
   ITEMS (MUY ROBUSTO)
   - Ancla cantidad/precio/valor desde el FINAL del ítem
   - Evita caer en "20 BOLSAS DE 2 KG" como cantidad/precio
   - cantidad final = SOLO número (sin unidad)
========================= */

/**
 * Une códigos rotos tipo:
 *  - "HP-\n02" -> "HP-02"
 *  - "HP- 02"  -> "HP-02"
 *  - "HP -02"  -> "HP-02"
 *  - "HP - 02" -> "HP-02"
 */
function repairBrokenCodes(s) {
  return (s || "").replace(/([A-Z]{2,5})\s*-\s*(\d{1,5})/g, "$1-$2");
}
function deglueItemTail(line) {
  let s = (line || "").trim();

  // Espacios entre número<->letra (ej: "1.400KG" -> "1.400 KG")
  s = s
    .replace(/(\d)([A-Za-zÁÉÍÓÚÑñ])/g, "$1 $2")
    .replace(/([A-Za-zÁÉÍÓÚÑñ])(\d)/g, "$1 $2")
    .replace(/\s+/g, " ")
    .trim();

  // Helpers
  const stripLeadZeros = (x) => {
    const out = String(x || "").replace(/^0+(?=\d)/, "");
    return out === "" ? "0" : out;
  };

  const numRe = "\\d{1,3}(?:\\.\\d{3})*|\\d+";
  const tailOkRe = new RegExp(
    `(?:^|\\s)(${numRe})\\s*(?:([A-Za-zÁÉÍÓÚÑñ\\.]{1,12})\\s+)?(${numRe})\\s+(${numRe})\\s*$`
  );

  // Si ya está bien separado al final, no tocar
  if (tailOkRe.test(s)) return s;

  // Caso especial: dos montos con miles PEGADOS al final
  // Ej: "... 13.5003.500" -> "... 13.500 3.500"
  // Luego intentamos formar "... <qty> <precio> <valor>"
  const mTwo = s.match(/(\d{1,3}(?:\.\d{3})+)(\d{1,3}(?:\.\d{3})+)\s*$/);
  if (mTwo) {
    const a = mTwo[1]; // ej 13.500
    const b = mTwo[2]; // ej 3.500

    const endLen = (a + b).length;
    const base = s.slice(0, s.length - endLen).trim();

    // Si base ya termina con una cantidad (número), la respetamos.
    // Si no, asumimos cantidad=1 (muy común en "servicios/transporte")
    const hasQty = /(\d{1,3}(?:\.\d{3})*|\d+)\s*$/.test(base);

    let rebuilt;
    
    if (/TRANSPORTE/i.test(base)) {
      // Para transporte, suele ser: qty=1, precio=último monto, valor=último monto
      rebuilt = `${base} 1 ${b} ${b}`;
    } else {
      rebuilt = hasQty
        ? `${base} ${a} ${b}`
        : `${base} 1 ${a} ${b}`;
    }

    // Si con eso ya calza el tail normal, retornamos
    if (tailOkRe.test(rebuilt)) return rebuilt;

    // Si no calza, igual devolvemos separado (ayuda al tailRe afuera)
    return rebuilt;
  }
  
  // Caso: algo termina con ".000" (o ".123") y viene pegado tipo "4045018.000"
  const m = s.match(/(\d+)(\.\d{3})+\s*$/);
  if (!m) return s;

  const dotsPart = m[0].trim(); // ej "4045018.000"
  const idxDots = s.lastIndexOf(dotsPart);
  const beforeAll = s.slice(0, idxDots).trim();

  // Separa digitsBeforeDot y suffix ".000..."
  // Para 4045018.000 -> digitsBeforeDot="4045018", suffix=".000"
  const firstDot = dotsPart.indexOf(".");
  const digitsBeforeDot = dotsPart.slice(0, firstDot);
  const suffix = dotsPart.slice(firstDot); // incluye todos los .xxx

  // Vamos a probar valorFirstLen = 1..3 tomando los ÚLTIMOS dígitos antes del punto:
  // "4045018" -> "8" / "18" / "018"
  // y ver cuál permite separar el resto en qty+price plausible.
  let best = null;

  const gluedBase = digitsBeforeDot; // solo dígitos
  for (let valFirstLen = 1; valFirstLen <= 3; valFirstLen++) {
    if (gluedBase.length < valFirstLen) continue;

    const valFirst = gluedBase.slice(-valFirstLen);
    const glued = gluedBase.slice(0, -valFirstLen); // lo que queda para qty+price

    // Si no queda nada para qty+price, descartar
    if (!glued) continue;

    // Separar glued en [qty][price] probando largos de precio
    for (let priceLen = 2; priceLen <= 6; priceLen++) {
      if (glued.length <= priceLen) continue;

      const priceDigits = glued.slice(-priceLen);
      const qtyDigits = glued.slice(0, -priceLen);

      const qty = parseInt(qtyDigits, 10);
      const price = parseInt(priceDigits, 10);

      if (!Number.isFinite(qty) || !Number.isFinite(price)) continue;

      // Rangos razonables (ajusta si lo necesitas)
      if (qty <= 0 || qty > 100000) continue;
      if (price <= 0 || price > 2000000) continue;

      // Valor reconstruido (quita ceros líderes del primer grupo)
      const valor = `${stripLeadZeros(valFirst)}${suffix}`;

      // Score: preferir qty de 2-4 dígitos y price de 2-5 dígitos
      // (esto hace que 40/450 gane sobre 4/045)
      let score =
        Math.abs(qtyDigits.length - 2) * 10 +
        Math.abs(priceLen - 3) * 5 +
        valFirstLen;
      
      // Penaliza soluciones típicamente malas
      if (qtyDigits.length === 1) score += 40;              // evita 4 en vez de 40, 1 en vez de 100
      if (parseInt(priceDigits, 10) < 100) score += 35;     // evita 45 en vez de 450 (ajusta si tienes precios <100 reales)
      if (/^0\d/.test(valFirst)) score += 25;               // evita 018.000 si existe 18.000
      
      // Premia cantidades que terminan en 0 (muy común en tus docs: 40, 100, 700...)
      if (/0$/.test(qtyDigits)) score -= 10;

      if (!best || score < best.score) {
        best = {
          qtyDigits: stripLeadZeros(qtyDigits),
          priceDigits: stripLeadZeros(priceDigits),
          valor,
          score,
        };
      }
    }
  }

  if (!best) return s;

  const rebuiltTail = `${best.qtyDigits} ${best.priceDigits} ${best.valor}`.trim();
  return `${beforeAll ? beforeAll + " " : ""}${rebuiltTail}`.trim();
}
function parseItems(text) {
  let t = (text || "")
    .replace(/\r/g, "\n")
    .replace(/\u00A0/g, " ")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{2,}/g, "\n");

  // 1) Reparar códigos partidos
  t = repairBrokenCodes(t);

  // Código de producto (sin \b final para permitir pegado)
  const codeReGlobal = /\b([A-Z]{2,5}-\d{1,5})/g;
  const codeReOne = /\b([A-Z]{2,5}-\d{1,5})/;

  // Cortes típicos donde ya NO hay ítems
  const stopRe = /\b(Referencias:|Forma de Pago:|MONTO NETO|I\.V\.A\.|TOTAL)\b/i;

  const firstCode = t.search(codeReOne);
  if (firstCode === -1) return [];

  let working = t.slice(firstCode);
  const stop = working.search(stopRe);
  if (stop !== -1) working = working.slice(0, stop);

  const matches = [...working.matchAll(codeReGlobal)];
  if (!matches.length) return [];

  const cleanDesc = (s) =>
    (s || "")
      .replace(/\s+/g, " ")
      .trim();

  // Números tipo Chile: 40, 450, 18.000, 1.400, etc.
  const numRe = "\\d{1,3}(?:\\.\\d{3})*|\\d+";
  const unitRe = "[A-Za-zÁÉÍÓÚÑñ\\.]{1,12}";

  // ANCLA DESDE EL FINAL:
  // ... <cantidad> <unidad opcional> <precio> <valor> (al final del ítem)
  //
  // Ej: "40 450 18.000"
  // Ej: "1.400 KG 285 399.000"
  const tailRe = new RegExp(
    `(?:^|\\s)(${numRe})\\s*(?:(${unitRe})\\s+)?(${numRe})\\s+(${numRe})\\s*$`
  );
  // Fallback: ignora 0–2 "basuras" entre precio y valor (0, 0%, -, —, etc.)
  const tailReSkip = new RegExp(
    `(?:^|\\s)(${numRe})\\s*(?:(${unitRe})\\s+)?(${numRe})` +
      `(?:\\s+(?:0(?:[\\.,]00)?|0%|[-—])){0,2}` +
      `\\s+(${numRe})\\s*$`
  );
  // Ruido típico que NO debería quedar en descripción (si se cuela)
  const noiseRe = /\b(LOTE|DESDE|HASTA)\s*:?\s*.*$/i;

  const items = [];

  for (let i = 0; i < matches.length; i++) {
    const code = matches[i][1];
    const start = matches[i].index ?? 0;
    const end =
      i + 1 < matches.length
        ? matches[i + 1].index ?? working.length
        : working.length;

    let chunk = working.slice(start, end);

    // normaliza chunk
    chunk = repairBrokenCodes(chunk);
    chunk = chunk.replace(/\n/g, " ").replace(/\s+/g, " ").trim();
    
    // FIX: despega el tail cuando viene aplastado (ej 4045018.000)
    chunk = deglueItemTail(chunk);

    // Quita el código al inicio
    const codeEsc = code.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    chunk = chunk.replace(new RegExp("^" + codeEsc + "\\s*"), "").trim();

    // 2) Extraer tail (cantidad/precio/valor) desde el FINAL del chunk
    const mTail = chunk.match(tailRe) || chunk.match(tailReSkip);

    if (!mTail) {
      const descripcion = cleanDesc(chunk.replace(noiseRe, "").trim()) || null;
      if (descripcion) {
        items.push({
          codigo: code,
          descripcion,
          cantidad: null,
          precio: null,
          valor: null,
        });
      }
      continue;
    }

    const qtyRaw = mTail[1];      // ej: "40" o "1.400"
    const unit = mTail[2] || "";  // ej: "KG" (opcional)
    const precioRaw = mTail[3];   // ej: "450" o "285"
    const valorRaw = mTail[4];    // ej: "18.000" o "399.000"

    const stripLeadZeros = (x) => String(x || "").replace(/^0+(?=\d)/, "");
    
    const cantidad = stripLeadZeros(qtyRaw);
    const precio = stripLeadZeros(precioRaw);
    
    // valor: elimina ceros a la izquierda solo del primer grupo (018.000 -> 18.000)
    const valor = String(valorRaw || "").replace(/^0+(?=\d)/, "");

    // 3) Descripción = todo lo anterior al tail
    const tailFull = mTail[0]; // incluye espacios
    let descPart = chunk.slice(0, chunk.length - tailFull.length).trim();

    // limpieza de ruido típico en descripción
    descPart = descPart.replace(noiseRe, "").trim();

    const descripcion = cleanDesc(descPart) || null;

    items.push({
      codigo: code,
      descripcion,
      cantidad,
      precio,
      valor,
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

    return res.json({
      ok: true,
      fields,
      preview: text.slice(0, 2500),
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








