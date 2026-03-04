import express from "express";
import cors from "cors";
import multer from "multer";
import pdf from "pdf-parse";

const app = express();

// CORS: permite tu web
app.use(cors({
  origin: ["https://hielokolder.cl", "https://www.hielokolder.cl"],
  methods: ["GET", "POST", "OPTIONS"],
}));
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
    .replace(/\u00A0/g, " ")           // no-break space
    .replace(/[ \t]+/g, " ")
    .replace(/\n{2,}/g, "\n")
    .trim();
}

function escRe(s) {
  return (s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function cleanValue(v) {
  if (v == null) return null;
  return String(v)
    .replace(/^[\s.:;-]+/, "")      // quita :, ., ;, - al inicio
    .replace(/[\s.:;-]+$/, "")      // quita al final
    .replace(/\s{2,}/g, " ")
    .trim() || null;
}

// Si el valor trae etiquetas pegadas, cortamos al empezar una etiqueta conocida
function stripTrailingLabels(value) {
  if (!value) return value;

  const stopRe = /\b(CONTACTO|GIRO|DIRECCI[ÓO]N|COMUNA|CIUDAD|FECHA\s+EMISI[ÓO]N|MONTO\s+NETO|I\.V\.A\.|TOTAL|REFERENCIAS)\b/i;
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
    const reNextLine = new RegExp(`(?:^|\\n)\\s*${L}\\s*:??\\s*\\n\\s*([^\\n]+)`, "i");
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
      const chunk = t.slice(idx, idx + 180);
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
    .map(l => l.trim())
    .filter(l => l.startsWith("-"))
    .map(l => l.replace(/^-+\s*/, ""));
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
  const block = stop === -1 ? after.slice(0, 350) : after.slice(0, stop);
  return block;
}

/**
 * COMUNA/CIUDAD robusto (corta si viene pegado “CIUDAD:STGO”)
 */
function extractComunaCiudad(text) {
  const block = getLocationBlock(text);

  const cutRe = /(?=\b(CONTACTO|GIRO|DIRECCI[ÓO]N|FECHA\s+EMISI[ÓO]N|MONTO\s+NETO|TOTAL|I\.V\.A\.)\b)/i;

  let comuna = null;
  {
    const m = block.match(/COMUNA\s*:?\s*([\s\S]{0,120})/i);
    if (m && m[1]) {
      let v = m[1].split(cutRe)[0];
      v = v.split(/CIUDAD\s*:?/i)[0]; // <-- CLAVE: corta aunque venga pegado
      comuna = cleanValue(v);
    }
  }

  let ciudad = null;
  {
    const m = block.match(/CIUDAD\s*:?\s*([\s\S]{0,120})/i);
    if (m && m[1]) {
      let v = m[1].split(cutRe)[0];
      v = v.split(/\bCONTACTO\b/i)[0];
      ciudad = cleanValue(v);
    }
  }

  return { comuna, ciudad };
}

/* =========================
   ITEMS (NUEVO ROBUSTO)
   Soporta:
   - HC-101 ... 40 450 18.000
   - HP-02 ... 1.400 KG 285 399.000
   - Descripción en varias líneas
========================= */

function parseItems(text) {
  const t = (text || "")
    .replace(/\r/g, "\n")
    .replace(/\u00A0/g, " ")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{2,}/g, "\n");

  // Código tipo HC-101 / HP-02 / etc.
  const codeRe = /\b([A-Z0-9]{1,8}-[A-Z0-9]{1,8})\b/g;

  // Cortes típicos donde ya NO hay ítems
  const stopRe = /\b(Referencias:|MONTO NETO|I\.V\.A\.|TOTAL)\b/i;

  // Toma solo desde el primer código hasta antes de montos/referencias (si existe)
  let working = t;
  const firstCode = working.search(codeRe);
  if (firstCode === -1) return [];

  working = working.slice(firstCode);
  const stop = working.search(stopRe);
  if (stop !== -1) working = working.slice(0, stop);

  // Encuentra todas las posiciones de códigos
  const matches = [...working.matchAll(codeRe)];
  if (!matches.length) return [];

  // Helpers
  const cleanDesc = (s) =>
    (s || "")
      .replace(/\s+/g, " ")
      .trim();

  const isMoneyish = (s) => /^[0-9][0-9\.]*$/.test(s);      // 18.000 / 399.000 / 450 / 620
  const isQty = (s) => /^[0-9][0-9\.]*$/.test(s);          // 40 / 1.400
  const isUnit = (s) => /^[A-Za-zÁÉÍÓÚÑñ\.]{1,10}$/.test(s); // KG / UN / etc.

  const items = [];

  for (let i = 0; i < matches.length; i++) {
    const code = matches[i][1];
    const start = matches[i].index ?? 0;
    const end = (i + 1 < matches.length) ? (matches[i + 1].index ?? working.length) : working.length;

    // Bloque del ítem (texto entre este código y el siguiente código)
    let chunk = working.slice(start, end);

    // Normaliza el chunk
    chunk = chunk.replace(/\n/g, " ").replace(/\s+/g, " ").trim();

    // Quita el código al inicio para quedarnos con el resto
    chunk = chunk.replace(new RegExp("^" + code.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "\\s*"), "");

    // Tokeniza
    const tokens = chunk.split(" ").filter(Boolean);

    // Heurística:
    // Al final suelen venir: cantidad [unidad] precio valor
    // - Caso A: "40 450 18.000"
    // - Caso B: "1.400 KG 285 399.000"
    // - Algunas facturas meten "Adic." "%Desc." en medio, pero al final igual está el valor.
    //
    // Estrategia: desde el final, busca los últimos 2 "moneyish" como precio y valor,
    // y antes de eso busca cantidad + unidad opcional.

    let valor = null;
    let precio = null;
    let cantidad = null;

    // 1) Encuentra desde el final los 2 últimos números tipo dinero
    let idxValor = -1;
    let idxPrecio = -1;

    for (let k = tokens.length - 1; k >= 0; k--) {
      if (isMoneyish(tokens[k])) {
        if (idxValor === -1) idxValor = k;
        else {
          idxPrecio = k;
          break;
        }
      }
    }

    if (idxValor !== -1) valor = tokens[idxValor];
    if (idxPrecio !== -1) precio = tokens[idxPrecio];

    // 2) Cantidad: busca hacia atrás antes de idxPrecio (o idxValor si no hay precio)
    const limit = (idxPrecio !== -1 ? idxPrecio : idxValor);
    if (limit > 0) {
      // patrones:
      // - qty unit (ej: 1.400 KG)
      // - qty (ej: 40)
      // Tomamos el último qty antes del precio/valor
      for (let k = limit - 1; k >= 0; k--) {
        if (isQty(tokens[k])) {
          const maybeUnit = tokens[k + 1];
          if (maybeUnit && isUnit(maybeUnit) && (k + 1) < limit) {
            cantidad = `${tokens[k]} ${maybeUnit}`;
          } else {
            cantidad = tokens[k];
          }
          break;
        }
      }
    }

    // 3) Descripción = todo lo anterior a la cantidad (si encontramos cantidad)
    let descTokens = tokens;
    if (cantidad) {
      // si cantidad es "1.400 KG" ocupa 2 tokens; si es "40" 1 token
      const qtyParts = cantidad.split(" ");
      // busca dónde aparece la cantidad en tokens para cortar descripción antes
      let cutIdx = -1;
      for (let k = 0; k < tokens.length; k++) {
        if (tokens[k] === qtyParts[0]) {
          if (qtyParts.length === 2) {
            if (tokens[k + 1] === qtyParts[1]) cutIdx = k;
          } else {
            cutIdx = k;
          }
        }
        if (cutIdx !== -1) break;
      }
      if (cutIdx !== -1) descTokens = tokens.slice(0, cutIdx);
    } else if (idxPrecio !== -1) {
      // fallback: si hay precio, desc antes del precio
      descTokens = tokens.slice(0, idxPrecio);
    } else if (idxValor !== -1) {
      // fallback: desc antes del valor
      descTokens = tokens.slice(0, idxValor);
    }

    const descripcion = cleanDesc(descTokens.join(" "));

    // Si no hay nada útil, lo saltamos
    if (!descripcion && !cantidad && !precio && !valor) continue;

    items.push({
      codigo: code,
      descripcion: descripcion || null,
      cantidad: cantidad || null,
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

  const razon_social = pickAfterLabel(text, ["SEÑOR(ES)", "SEÑOR (ES)", "SEÑOR(ES):", "SEÑOR (ES):"]);

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

