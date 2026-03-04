import express from "express";
import cors from "cors";
import multer from "multer";
import pdf from "pdf-parse";

const app = express();

// CORS: permite tu web
app.use(cors({
  origin: ["https://hielokolder.cl", "https://www.hielokolder.cl"],
  methods: ["GET", "POST"],
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
   PARSER ROBUSTO (FIX COMUNA/CIUDAD/CONTACTO)
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

  // etiquetas típicas que se “pegan” en el texto extraído
  const stopRe = /\b(CONTACTO|GIRO|DIRECCI[ÓO]N|COMUNA|CIUDAD|FECHA\s+EMISI[ÓO]N|MONTO\s+NETO|I\.V\.A\.|TOTAL|REFERENCIAS)\b/i;
  const m = value.match(stopRe);
  if (!m) return value;

  // si aparece una etiqueta a mitad del string, cortamos ahí
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

function parseItems(text) {
  const t = text || "";
  const start = t.search(/Codigo\s+Descripcion/i);
  if (start === -1) return [];

  const afterStart = t.slice(start);
  const endIdx = afterStart.search(/\nReferencias:\s*/i);
  const block = endIdx === -1 ? afterStart : afterStart.slice(0, endIdx);

  const lines = block.split("\n").map(l => l.trim()).filter(Boolean);
  const cleaned = lines.filter(
    l => !/^Codigo\s+Descripcion/i.test(l) && !/^Adic\.\*/i.test(l)
  );

  const items = [];
  let current = null;

  const isCodeLine = (l) => /^[A-Z0-9]{1,6}-[A-Z0-9]{1,10}\b/.test(l);
  const qtyPriceValue = (l) => {
    const m = l.match(/^([\d\.]+)\s+([A-Za-zÁÉÍÓÚÑñ\.]+)\s+(\d+)\s+([\d\.]+)$/);
    if (!m) return null;
    return { cantidad: `${m[1]} ${m[2]}`, precio: m[3], valor: m[4] };
  };

  for (const line of cleaned) {
    if (isCodeLine(line)) {
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
      current.descripcion = (current.descripcion ? current.descripcion + "\n" : "") + line;
    }
  }

  if (current) items.push(current);
  return items;
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
 * Extrae un bloque de texto “de ubicación” para no capturar de más.
 * Buscamos desde "DIRECCION" hasta antes de "Fecha Emision" o "MONTO NETO" o "Codigo Descripcion".
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
 * Extrae COMUNA y CIUDAD sin pegar etiquetas (corta en CONTACTO/GIRO/etc).
 * Funciona aunque venga “COMUNA QUILICURA CIUDAD:SANTIAGO CONTACTO:JAVIERA”.
 */
function extractComunaCiudad(text) {
  const block = getLocationBlock(text);

  const cutRe = /(?=\b(CONTACTO|GIRO|DIRECCI[ÓO]N|FECHA\s+EMISI[ÓO]N|MONTO\s+NETO|TOTAL|I\.V\.A\.)\b)/i;

  // COMUNA ...
  let comuna = null;
  {
    const m = block.match(/COMUNA\s*:?\s*([\s\S]{0,80})/i);
    if (m && m[1]) {
      let v = m[1].split(cutRe)[0]; // corta al aparecer otra etiqueta
      // si también contiene "CIUDAD" pegado, cortamos antes
      v = v.split(/CIUDAD\s*:?/i)[0];
      comuna = cleanValue(v);
    }
  }

  // CIUDAD ...
  let ciudad = null;
  {
    const m = block.match(/CIUDAD\s*:?\s*([\s\S]{0,80})/i);
    if (m && m[1]) {
      let v = m[1].split(cutRe)[0];
      // si contiene "CONTACTO" pegado, cortamos antes
      v = v.split(/\bCONTACTO\b/i)[0];
      ciudad = cleanValue(v);
    }
  }

  return { comuna, ciudad };
}

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

  // FIX: comuna/ciudad robusto, sin “pegote”
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

