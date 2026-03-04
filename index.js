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
   PARSER ROBUSTO
========================= */

function normalizeText(t) {
  return (t || "")
    .replace(/\r/g, "\n")
    .replace(/[ \t]+/g, " ")
    .replace(/\u00A0/g, " ")           // espacios no-break
    .replace(/\n{2,}/g, "\n")
    .trim();
}

function escRe(s) {
  return (s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
function cleanValue(v) {
  if (v == null) return null;
  return String(v)
    .replace(/^[\s.:;-]+/, "")   // quita :, ., ;, - al inicio
    .replace(/[\s.:;-]+$/, "")   // quita al final
    .replace(/\s{2,}/g, " ")
    .trim() || null;
}
// Toma primera página aproximada si el PDF repite encabezado
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
 * Encuentra el valor asociado a una etiqueta (muy tolerante):
 * - acepta ":" opcional
 * - si el valor no está en la misma línea, toma la siguiente línea no vacía
 * - si el PDF parte el texto, también intenta una búsqueda "suave"
 */
function pickAfterLabel(text, labelVariants) {
  const t = text || "";

  for (const label of labelVariants) {
    const L = escRe(label);

    // Caso 1: misma línea
    // (?:^|\n) LABEL : valor
    const reSameLine = new RegExp(`(?:^|\\n)\\s*${L}\\s*:??\\s*(.+)`, "i");
    const m1 = t.match(reSameLine);
    if (m1 && m1[1]) {
    const v = cleanValue(m1[1]);
    if (v && v !== "-" && v !== "—") return v;
    }

    // Caso 2: línea siguiente
    const reNextLine = new RegExp(`(?:^|\\n)\\s*${L}\\s*:??\\s*\\n\\s*([^\\n]+)`, "i");
    const m2 = t.match(reNextLine);
    if (m2 && m2[1]) {
    const v = cleanValue(m2[1]);
    if (v && v !== "-" && v !== "—") return v;
    }
  }

  // Caso 3 (fallback): búsqueda suave por si el PDF mete saltos raros
  // Busca "LABEL" y toma los siguientes 80 caracteres, y saca la primera línea útil
  for (const label of labelVariants) {
    const idx = t.toUpperCase().indexOf(label.toUpperCase());
    if (idx !== -1) {
      const chunk = t.slice(idx, idx + 120);
      const after = chunk.split("\n").slice(1).join("\n").trim();
      if (after) {
        const firstLine = after.split("\n")[0].trim();
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
  return m ? m[1].trim() : null;
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

  // COMUNA / CIUDAD: juntas o separadas
  let comuna = null, ciudad = null;

  // Caso 1: misma línea
  let mLoc = text.match(/COMUNA\s*:?\s*(.+?)\s+CIUDAD\s*:?\s*(.+)/i);
  if (mLoc) {
    comuna = mLoc[1].trim();
    ciudad = mLoc[2].trim();
  } else {
    // Caso 2: separadas
    comuna = pickAfterLabel(text, ["COMUNA", "COMUNA:"]);
    ciudad = pickAfterLabel(text, ["CIUDAD", "CIUDAD:"]);
  }

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
    const preview = text.slice(0, 2000);

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

