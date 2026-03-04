import express from "express";
import cors from "cors";
import multer from "multer";
import pdf from "pdf-parse";

const app = express();

// CORS: permite tu web (y también pruebas)
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
   TU PARSER (tal cual)
========================= */

function normalizeText(t) {
  return (t || "")
    .replace(/\r/g, "\n")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{2,}/g, "\n")
    .trim();
}

function keepFirstPageOnly(text) {
  const key = "SEÑOR(ES):";
  const first = text.indexOf(key);
  if (first === -1) return text;

  const second = text.indexOf(key, first + key.length);
  if (second === -1) return text;

  return text.slice(0, second).trim();
}

function pickAfterLabel(text, label) {
  const re = new RegExp(label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "\\s*(.+)", "i");
  const m = text.match(re);
  return m ? m[1].trim() : null;
}

function pickDocType(text) {
  const m = text.match(/\b(FACTURA ELECTRONICA|NOTA DE CREDITO|NOTA DE D[ÉE]BITO|GUIA DE DESPACHO(?: ELECTR[ÓO]NICA)?)\b/i);
  return m ? m[1].trim() : null;
}

function pickDocNumber(text) {
  const m = text.match(/\bN[º°]\s*([0-9]{1,10})\b/i);
  return m ? m[1] : null;
}

function pickFechaEmision(text) {
  const m = text.match(/Fecha\s+Emision:\s*(.+)/i);
  return m ? m[1].trim() : null;
}

function parseMoneyLine(text, label) {
  const re = new RegExp(label + "\\s*\\$\\s*([\\d\\.]+)", "i");
  const m = text.match(re);
  return m ? m[1] : null;
}

function parseItems(text) {
  const start = text.search(/Codigo\s+Descripcion/i);
  if (start === -1) return [];

  const afterStart = text.slice(start);
  const endIdx = afterStart.search(/\nReferencias:\s*/i);
  const block = endIdx === -1 ? afterStart : afterStart.slice(0, endIdx);

  const lines = block.split("\n").map(l => l.trim()).filter(Boolean);

  const cleaned = lines.filter(l => !/^Codigo\s+Descripcion/i.test(l) && !/^Adic\.\*/i.test(l));

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
  const idx = text.search(/\nReferencias:\s*/i);
  if (idx === -1) return [];

  const after = text.slice(idx);
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

  const tipo_documento = pickDocType(text);
  const numero_documento = pickDocNumber(text);

  const razon_social = pickAfterLabel(text, "SEÑOR\\(ES\\):");
  const rut = pickAfterLabel(text, "R\\.U\\.T\\.:");
  const giro = pickAfterLabel(text, "GIRO:");
  const direccion = pickAfterLabel(text, "DIRECCION:");
  const contacto = pickAfterLabel(text, "CONTACTO:");
  const fecha_emision = pickFechaEmision(text);

  let comuna = null, ciudad = null;
  const mLoc = text.match(/COMUNA\s+(.+?)\s+CIUDAD:\s*(.+)/i);
  if (mLoc) {
    comuna = mLoc[1].trim();
    ciudad = mLoc[2].trim();
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
    const preview = text.slice(0, 1200);

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
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
