import express from "express";
import cors from "cors";
import multer from "multer";
import pdf from "pdf-parse";
import crypto from "crypto";
import mysql from "mysql2/promise";

const app = express();

app.use(
  cors({
    origin: ["https://hielokolder.cl", "https://www.hielokolder.cl"],
    methods: ["GET", "POST", "DELETE", "PATCH", "OPTIONS"],
  })
);
app.use(express.json());

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const ok = file.mimetype === "application/pdf";
    cb(ok ? null : new Error("Solo se permiten PDFs"), ok);
  },
});

/* =========================
   BASE DE DATOS MySQL
========================= */

const db = await mysql.createPool({
  host: "srv2.cpanelhost.cl",
  user: "chi111878_erp_user",
  password: "9002Tabu-",
  database: "chi111878_erp_lite",
  waitForConnections: true,
  connectionLimit: 5,
});

// Crear tabla si no existe
await db.execute(`
  CREATE TABLE IF NOT EXISTS documentos (
    id VARCHAR(64) PRIMARY KEY,
    created_at VARCHAR(64),
    source_name VARCHAR(255),
    rut VARCHAR(32),
    cliente VARCHAR(255),
    tipo_documento VARCHAR(100),
    nro_documento VARCHAR(32),
    fecha VARCHAR(64),
    monto_total_digits VARCHAR(32),
    lote VARCHAR(64),
    detalle JSON,
    preview TEXT
  )
`);

console.log("Base de datos conectada y lista.");
try {
  await db.execute("ALTER TABLE documentos ADD COLUMN lotes JSON");
} catch(e) { /* ya existe */ }
/* =========================
   USUARIOS Y AUTH
========================= */

function sha256(s) {
  return crypto.createHash("sha256").update(s).digest("hex");
}

const USERS = [
  { rut: "16088206-9", passHash: sha256("123456Aa-"), nombre: "Cristian", rol: "admin" },
  { rut: "10081284-3", passHash: sha256("123456Bb-"), nombre: "Bernardo", rol: "editor" },
];

const activeSessions = {};
const SESSION_MS = 8 * 60 * 60 * 1000;

function generateToken() {
  return crypto.randomBytes(32).toString("hex");
}

function authenticate(req, res, next) {
  const auth = req.headers["authorization"] || "";
  const token = auth.replace("Bearer ", "").trim();
  const session = activeSessions[token];
  if (!session || Date.now() > session.expiresAt) {
    return res.status(401).json({ ok: false, error: "No autorizado. Inicia sesión." });
  }
  req.session = session;
  next();
}

/* =========================
   PARSER ROBUSTO
========================= */

function normalizeText(t) {
  return (t || "")
    .replace(/\r/g, "\n")
    .replace(/\u00A0/g, " ")
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

function stripTrailingLabels(value) {
  if (!value) return value;
  const stopRe =
    /\b(CONTACTO|GIRO|DIRECCI[ÓO]N|COMUNA|CIUDAD|FECHA\s+EMISI[ÓO]N|MONTO\s+NETO|I\.V\.A\.|TOTAL|REFERENCIAS|FORMA\s+DE\s+PAGO)\b/i;
  const idx = value.search(stopRe);
  if (idx > 0) return value.slice(0, idx).trim();
  return value.trim();
}

function keepFirstPageOnly(text) {
  const t = text || "";
  const re = /SEÑOR\s*\(ES\)\s*:?\s*/gi;
  const matches = [...t.matchAll(re)];
  if (matches.length <= 1) return t.trim();
  const secondIdx = matches[1].index ?? -1;
  if (secondIdx === -1) return t.trim();
  return t.slice(0, secondIdx).trim();
}

function pickAfterLabel(text, labelVariants) {
  const t = text || "";
  for (const label of labelVariants) {
    const L = escRe(label);
    const reSameLine = new RegExp(`(?:^|\\n)\\s*${L}\\s*:??\\s*(.+)`, "i");
    const m1 = t.match(reSameLine);
    if (m1 && m1[1]) {
      let v = cleanValue(m1[1]);
      v = stripTrailingLabels(v);
      if (v && v !== "-" && v !== "—") return v;
    }
    const reNextLine = new RegExp(`(?:^|\\n)\\s*${L}\\s*:??\\s*\\n\\s*([^\\n]+)`, "i");
    const m2 = t.match(reNextLine);
    if (m2 && m2[1]) {
      let v = cleanValue(m2[1]);
      v = stripTrailingLabels(v);
      if (v && v !== "-" && v !== "—") return v;
    }
  }
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
  return block.split("\n").map((l) => l.trim()).filter((l) => l.startsWith("-")).map((l) => l.replace(/^-+\s*/, ""));
}

function findRutByPattern(text) {
  const t = text || "";
  const m = t.match(/\b\d{1,2}\.?\d{3}\.?\d{3}-[0-9Kk]\b/);
  return m ? cleanValue(m[0].replace(/\./g, "")) : null;
}

function getLocationBlock(text) {
  const t = text || "";
  const start = t.search(/DIRECCI[ÓO]N/i);
  if (start === -1) return t;
  const after = t.slice(start);
  const stop = after.search(/\n(Fecha\s+Emisi[óo]n|MONTO\s+NETO|Codigo\s+Descripcion)\b/i);
  const block = stop === -1 ? after.slice(0, 450) : after.slice(0, stop);
  return block;
}

function extractComunaCiudad(text) {
  const block = getLocationBlock(text);
  const cutRe = /(?=\b(CONTACTO|GIRO|DIRECCI[ÓO]N|FECHA\s+EMISI[ÓO]N|MONTO\s+NETO|TOTAL|I\.V\.A\.|FORMA\s+DE\s+PAGO)\b)/i;
  let comuna = null;
  {
    const m = block.match(/COMUNA\s*:?\s*([\s\S]{0,160})/i);
    if (m && m[1]) {
      let v = m[1].split(cutRe)[0];
      v = v.split(/CIUDAD\s*:?/i)[0];
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

function repairBrokenCodes(s) {
  return (s || "").replace(/([A-Z]{2,5})\s*-\s*(\d{1,5})/g, "$1-$2");
}

function deglueItemTail(line) {
  let s = (line || "").trim();
  s = s.replace(/(\d)([A-Za-zÁÉÍÓÚÑñ])/g, "$1 $2").replace(/([A-Za-zÁÉÍÓÚÑñ])(\d)/g, "$1 $2").replace(/\s+/g, " ").trim();
  const stripLeadZeros = (x) => { const out = String(x || "").replace(/^0+(?=\d)/, ""); return out === "" ? "0" : out; };
  const numRe = "\\d{1,3}(?:\\.\\d{3})*|\\d+";
  const tailOkRe = new RegExp(`(?:^|\\s)(${numRe})\\s*(?:([A-Za-zÁÉÍÓÚÑñ\\.]{1,12})\\s+)?(${numRe})\\s+(${numRe})\\s*$`);
  if (tailOkRe.test(s)) return s;
  const mTwo = s.match(/(\d{1,3}(?:\.\d{3})+)(\d{1,3}(?:\.\d{3})+)\s*$/);
  if (mTwo) {
    const a = mTwo[1]; const b = mTwo[2];
    const endLen = (a + b).length;
    const base = s.slice(0, s.length - endLen).trim();
    const hasQty = /(\d{1,3}(?:\.\d{3})*|\d+)\s*$/.test(base);
    let rebuilt;
    if (/TRANSPORTE/i.test(base)) { rebuilt = `${base} 1 ${b} ${b}`; }
    else { rebuilt = hasQty ? `${base} ${a} ${b}` : `${base} 1 ${a} ${b}`; }
    if (tailOkRe.test(rebuilt)) return rebuilt;
    return rebuilt;
  }
  const m = s.match(/(\d+)(\.\d{3})+\s*$/);
  if (!m) return s;
  const dotsPart = m[0].trim();
  const idxDots = s.lastIndexOf(dotsPart);
  const beforeAll = s.slice(0, idxDots).trim();
  const firstDot = dotsPart.indexOf(".");
  const digitsBeforeDot = dotsPart.slice(0, firstDot);
  const suffix = dotsPart.slice(firstDot);
  let best = null;
  const gluedBase = digitsBeforeDot;
  for (let valFirstLen = 1; valFirstLen <= 3; valFirstLen++) {
    if (gluedBase.length < valFirstLen) continue;
    const valFirst = gluedBase.slice(-valFirstLen);
    const glued = gluedBase.slice(0, -valFirstLen);
    if (!glued) continue;
    for (let priceLen = 2; priceLen <= 6; priceLen++) {
      if (glued.length <= priceLen) continue;
      const priceDigits = glued.slice(-priceLen);
      const qtyDigits = glued.slice(0, -priceLen);
      const qty = parseInt(qtyDigits, 10);
      const price = parseInt(priceDigits, 10);
      if (!Number.isFinite(qty) || !Number.isFinite(price)) continue;
      if (qty <= 0 || qty > 100000) continue;
      if (price <= 0 || price > 2000000) continue;
      const valor = `${stripLeadZeros(valFirst)}${suffix}`;
      let score = Math.abs(qtyDigits.length - 2) * 10 + Math.abs(priceLen - 3) * 5 + valFirstLen;
      if (qtyDigits.length === 1) score += 40;
      if (parseInt(priceDigits, 10) < 100) score += 35;
      if (/^0\d/.test(valFirst)) score += 25;
      if (/0$/.test(qtyDigits)) score -= 10;
      if (!best || score < best.score) { best = { qtyDigits: stripLeadZeros(qtyDigits), priceDigits: stripLeadZeros(priceDigits), valor, score }; }
    }
  }
  if (!best) return s;
  return `${beforeAll ? beforeAll + " " : ""}${best.qtyDigits} ${best.priceDigits} ${best.valor}`.trim();
}

function parseItems(text) {
  let t = (text || "").replace(/\r/g, "\n").replace(/\u00A0/g, " ").replace(/[ \t]+/g, " ").replace(/\n{2,}/g, "\n");
  t = repairBrokenCodes(t);
  const codeReGlobal = /\b([A-Z]{2,5}-\d{1,5})/g;
  const codeReOne = /\b([A-Z]{2,5}-\d{1,5})/;
  const stopRe = /\b(Referencias:|Forma de Pago:|MONTO NETO|I\.V\.A\.|TOTAL)\b/i;
  const firstCode = t.search(codeReOne);
  if (firstCode === -1) return [];
  let working = t.slice(firstCode);
  const stop = working.search(stopRe);
  if (stop !== -1) working = working.slice(0, stop);
  const matches = [...working.matchAll(codeReGlobal)];
  if (!matches.length) return [];
  const cleanDesc = (s) => (s || "").replace(/\s+/g, " ").trim();
  const numRe = "\\d{1,3}(?:\\.\\d{3})*|\\d+";
  const unitRe = "[A-Za-zÁÉÍÓÚÑñ\\.]{1,12}";
  const tailRe = new RegExp(`(?:^|\\s)(${numRe})\\s*(?:(${unitRe})\\s+)?(${numRe})\\s+(${numRe})\\s*$`);
  const tailFindRe = new RegExp(`(${numRe})\\s*(?:(${unitRe})\\s+)?(${numRe})\\s+(${numRe})`, "g");
  const sanitizeTail = (s) => (s || "").replace(/[$*]/g, " ").replace(/\s+/g, " ").trim();
  const noiseRe = /\b(LOTE|DESDE|HASTA)\s*:?\s*.*$/i;
  const stripLeadZeros = (x) => String(x || "").replace(/^0+(?=\d)/, "");
  const toIntCL = (s) => { if (s == null) return NaN; const n = String(s).replace(/\./g, ""); const v = parseInt(n, 10); return Number.isFinite(v) ? v : NaN; };
  const formatCL = (n) => { const s = String(Math.trunc(n)); return s.replace(/\B(?=(\d{3})+(?!\d))/g, "."); };
  const fixGluedQtyIntoPrice = (qtyStr, priceStr, valorStr) => {
    const qtyNum = toIntCL(qtyStr); const valNum = toIntCL(valorStr);
    if (!Number.isFinite(qtyNum) || !Number.isFinite(valNum) || valNum <= 0) return null;
    if (!/^\d{2,3}\.\d{3}$/.test(String(priceStr))) return null;
    const [a, b] = String(priceStr).split(".");
    if (!a || !b || a.length < 2) return null;
    const qtyCandStr = a.slice(0, -1); const priceCandStr = `${a.slice(-1)}.${b}`;
    const qtyCand = toIntCL(qtyCandStr); const priceCand = toIntCL(priceCandStr);
    if (!Number.isFinite(qtyCand) || !Number.isFinite(priceCand)) return null;
    if (qtyCand <= 0 || priceCand <= 0) return null;
    if (qtyCand * priceCand !== valNum) return null;
    return { cantidad: String(qtyCand), precio: formatCL(priceCand), valor: formatCL(valNum) };
  };
  const items = [];
  for (let i = 0; i < matches.length; i++) {
    const code = matches[i][1];
    const start = matches[i].index ?? 0;
    const end = i + 1 < matches.length ? matches[i + 1].index ?? working.length : working.length;
    let chunk = working.slice(start, end);
    chunk = repairBrokenCodes(chunk);
    chunk = chunk.replace(/\n/g, " ").replace(/\s+/g, " ").trim();
    chunk = deglueItemTail(chunk);
    const codeEsc = code.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    chunk = chunk.replace(new RegExp("^" + codeEsc + "\\s*"), "").trim();
    const chunkSan = sanitizeTail(chunk);
    let mTail = chunkSan.match(tailRe);
    let lastFound = null;
    if (!mTail) { for (const m of chunkSan.matchAll(tailFindRe)) lastFound = m; if (lastFound) mTail = lastFound; }
    if (!mTail) {
      const descripcion = cleanDesc(chunkSan.replace(noiseRe, "").trim()) || null;
      if (descripcion) items.push({ codigo: code, descripcion, cantidad: null, precio: null, valor: null });
      continue;
    }
    let qtyRaw = mTail[1]; let precioRaw = mTail[3]; let valorRaw = mTail[4];
    let cantidad = stripLeadZeros(qtyRaw); let precio = stripLeadZeros(precioRaw);
    let valor = String(valorRaw || "").replace(/^0+(?=\d)/, "");
    const fixed = fixGluedQtyIntoPrice(cantidad, precioRaw, valorRaw);
    if (fixed) { cantidad = fixed.cantidad; precio = fixed.precio; valor = fixed.valor; }
    else {
      const pNum = toIntCL(precioRaw); const vNum = toIntCL(valorRaw);
      if (!/\./.test(String(precioRaw)) && Number.isFinite(pNum) && pNum >= 1000) precio = formatCL(pNum);
      if (!/\./.test(String(valorRaw)) && Number.isFinite(vNum) && vNum >= 1000) valor = formatCL(vNum);
    }
    let descPart;
    if (lastFound && typeof lastFound.index === "number") { descPart = chunkSan.slice(0, lastFound.index).trim(); }
    else { const tailFull = mTail[0]; descPart = chunkSan.slice(0, chunkSan.length - tailFull.length).trim(); }
    descPart = descPart.replace(noiseRe, "").trim();
    const descripcion = cleanDesc(descPart) || null;
    items.push({ codigo: code, descripcion, cantidad, precio, valor });
  }
  return items;
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
  const { comuna, ciudad } = extractComunaCiudad(text);
  const items = parseItems(text);
  const referencias = parseReferences(text);
  const monto_neto = parseMoneyLine(text, "MONTO NETO");
  const iva_19 = parseMoneyLine(text, "I\\.V\\.A\\.\\s*19%");
  const total = parseMoneyLine(text, "TOTAL");
  return { tipo_documento, numero_documento, razon_social, rut, giro, direccion, comuna, ciudad, contacto, fecha_emision, items, referencias, monto_neto, iva_19, total };
}

/* =========================
   ENDPOINTS
========================= */

app.get("/health", (req, res) => res.json({ ok: true }));

// LOGIN
app.post("/api/login", (req, res) => {
  const { rut, password } = req.body || {};
  if (!rut || !password) return res.status(400).json({ ok: false, error: "Falta RUT o contraseña." });
  const rutNorm = String(rut).replace(/\s/g, "").toLowerCase();
  const user = USERS.find((u) => u.rut.toLowerCase().replace(/\s/g, "") === rutNorm);
  if (!user || user.passHash !== sha256(password)) return res.status(401).json({ ok: false, error: "RUT o contraseña incorrectos." });
  const token = generateToken();
  activeSessions[token] = { rut: user.rut, nombre: user.nombre, rol: user.rol, expiresAt: Date.now() + SESSION_MS };
  return res.json({ ok: true, token, nombre: user.nombre, rol: user.rol });
});

// LOGOUT
app.post("/api/logout", (req, res) => {
  const auth = req.headers["authorization"] || "";
  const token = auth.replace("Bearer ", "").trim();
  delete activeSessions[token];
  return res.json({ ok: true });
});

// VERIFICAR SESIÓN
app.get("/api/me", authenticate, (req, res) => {
  return res.json({ ok: true, rut: req.session.rut, nombre: req.session.nombre, rol: req.session.rol });
});

// OBTENER TODOS LOS DOCUMENTOS
app.get("/api/documentos", authenticate, async (req, res) => {
  try {
    const [rows] = await db.execute(
      "SELECT id, created_at, source_name, rut, cliente, tipo_documento, nro_documento, fecha, monto_total_digits, lote FROM documentos ORDER BY CAST(nro_documento AS UNSIGNED) DESC"
    );
    return res.json({ ok: true, documentos: rows });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error obteniendo documentos." });
  }
});

// OBTENER UN DOCUMENTO POR ID
app.get("/api/documentos/:id", authenticate, async (req, res) => {
  try {
    const [rows] = await db.execute("SELECT * FROM documentos WHERE id = ?", [req.params.id]);
    if (!rows.length) return res.status(404).json({ ok: false, error: "No encontrado." });
    const doc = rows[0];
    doc.detalle = typeof doc.detalle === "string" ? JSON.parse(doc.detalle) : doc.detalle;
    doc.lotes = typeof doc.lotes === "string" ? JSON.parse(doc.lotes || "[]") : (doc.lotes || []);
    return res.json({ ok: true, documento: doc });

  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error obteniendo documento." });
  }
});
// ACTUALIZAR LOTES DE UNA FACTURA
app.patch("/api/documentos/:id/lotes", authenticate, async (req, res) => {
  if (req.session.rol === "readonly") return res.status(403).json({ ok: false, error: "Sin permisos." });
  try {
    const { lotes } = req.body;
    if (!Array.isArray(lotes)) return res.status(400).json({ ok: false, error: "Formato inválido." });
    if (lotes.length > 10) return res.status(400).json({ ok: false, error: "Máximo 10 lotes por factura." });
    const lotesCodigos = lotes.map(l => l.codigo).join(", ");
    await db.execute(
      "UPDATE documentos SET lotes = ?, lote = ? WHERE id = ?",
      [JSON.stringify(lotes), lotesCodigos, req.params.id]
    );
    return res.json({ ok: true });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error actualizando lotes." });
  }
});
// ELIMINAR UN DOCUMENTO (solo admin)
app.delete("/api/documentos/:id", authenticate, async (req, res) => {
  if (req.session.rol !== "admin") return res.status(403).json({ ok: false, error: "Sin permisos." });
  try {
    await db.execute("DELETE FROM documentos WHERE id = ?", [req.params.id]);
    return res.json({ ok: true });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error eliminando documento." });
  }
});

// ELIMINAR TODOS (solo admin)
app.delete("/api/documentos", authenticate, async (req, res) => {
  if (req.session.rol !== "admin") return res.status(403).json({ ok: false, error: "Sin permisos." });
  try {
    await db.execute("DELETE FROM documentos");
    return res.json({ ok: true });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error eliminando documentos." });
  }
});

// SUBIR PDF (protegido)
app.post("/api/upload-pdf", authenticate, upload.single("pdf"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Falta archivo PDF" });
    const parsed = await pdf(req.file.buffer);
    const text = normalizeText(parsed.text);
    if (!text || text.length < 30) return res.status(422).json({ ok: false, error: "No se pudo leer texto suficiente. ¿Seguro que es PDF nativo?" });

    const fields = extractFacturaKolderStyle(text);
    const nro = String(fields?.numero_documento || "").trim();
    if (!nro) return res.status(422).json({ ok: false, error: "No se detectó el número de documento." });

    // Verificar duplicado
    const [existing] = await db.execute(
      "SELECT id FROM documentos WHERE CAST(nro_documento AS UNSIGNED) = CAST(? AS UNSIGNED)",
      [nro]
    );
    if (existing.length > 0) return res.status(409).json({ ok: false, error: `La factura ${nro} ya existe.` });

    const id = "doc_" + Date.now().toString(36) + "_" + crypto.randomBytes(3).toString("hex");
    const now = new Date().toISOString();

    // Extraer lote
    const items = fields?.items || [];
    const joined = items.map(it => it?.descripcion || "").join("\n");
    const loteMatch = joined.match(/LOTE\s*:\s*([A-Za-z0-9\-_.]+)/i);
    const lote = loteMatch ? loteMatch[1].trim() : "";

    // Normalizar RUT
    const rut = (fields?.rut || "").replace(/\s+/g, "").trim();

    // Normalizar monto
    const montoRaw = fields?.total || "";
    const monto_total_digits = String(montoRaw).replace(/[^\d]/g, "").replace(/^0+/, "") || "0";

    // Normalizar fecha
    const fechaRaw = fields?.fecha_emision || "";
    const fechaMatch = String(fechaRaw).match(/\b(\d{2})[\/-](\d{2})[\/-](\d{4})\b/);
    const fecha = fechaMatch ? `${fechaMatch[1]}-${fechaMatch[2]}-${fechaMatch[3]}` : String(fechaRaw).trim();

    await db.execute(
      `INSERT INTO documentos (id, created_at, source_name, rut, cliente, tipo_documento, nro_documento, fecha, monto_total_digits, lote, detalle, preview)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        id, now, req.file.originalname,
        rut,
        (fields?.razon_social || "").trim(),
        (fields?.tipo_documento || "").replace(/\s+/g, " ").trim(),
        nro, fecha, monto_total_digits, lote,
        JSON.stringify(fields),
        text.slice(0, 2500),
      ]
    );

    return res.json({ ok: true, fields, preview: text.slice(0, 2500), meta: { pages: parsed.numpages || null } });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error procesando PDF" });
  }
});
// VERIFICAR CONTRASEÑA (para confirmar acciones críticas)
app.post("/api/verify-password", authenticate, (req, res) => {
  const { password } = req.body || {};
  if (!password) return res.status(400).json({ ok: false, error: "Falta contraseña." });
  const rutNorm = req.session.rut.toLowerCase().replace(/\s/g, "");
  const user = USERS.find((u) => u.rut.toLowerCase().replace(/\s/g, "") === rutNorm);
  if (!user || user.passHash !== sha256(password)) {
    return res.status(401).json({ ok: false, error: "Contraseña incorrecta." });
  }
  return res.json({ ok: true });
});
/* =========================
   COMPRAS
========================= */

await db.execute(`
  CREATE TABLE IF NOT EXISTS compras (
    id VARCHAR(64) PRIMARY KEY,
    created_at VARCHAR(64),
    rut VARCHAR(32),
    proveedor VARCHAR(255),
    tipo_documento VARCHAR(100),
    nro_documento VARCHAR(32),
    fecha VARCHAR(64),
    neto BIGINT,
    iva BIGINT,
    descuento BIGINT DEFAULT 0,
    total BIGINT,
    items JSON
  )
`);

// LISTAR COMPRAS
app.get("/api/compras", authenticate, async (req, res) => {
  try {
    const [rows] = await db.execute(
      "SELECT id, created_at, rut, proveedor, tipo_documento, nro_documento, fecha, neto, iva, descuento, total FROM compras ORDER BY CAST(nro_documento AS UNSIGNED) DESC"
    );
    return res.json({ ok: true, compras: rows });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error obteniendo compras." });
  }
});

// OBTENER COMPRA POR ID
app.get("/api/compras/:id", authenticate, async (req, res) => {
  try {
    const [rows] = await db.execute("SELECT * FROM compras WHERE id = ?", [req.params.id]);
    if (!rows.length) return res.status(404).json({ ok: false, error: "No encontrado." });
    const doc = rows[0];
    doc.items = typeof doc.items === "string" ? JSON.parse(doc.items) : doc.items;
    return res.json({ ok: true, compra: doc });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error obteniendo compra." });
  }
});

// CREAR COMPRA
app.post("/api/compras", authenticate, async (req, res) => {
  if (req.session.rol === "readonly") return res.status(403).json({ ok: false, error: "Sin permisos." });
  try {
    const { rut, proveedor, tipo_documento, nro_documento, fecha, neto, iva, descuento, total, items } = req.body;
    if (!nro_documento) return res.status(400).json({ ok: false, error: "Falta número de documento." });

    const [existing] = await db.execute(
      "SELECT id FROM compras WHERE CAST(nro_documento AS UNSIGNED) = CAST(? AS UNSIGNED)",
      [nro_documento]
    );
    if (existing.length > 0) return res.status(409).json({ ok: false, error: `La factura ${nro_documento} ya existe.` });

    const id = "cmp_" + Date.now().toString(36) + "_" + crypto.randomBytes(3).toString("hex");
    const now = new Date().toISOString();

    await db.execute(
      `INSERT INTO compras (id, created_at, rut, proveedor, tipo_documento, nro_documento, fecha, neto, iva, descuento, total, items)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [id, now, rut, proveedor, tipo_documento, nro_documento, fecha, neto || 0, iva || 0, descuento || 0, total || 0, JSON.stringify(items || [])]
    );

    return res.json({ ok: true, id });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error guardando compra." });
  }
});

// ELIMINAR COMPRA (solo admin)
app.delete("/api/compras/:id", authenticate, async (req, res) => {
  if (req.session.rol !== "admin") return res.status(403).json({ ok: false, error: "Sin permisos." });
  try {
    await db.execute("DELETE FROM compras WHERE id = ?", [req.params.id]);
    return res.json({ ok: true });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error eliminando compra." });
  }
});
/* =========================
   BOLETAS
========================= */

await db.execute(`
  CREATE TABLE IF NOT EXISTS boletas (
    id VARCHAR(64) PRIMARY KEY,
    created_at VARCHAR(64),
    nro_documento VARCHAR(32),
    fecha VARCHAR(64),
    neto BIGINT,
    iva BIGINT,
    total BIGINT,
    items JSON
  )
`);

app.get("/api/boletas", authenticate, async (req, res) => {
  try {
    const [rows] = await db.execute(
      "SELECT id, created_at, nro_documento, fecha, neto, iva, total FROM boletas ORDER BY CAST(nro_documento AS UNSIGNED) DESC"
    );
    return res.json({ ok: true, boletas: rows });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error obteniendo boletas." });
  }
});

app.get("/api/boletas/:id", authenticate, async (req, res) => {
  try {
    const [rows] = await db.execute("SELECT * FROM boletas WHERE id = ?", [req.params.id]);
    if (!rows.length) return res.status(404).json({ ok: false, error: "No encontrado." });
    const doc = rows[0];
    doc.items = typeof doc.items === "string" ? JSON.parse(doc.items) : doc.items;
    return res.json({ ok: true, boleta: doc });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error obteniendo boleta." });
  }
});

app.post("/api/boletas", authenticate, async (req, res) => {
  if (req.session.rol === "readonly") return res.status(403).json({ ok: false, error: "Sin permisos." });
  try {
    const { nro_documento, fecha, neto, iva, total, items } = req.body;
    if (!nro_documento) return res.status(400).json({ ok: false, error: "Falta número de boleta." });
    const [existing] = await db.execute(
      "SELECT id FROM boletas WHERE CAST(nro_documento AS UNSIGNED) = CAST(? AS UNSIGNED)",
      [nro_documento]
    );
    if (existing.length > 0) return res.status(409).json({ ok: false, error: `La boleta ${nro_documento} ya existe.` });
    const id = "bol_" + Date.now().toString(36) + "_" + crypto.randomBytes(3).toString("hex");
    await db.execute(
      `INSERT INTO boletas (id, created_at, nro_documento, fecha, neto, iva, total, items) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [id, new Date().toISOString(), nro_documento, fecha, neto || 0, iva || 0, total || 0, JSON.stringify(items || [])]
    );
    return res.json({ ok: true, id });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error guardando boleta." });
  }
});

app.delete("/api/boletas/:id", authenticate, async (req, res) => {
  if (req.session.rol !== "admin") return res.status(403).json({ ok: false, error: "Sin permisos." });
  try {
    await db.execute("DELETE FROM boletas WHERE id = ?", [req.params.id]);
    return res.json({ ok: true });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error eliminando boleta." });
  }
});
/* =========================
   ANÁLISIS DE VENTAS
========================= */

app.get("/api/analisis/ventas", authenticate, async (req, res) => {
  if (req.session.rol !== "admin") return res.status(403).json({ ok: false, error: "Sin permisos." });
  try {
    const [facturas] = await db.execute(
      "SELECT id, nro_documento, fecha, rut, cliente, tipo_documento, monto_total_digits, detalle, lotes FROM documentos"
    );
    const [boletas] = await db.execute(
      "SELECT id, nro_documento, fecha, neto, iva, total, items FROM boletas"
    );
    const [compras] = await db.execute(
      "SELECT id, nro_documento, fecha, rut, proveedor, tipo_documento, neto, iva, total, items FROM compras"
    );
    return res.json({ ok: true, facturas, boletas, compras });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error obteniendo datos." });
  }
});
/* =========================
   TRAZABILIDAD - LOTES
========================= */

await db.execute(`
  CREATE TABLE IF NOT EXISTS lotes (
    id VARCHAR(64) PRIMARY KEY,
    codigo VARCHAR(32) UNIQUE NOT NULL,
    producto VARCHAR(100),
    codigo_producto VARCHAR(20),
    fecha_produccion VARCHAR(20),
    hora_inicio VARCHAR(10),
    hora_fin VARCHAR(10),
    turno VARCHAR(20),
    operario VARCHAR(100),
    responsable_liberacion VARCHAR(100),
    linea VARCHAR(100),
    cantidad_kg DECIMAL(10,2),
    estado VARCHAR(30) DEFAULT 'Liberado',
    fecha_vencimiento VARCHAR(20),
    observaciones TEXT,
    created_at VARCHAR(64),
    -- Proceso
    temperatura DECIMAL(5,2),
    limpieza_previa TINYINT(1) DEFAULT 0,
    incidencias TEXT,
    -- Insumos (JSON)
    insumos JSON,
    -- Calidad
    inspeccion_visual VARCHAR(50),
    control_temperatura VARCHAR(50),
    liberado_por VARCHAR(100),
    fecha_liberacion VARCHAR(20)
  )
`);

// LISTAR LOTES
app.get("/api/lotes", authenticate, async (req, res) => {
  try {
    const [rows] = await db.execute(
      "SELECT id, codigo, producto, codigo_producto, fecha_produccion, turno, operario, cantidad_kg, estado, linea FROM lotes ORDER BY created_at DESC"
    );
    return res.json({ ok: true, lotes: rows });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error obteniendo lotes." });
  }
});

// OBTENER LOTE POR ID
app.get("/api/lotes/:id", authenticate, async (req, res) => {
  try {
    const [rows] = await db.execute("SELECT * FROM lotes WHERE id = ?", [req.params.id]);
    if (!rows.length) return res.status(404).json({ ok: false, error: "Lote no encontrado." });
    const lote = rows[0];
    lote.insumos = typeof lote.insumos === "string" ? JSON.parse(lote.insumos || "[]") : (lote.insumos || []);

    // Buscar facturas vinculadas por código de lote
    const [facturas] = await db.execute(
      "SELECT id, nro_documento, fecha, cliente, rut, monto_total_digits, lotes FROM documentos"
    );
    const vinculadas = facturas.filter(f => {
      const lotesJSON = typeof f.lotes === "string" ? JSON.parse(f.lotes || "[]") : (f.lotes || []);
      return lotesJSON.some(l => l.codigo?.trim().toUpperCase() === lote.codigo.trim().toUpperCase());
    }).map(f => ({
      id: f.id, nro_documento: f.nro_documento, fecha: f.fecha,
      cliente: f.cliente, rut: f.rut, total: f.monto_total_digits
    }));

    return res.json({ ok: true, lote, facturas_vinculadas: vinculadas });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error obteniendo lote." });
  }
});

// BUSCAR LOTES (para autocomplete — máximo 1 mes hacia atrás desde fecha factura)
app.get("/api/lotes/buscar/:q", authenticate, async (req, res) => {
  try {
    const q = `%${req.params.q}%`;
    const fechaRef = req.query.fecha || null; // fecha de la factura DD-MM-YYYY
    
    let desde = null;
    if (fechaRef) {
      const m = fechaRef.match(/^(\d{2})-(\d{2})-(\d{4})$/);
      if (m) {
        const d = new Date(parseInt(m[3]), parseInt(m[2])-1, parseInt(m[1]));
        d.setMonth(d.getMonth() - 1);
        desde = d.toISOString().slice(0,10);
      }
    }

    const [rows] = await db.execute(
      `SELECT id, codigo, producto, fecha_produccion, turno, estado
       FROM lotes 
       WHERE (codigo LIKE ? OR producto LIKE ?)
       ${desde ? "AND STR_TO_DATE(fecha_produccion, '%d-%m-%Y') >= ?" : ""}
       ORDER BY created_at DESC LIMIT 20`,
      desde ? [q, q, desde] : [q, q]
    );
    return res.json({ ok: true, lotes: rows });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error buscando lotes." });
  }
});

// CREAR LOTE
app.post("/api/lotes", authenticate, async (req, res) => {
  if (req.session.rol === "readonly") return res.status(403).json({ ok: false, error: "Sin permisos." });
  try {
    const {
      codigo, producto, codigo_producto, fecha_produccion, hora_inicio, hora_fin,
      turno, operario, responsable_liberacion, linea, cantidad_kg, estado,
      fecha_vencimiento, observaciones, temperatura, limpieza_previa, incidencias,
      insumos, inspeccion_visual, control_temperatura, liberado_por, fecha_liberacion
    } = req.body;

    if (!codigo) return res.status(400).json({ ok: false, error: "Falta código de lote." });

    const [existing] = await db.execute("SELECT id FROM lotes WHERE codigo = ?", [codigo]);
    if (existing.length > 0) return res.status(409).json({ ok: false, error: `El lote ${codigo} ya existe.` });

    const id = "lot_" + Date.now().toString(36) + "_" + crypto.randomBytes(3).toString("hex");

    const nullify = v => (v === undefined || v === "" ? null : v);
    
    await db.execute(
      `INSERT INTO lotes (id, codigo, producto, codigo_producto, fecha_produccion, hora_inicio, hora_fin,
        turno, operario, responsable_liberacion, linea, cantidad_kg, estado, fecha_vencimiento,
        observaciones, created_at, temperatura, limpieza_previa, incidencias, insumos,
        inspeccion_visual, control_temperatura, liberado_por, fecha_liberacion)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      [id, codigo, nullify(producto), nullify(codigo_producto), nullify(fecha_produccion), nullify(hora_inicio), nullify(hora_fin),
       nullify(turno), nullify(operario), nullify(responsable_liberacion), nullify(linea), cantidad_kg || 0, estado || "Liberado",
       nullify(fecha_vencimiento), nullify(observaciones), new Date().toISOString(),
       temperatura != null ? temperatura : null, limpieza_previa ? 1 : 0, nullify(incidencias),
       JSON.stringify(insumos || []),
       nullify(inspeccion_visual), nullify(control_temperatura), nullify(liberado_por), nullify(fecha_liberacion)]
    );

    return res.json({ ok: true, id });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error guardando lote." });
  }
});

// ACTUALIZAR ESTADO LOTE
app.patch("/api/lotes/:id", authenticate, async (req, res) => {
  if (req.session.rol === "readonly") return res.status(403).json({ ok: false, error: "Sin permisos." });
  try {
    const { estado, observaciones } = req.body;
    await db.execute("UPDATE lotes SET estado = ?, observaciones = ? WHERE id = ?",
      [estado, observaciones, req.params.id]);
    return res.json({ ok: true });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error actualizando lote." });
  }
});

// ELIMINAR LOTE
app.delete("/api/lotes/:id", authenticate, async (req, res) => {
  if (req.session.rol !== "admin") return res.status(403).json({ ok: false, error: "Sin permisos." });
  try {
    await db.execute("DELETE FROM lotes WHERE id = ?", [req.params.id]);
    return res.json({ ok: true });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error eliminando lote." });
  }
});
const PORT = process.env.PORT || 5050;
app.listen(PORT, "0.0.0.0", () => console.log(`Server running on port ${PORT}`));



