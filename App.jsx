import SuperAdminPanel from "./AdminPanel";
import React, { useState, useEffect, useRef, useCallback } from "react";
const API_URL = "https://7zap-inbox-production.up.railway.app";
const API_KEY = "7zap_inbox_secret";
// Supabase OAuth — redirect direto, sem SDK
const SUPABASE_URL = "https://raxnwyjcsplctrfcyeqs.supabase.co";
const oauthRedirect = (provider) => {
  const redirectTo = encodeURIComponent(window.location.origin + "/?social_callback=1");
  window.location.href = `${SUPABASE_URL}/auth/v1/authorize?provider=${provider}&redirect_to=${redirectTo}`;
};
// parse hash token after OAuth redirect
const parseOAuthHash = () => {
  const hash = window.location.hash.substring(1);
  const params = new URLSearchParams(hash);
  return params.get("access_token") || null;
};
const getSupabase = () => null; // legacy compat
const TENANT_ID = "98c38c97-2796-471f-bfc9-f093ff3ae6e9";
const headers = { "x-api-key": API_KEY, "Content-Type": "application/json" };

// Auth helpers
const authHeaders = (token) => ({ ...headers, "Authorization": `Bearer ${token}` });
const getStoredAuth = () => { try { return JSON.parse(localStorage.getItem("7crm_auth") || "null"); } catch { return null; } };
const setStoredAuth = (data) => { if (data) localStorage.setItem("7crm_auth", JSON.stringify(data)); else localStorage.removeItem("7crm_auth"); };

function timeAgo(dateStr) {
  if (!dateStr) return "";
  const now = new Date();
  const date = new Date(dateStr);
  const diff = Math.floor((now - date) / 1000);
  const isToday = now.toDateString() === date.toDateString();
  if (isToday) return date.toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" });
  const yesterday = new Date(now); yesterday.setDate(yesterday.getDate() - 1);
  if (yesterday.toDateString() === date.toDateString()) return "Ontem";
  if (diff < 6 * 86400) return date.toLocaleDateString("pt-BR", { weekday: "short" }).replace(".", "");
  return date.toLocaleDateString("pt-BR", { day: "2-digit", month: "2-digit", year: "2-digit" });
}
function initials(name) {
  if (!name) return "?";
  // Se for número de telefone (só tem dígitos, +, -, espaços, @), mostra ícone de pessoa
  const isPhone = /^[+\d\s\-().@lid]+$/.test(name.trim());
  if (isPhone) return "👤";
  return name.trim().split(/\s+/).slice(0, 2).map(w => w[0]).join("").toUpperCase();
}

// Limpa nomes como "84097321234@lid" → exibe como "+55 84 9732-1234" (se for número)
// ou mantém o nome real se não for número
function displayName(name, phone) {
  if (!name || name === phone) return formatPhone(phone || name || "");
  // Se o "nome" for um JID tipo @lid ou @c.us
  if (/@(lid|c\.us|s\.whatsapp\.net)/.test(name)) return formatPhone(phone || name);
  return name;
}

function formatPhone(raw) {
  if (!raw) return "";
  // Remove sufixos WhatsApp (@lid, @c.us, etc)
  const clean = raw.replace(/@[\w.]+/g, "").replace(/\D/g, "");
  if (!clean) return raw;
  // Formata BR: 55 + DDD (2) + número (8 ou 9)
  if (clean.startsWith("55") && (clean.length === 13 || clean.length === 12)) {
    const ddd = clean.slice(2, 4);
    const num = clean.slice(4);
    if (num.length === 9) return `+55 ${ddd} ${num.slice(0,5)}-${num.slice(5)}`;
    if (num.length === 8) return `+55 ${ddd} ${num.slice(0,4)}-${num.slice(4)}`;
  }
  // Número sem código do país (11 dígitos BR)
  if (clean.length === 11 && clean.startsWith("0") === false) {
    const ddd = clean.slice(0, 2);
    const num = clean.slice(2);
    if (num.length === 9) return `+55 ${ddd} ${num.slice(0,5)}-${num.slice(5)}`;
    if (num.length === 8) return `+55 ${ddd} ${num.slice(0,4)}-${num.slice(4)}`;
  }
  if (clean.length >= 10) return `+${clean}`;
  return raw;
}
function uid() { return Math.random().toString(36).slice(2, 10); }

const DEFAULT_COLUMNS = [
  { id: "new", label: "Nova", color: "#7c4dff" },
  { id: "attending", label: "Em Atendimento", color: "#00a884" },
  { id: "waiting", label: "Aguardando", color: "#ffd600" },
  { id: "resolved", label: "Resolvida", color: "#555555" },
];
const PALETTE = ["#00a884","#00a884","#7c4dff","#ff6d00","#e91e63","#3d5afe","#f44336","#8bc34a","#ffd600","#ff5722","#9c27b0","#555555"];

const STATUS_OPTIONS = [
  { id: "open",     label: "Aberto",    color: "#00a884", icon: "▶" },
  { id: "pending",  label: "Pendente",  color: "#ffd600", icon: "⏸" },
  { id: "resolved", label: "Resolvido", color: "#667781",    icon: "✓" },
];

const DEFAULT_LABELS_INIT = [
  { id: "lead",      name: "Lead quente", color: "#00a884" },
  { id: "doubt",     name: "Dúvida",      color: "#00a884" },
  { id: "complaint", name: "Reclamação",  color: "#f44336" },
  { id: "renewal",   name: "Renovação",   color: "#ff6d00" },
  { id: "financial", name: "Financeiro",  color: "#7c4dff" },
];

function loadLabels() {
  try { const s = localStorage.getItem("7zap_labels"); if (s) return JSON.parse(s); } catch (e) {}
  return DEFAULT_LABELS_INIT;
}
function saveLabels(labels) { try { localStorage.setItem("7zap_labels", JSON.stringify(labels)); } catch (e) {} }
function loadColumns() {
  try { const s = localStorage.getItem("7zap_kanban_columns"); if (s) return JSON.parse(s); } catch (e) {}
  return DEFAULT_COLUMNS;
}
function saveColumns(cols) { try { localStorage.setItem("7zap_kanban_columns", JSON.stringify(cols)); } catch (e) {} }

// Cache de fotos de perfil: phone → url (ou false se não tem)
const _photoCache = {};

function Avatar({ name, size = 36, phone, instanceFilter }) {
  const colors = ["#00a884","#00a884","#7c4dff","#ff6d00","#e91e63","#3d5afe"];
  const color = colors[(name || "").charCodeAt(0) % colors.length];
  const [photoUrl, setPhotoUrl] = React.useState(() => {
    const key = phone || name;
    return _photoCache[key] || null;
  });

  React.useEffect(() => {
    if (!phone || !instanceFilter) return;
    const key = phone;
    if (_photoCache[key] !== undefined) {
      if (_photoCache[key]) setPhotoUrl(_photoCache[key]);
      return;
    }
    // Mark as loading to avoid duplicate requests
    _photoCache[key] = false;
    const API_URL_LOCAL = "https://7zap-inbox-production.up.railway.app";
    const API_KEY_LOCAL = "7zap_inbox_secret";
    fetch(`${API_URL_LOCAL}/contacts/profile-picture?phone=${encodeURIComponent(phone)}&instance=${encodeURIComponent(instanceFilter)}`, {
      headers: { "x-api-key": API_KEY_LOCAL }
    })
      .then(r => r.ok ? r.json() : null)
      .then(d => {
        if (d?.url) {
          _photoCache[key] = d.url;
          setPhotoUrl(d.url);
        }
      })
      .catch(() => {});
  }, [phone, instanceFilter]);

  if (photoUrl) {
    return (
      <div style={{ width: size, height: size, borderRadius: "50%", flexShrink: 0, overflow: "hidden", background: color }}>
        <img src={photoUrl} alt={name} width={size} height={size}
          style={{ width: "100%", height: "100%", objectFit: "cover" }}
          onError={() => { _photoCache[phone] = false; setPhotoUrl(null); }}
        />
      </div>
    );
  }
  return <div style={{ width: size, height: size, borderRadius: "50%", background: color, display: "flex", alignItems: "center", justifyContent: "center", fontSize: size * 0.38, fontWeight: 700, color: "#fff", flexShrink: 0, fontFamily: "inherit" }}>{initials(name)}</div>;
}
function StatusDot({ status }) {
  const colors = { open: "#00a884", pending: "#ffd600", resolved: "#667781" };
  return <span style={{ width: 8, height: 8, borderRadius: "50%", background: colors[status] || "#667781", display: "inline-block", flexShrink: 0 }} />;
}
function LabelChip({ label, onRemove }) {
  return <span style={{ display: "inline-flex", alignItems: "center", gap: 4, padding: "2px 8px", borderRadius: 20, background: label.color + "22", border: `1px solid ${label.color}44`, color: label.color, fontSize: 11, fontWeight: 600 }}>{label.name}{onRemove && <span onClick={onRemove} style={{ cursor: "pointer", opacity: 0.7, marginLeft: 2 }}>×</span>}</span>;
}
function KanbanBadge({ stage, columns }) {
  if (!stage || stage === "new") return null;
  const col = columns.find(c => c.id === stage);
  if (!col) return null;
  return <span style={{ display: "inline-flex", alignItems: "center", gap: 4, padding: "2px 8px", borderRadius: 20, background: col.color + "22", border: `1px solid ${col.color}44`, color: col.color, fontSize: 11, fontWeight: 600 }}><span style={{ width: 6, height: 6, borderRadius: "50%", background: col.color, display: "inline-block" }} />{col.label}</span>;
}

// ─── Status Dropdown ──────────────────────────────────────────────────────────
function StatusDropdown({ status, onChange, isChanging }) {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);
  const current = STATUS_OPTIONS.find(s => s.id === status) || STATUS_OPTIONS[0];

  useEffect(() => {
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  return (
    <div ref={ref} style={{ position: "relative" }}>
      <button
        onClick={() => setOpen(o => !o)}
        disabled={!!isChanging}
        style={{ display: "flex", alignItems: "center", gap: 6, padding: "5px 12px", borderRadius: 6, border: `1px solid ${current.color}44`, background: current.color + "15", color: isChanging ? "#667781" : current.color, fontSize: 12, fontWeight: 700, cursor: isChanging ? "wait" : "pointer", fontFamily: "inherit", opacity: isChanging ? 0.7 : 1 }}
      >
        {isChanging
          ? <span style={{ width: 12, height: 12, border: "2px solid #d1d7db", borderTop: `2px solid ${current.color}`, borderRadius: "50%", display: "inline-block", animation: "spin 0.7s linear infinite" }} />
          : <span style={{ width: 7, height: 7, borderRadius: "50%", background: current.color, display: "inline-block" }} />}
        {isChanging ? "Atualizando..." : current.label}
        {!isChanging && <span style={{ fontSize: 10, opacity: 0.7 }}>{open ? "▲" : "▼"}</span>}
      </button>
      {open && (
        <div style={{ position: "absolute", top: "110%", right: 0, background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 10, padding: 6, minWidth: 160, zIndex: 200, boxShadow: "0 1px 3px #0000001a, 0 4px 12px #0000000f" }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: "#667781", padding: "4px 10px 6px", letterSpacing: 1 }}>ALTERAR STATUS</div>
          {STATUS_OPTIONS.map(opt => (
            <div
              key={opt.id}
              onClick={() => { onChange(opt.id); setOpen(false); }}
              style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 12px", borderRadius: 7, cursor: "pointer", background: status === opt.id ? opt.color + "18" : "transparent" }}
              onMouseEnter={e => e.currentTarget.style.background = opt.color + "18"}
              onMouseLeave={e => e.currentTarget.style.background = status === opt.id ? opt.color + "18" : "transparent"}
            >
              <span style={{ width: 8, height: 8, borderRadius: "50%", background: opt.color, flexShrink: 0 }} />
              <span style={{ fontSize: 13, fontWeight: 600, color: status === opt.id ? opt.color : "#54656f" }}>{opt.label}</span>
              {status === opt.id && <span style={{ marginLeft: "auto", color: opt.color, fontSize: 12 }}>✓</span>}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ─── Label Manager Modal ──────────────────────────────────────────────────────
function LabelManagerModal({ labels, onChange, onClose, tenantId, authHeaders, labelsApiError }) {
  const [items, setItems] = useState(labels.map(l => ({ ...l })));
  const [editingId, setEditingId] = useState(null);
  const [pickingColorFor, setPickingColorFor] = useState(null);
  const [newName, setNewName] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  const update = (id, patch) => setItems(prev => prev.map(l => l.id === id ? { ...l, ...patch } : l));
  const remove = (id) => setItems(prev => prev.filter(l => l.id !== id));
  const addLabel = () => {
    if (!newName.trim()) return;
    // Temporary ID with prefix so we know it's new
    setItems(prev => [...prev, { id: "NEW_" + uid(), name: newName.trim(), color: PALETTE[prev.length % PALETTE.length] }]);
    setNewName("");
  };

  const save = async () => {
    setSaving(true); setError("");
    // If backend not deployed, save to localStorage only
    if (labelsApiError === "backend_not_deployed") {
      const finalized = items.map(i => i.id.startsWith("NEW_") ? { ...i, id: uid() } : i);
      onChange(finalized);
      onClose();
      setSaving(false);
      return;
    }
    try {
      const original = labels;
      const results = [];
      for (const item of items) {
        if (item.id.startsWith("NEW_")) {
          const r = await fetch(`${API_URL}/labels`, { method: "POST", headers: authHeaders,
            body: JSON.stringify({ tenant_id: tenantId, name: item.name, color: item.color }) });
          if (!r.ok) {
            // Fallback: save locally with real uuid
            results.push({ ...item, id: uid() });
          } else {
            const d = await r.json();
            results.push(d.label);
          }
        } else {
          const orig = original.find(o => o.id === item.id);
          if (orig && (orig.name !== item.name || orig.color !== item.color)) {
            const r = await fetch(`${API_URL}/labels/${item.id}`, { method: "PUT", headers: authHeaders,
              body: JSON.stringify({ name: item.name, color: item.color }) });
            if (r.ok) { const d = await r.json(); results.push(d.label); }
            else results.push(item);
          } else {
            results.push(item);
          }
        }
      }
      for (const orig of original) {
        if (!items.find(i => i.id === orig.id)) {
          await fetch(`${API_URL}/labels/${orig.id}`, { method: "DELETE", headers: authHeaders }).catch(() => {});
        }
      }
      onChange(results);
      onClose();
    } catch (e) {
      setError(e.message || "Erro ao salvar");
    }
    setSaving(false);
  };

  return (
    <div style={{ position: "fixed", inset: 0, background: "#00000055", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 200 }} onClick={onClose}>
      <div onClick={e => e.stopPropagation()} onMouseDown={e => e.preventDefault()} style={{ background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 14, padding: 24, width: 420, maxHeight: "85vh", overflowY: "auto", boxShadow: "0 2px 5px #0000001a, 0 8px 20px #00000012" }}>
        <div style={{ fontWeight: 700, fontSize: 16, marginBottom: 4 }}>🏷 Gerenciar Etiquetas</div>
        <div style={{ fontSize: 12, color: "#667781", marginBottom: 12 }}>Crie, renomeie, recolora ou remova etiquetas</div>
        {labelsApiError === "backend_not_deployed" && (
          <div style={{ background: "#ff6d0015", border: "1px solid #ff6d0033", borderRadius: 8, padding: "8px 12px", marginBottom: 12, fontSize: 11, color: "#ff6d00" }}>
            ⚠️ Endpoint <code>/labels</code> não encontrado no backend. Suba o <strong>main.py</strong> atualizado no Railway. Usando etiquetas locais por enquanto.
          </div>
        )}

        <div style={{ display: "flex", flexDirection: "column", gap: 8, marginBottom: 20 }}>
          {items.map(label => (
            <div key={label.id} style={{ position: "relative" }}>
              <div style={{ display: "flex", alignItems: "center", gap: 10, background: "#ffffff", border: "1px solid #e9edef", borderRadius: 10, padding: "10px 14px" }}>
                <div
                  onClick={() => setPickingColorFor(pickingColorFor === label.id ? null : label.id)}
                  style={{ width: 22, height: 22, borderRadius: "50%", background: label.color, cursor: "pointer", flexShrink: 0, border: "1px solid #e9edef" }}
                />
                {editingId === label.id
                  ? <input autoFocus value={label.name} onChange={e => update(label.id, { name: e.target.value })} onBlur={() => setEditingId(null)} onKeyDown={e => e.key === "Enter" && setEditingId(null)} style={{ flex: 1, background: "#e9edef", border: `1px solid ${label.color}66`, borderRadius: 6, color: "#111b21", fontSize: 13, padding: "4px 10px", outline: "none", fontFamily: "inherit" }} />
                  : <span onClick={() => setEditingId(label.id)} style={{ flex: 1, fontSize: 13, fontWeight: 600, cursor: "text", color: label.color }}>{label.name}</span>
                }
                <span onClick={() => setEditingId(label.id)} style={{ fontSize: 14, cursor: "pointer", opacity: 0.4 }}>✏️</span>
                <span onClick={() => remove(label.id)} style={{ fontSize: 14, cursor: "pointer", opacity: 0.4 }}>🗑</span>
              </div>
              {pickingColorFor === label.id && (
                <div style={{ position: "absolute", top: "110%", left: 0, zIndex: 300, background: "#e9edef", border: "1px solid #e9edef", borderRadius: 10, padding: 12, display: "flex", flexWrap: "wrap", gap: 8, width: 200, boxShadow: "0 1px 3px #0000001a, 0 4px 12px #0000000f" }}>
                  {PALETTE.map(c => (
                    <div key={c} onClick={() => { update(label.id, { color: c }); setPickingColorFor(null); }} style={{ width: 28, height: 28, borderRadius: "50%", background: c, cursor: "pointer", border: label.color === c ? "3px solid #fff" : "2px solid transparent" }} />
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>

        <div style={{ marginBottom: 20 }}>
          <div style={{ fontSize: 12, fontWeight: 600, color: "#667781", marginBottom: 8 }}>Nova etiqueta</div>
          <div style={{ display: "flex", gap: 8 }}>
            <input value={newName} onChange={e => setNewName(e.target.value)} onKeyDown={e => e.key === "Enter" && addLabel()} placeholder="Nome da etiqueta..." style={{ flex: 1, padding: "8px 12px", background: "#ffffff", border: "1px solid #e9edef", borderRadius: 8, color: "#111b21", fontSize: 13, outline: "none", fontFamily: "inherit" }} />
            <button onClick={addLabel} disabled={!newName.trim()} style={{ padding: "8px 16px", borderRadius: 8, border: "none", background: newName.trim() ? "linear-gradient(135deg, #00a884, #017561)" : "#e9edef", color: newName.trim() ? "#000" : "#667781", fontSize: 13, fontWeight: 700, cursor: newName.trim() ? "pointer" : "not-allowed", fontFamily: "inherit" }}>+ Criar</button>
          </div>
        </div>

        {error && <div style={{ background: "#f4433315", border: "1px solid #f4433333", borderRadius: 8, padding: "8px 12px", marginBottom: 12, fontSize: 12, color: "#f44336" }}>❌ {error}</div>}
        <div style={{ display: "flex", gap: 8 }}>
          <button onClick={onClose} style={{ flex: 1, padding: "9px 0", borderRadius: 8, border: "1px solid #e9edef", background: "transparent", color: "#667781", fontSize: 13, cursor: "pointer", fontFamily: "inherit" }}>Cancelar</button>
          <button onClick={save} disabled={saving} style={{ flex: 2, padding: "9px 0", borderRadius: 8, border: "none", background: saving ? "#e9edef" : "linear-gradient(135deg, #00a884, #017561)", color: saving ? "#667781" : "#000", fontSize: 13, fontWeight: 700, cursor: saving ? "not-allowed" : "pointer", fontFamily: "inherit" }}>{saving ? "Salvando..." : "💾 Salvar etiquetas"}</button>
        </div>
      </div>
    </div>
  );
}

// ─── Login Screen ────────────────────────────────────────────────────────────
function LoginScreen({ onLogin }) {
  const params = new URLSearchParams(window.location.search);
  const resetToken = params.get("reset");
  const inviteCode = params.get("invite");

  // qual tela mostrar: "login" | "forgot" | "reset" | "register"
  const [screen, setScreen] = useState(resetToken ? "reset" : inviteCode ? "register" : "login");

  // campos login
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [showPw, setShowPw] = useState(false);

  // campos forgot
  const [fEmail, setFEmail] = useState("");
  const [fPhone, setFPhone] = useState("");
  const [fMode, setFMode] = useState("email"); // "email" | "whatsapp"
  const [fDone, setFDone] = useState(false);
  const [fResetUrl, setFResetUrl] = useState("");
  const [fWappSent, setFWappSent] = useState(false);

  // campos reset
  const [rToken] = useState(resetToken || "");
  const [rPw, setRPw] = useState("");
  const [rPw2, setRPw2] = useState("");
  const [rDone, setRDone] = useState(false);

  // campos register (convite)
  const [invInfo, setInvInfo] = useState(null);
  const [regName, setRegName] = useState("");
  const [regEmail, setRegEmail] = useState("");
  const [regPw, setRegPw] = useState("");
  const [regDone, setRegDone] = useState(false);

  // campos trial (auto-cadastro)
  const [trialCompany, setTrialCompany] = useState("");
  const [trialName, setTrialName] = useState("");
  const [trialEmail, setTrialEmail] = useState("");
  const [trialPhone, setTrialPhone] = useState("");
  const [trialPw, setTrialPw] = useState("");
  const [trialSegment, setTrialSegment] = useState("academia");
  const [trialDone, setTrialDone] = useState(false);
  const [trialResult, setTrialResult] = useState(null);

  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  // Valida convite ao carregar
  useEffect(() => {
    if (inviteCode) {
      fetch(`${API_URL}/auth/invite/${inviteCode}`)
        .then(r => r.json())
        .then(d => { if (d.ok) setInvInfo(d); else setError("Convite inválido ou expirado."); })
        .catch(() => setError("Erro ao validar convite."));
    }
  }, []);

  const Logo = () => (
    <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 32, justifyContent: "center" }}>
      <div style={{ width: 44, height: 44, borderRadius: 12, background: "linear-gradient(135deg, #00a884, #00695c)", display: "flex", alignItems: "center", justifyContent: "center", boxShadow: "0 2px 12px #00a88440" }}>
        <span style={{ fontSize: 26, fontWeight: 900, color: "#fff", fontFamily: "sans-serif", lineHeight: 1 }}>7</span>
      </div>
      <span style={{ fontSize: 26, fontWeight: 800, letterSpacing: "-0.5px", color: "#00a884" }}>CRM</span>
    </div>
  );

  const inp = (extra={}) => ({ width: "100%", padding: "11px 14px", background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 10, color: "#111b21", fontSize: 14, outline: "none", fontFamily: "inherit", boxSizing: "border-box", ...extra });
  const btn = (active) => ({ marginTop: 4, padding: "13px 0", borderRadius: 10, border: "none", background: active ? "linear-gradient(135deg,#00a884,#017561)" : "#e9edef", color: active ? "#000" : "#667781", fontSize: 15, fontWeight: 700, cursor: active ? "pointer" : "default", fontFamily: "inherit", width: "100%" });

  // ── LOGIN ──
  const submitLogin = async () => {
    if (!email.trim() || !password.trim()) return;
    setLoading(true); setError("");
    try {
      const r = await fetch(`${API_URL}/auth/login`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ email: email.trim(), password }) });
      const d = await r.json();
      if (!r.ok) { setError(d.detail || "Erro ao fazer login"); setLoading(false); return; }
      onLogin(d);
    } catch (e) { setError("Erro de conexão. Tente novamente."); }
    setLoading(false);
  };

  // ── SOCIAL LOGIN ──
  const [socialLoading, setSocialLoading] = useState("");
  const socialLogin = (provider) => {
    setSocialLoading(provider);
    oauthRedirect(provider); // redirect to Supabase OAuth
  };

  // Handle OAuth callback — Supabase returns token in URL hash
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (!params.get("social_callback")) return;
    // Token comes in hash fragment after redirect
    const token = parseOAuthHash();
    if (!token) { setError("Não foi possível obter o token de autenticação."); return; }
    // Clean URL
    window.history.replaceState({}, "", "/");
    setLoading(true);
    fetch(`${API_URL}/auth/social`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ access_token: token })
    }).then(r => r.json()).then(d => {
      if (d.token) { onLogin(d); }
      else { setError(d.detail || "Conta não encontrada. Solicite um convite ao administrador."); }
    }).catch(() => setError("Erro de conexão."))
    .finally(() => setLoading(false));
  }, []);

  // ── TRIAL REGISTRATION ──
  const submitTrial = async () => {
    if (!trialCompany.trim() || !trialName.trim() || !trialEmail.trim() || !trialPw.trim()) {
      setError("Preencha todos os campos obrigatórios."); return;
    }
    if (trialPw.length < 6) { setError("Senha deve ter pelo menos 6 caracteres."); return; }
    setLoading(true); setError("");
    try {
      const r = await fetch(`${API_URL}/auth/register-trial`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          company: trialCompany.trim(),
          name: trialName.trim(),
          email: trialEmail.trim().toLowerCase(),
          password: trialPw,
          phone: trialPhone.trim(),
          segment: trialSegment,
        })
      });
      const d = await r.json();
      if (!r.ok) { setError(d.detail || "Erro ao criar conta."); setLoading(false); return; }
      setTrialResult(d);
      setTrialDone(true);
      // Auto-login after trial registration
      setTimeout(async () => {
        try {
          const lr = await fetch(`${API_URL}/auth/login`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ email: trialEmail.trim().toLowerCase(), password: trialPw })
          });
          const ld = await lr.json();
          if (lr.ok && ld.token) onLogin(ld);
        } catch (e) {}
      }, 2000);
    } catch (e) { setError("Erro de conexão. Tente novamente."); }
    setLoading(false);
  };

  // ── FORGOT PASSWORD ──
  const submitForgot = async () => {
    if (!fEmail.trim()) return;
    setLoading(true); setError("");
    try {
      const r = await fetch(`${API_URL}/auth/forgot-password`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ email: fEmail.trim() }) });
      const d = await r.json();
      setFDone(true);
      if (d.reset_url) setFResetUrl(d.reset_url);
    } catch (e) { setError("Erro de conexão."); }
    setLoading(false);
  };

  // ── FORGOT via WHATSAPP ──
  const submitForgotWhatsApp = async () => {
    const phoneClean = fPhone.replace(/\D/g, "");
    if (phoneClean.length < 10) { setError("Digite um telefone válido com DDD"); return; }
    setLoading(true); setError("");
    try {
      const r = await fetch(`${API_URL}/auth/forgot-password-whatsapp`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ phone: phoneClean })
      });
      const d = await r.json();
      setFDone(true);
      setFWappSent(d.whatsapp_sent || false);
      if (d.reset_url) setFResetUrl(d.reset_url);
    } catch (e) { setError("Erro de conexão."); }
    setLoading(false);
  };

  // ── RESET PASSWORD ──
  const submitReset = async () => {
    if (!rPw || rPw !== rPw2) { setError("Senhas não coincidem"); return; }
    if (rPw.length < 6) { setError("Mínimo 6 caracteres"); return; }
    setLoading(true); setError("");
    try {
      const r = await fetch(`${API_URL}/auth/reset-password`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ token: rToken, password: rPw }) });
      const d = await r.json();
      if (!r.ok) { setError(d.detail || "Erro"); setLoading(false); return; }
      setRDone(true);
    } catch (e) { setError("Erro de conexão."); }
    setLoading(false);
  };

  // ── REGISTER WITH INVITE ──
  const submitRegister = async () => {
    if (!regName.trim() || !regEmail.trim() || regPw.length < 6) return;
    setLoading(true); setError("");
    try {
      const r = await fetch(`${API_URL}/auth/register`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ invite_code: inviteCode || invInfo?.code, name: regName.trim(), email: regEmail.trim(), password: regPw }) });
      const d = await r.json();
      if (!r.ok) { setError(d.detail || "Erro"); setLoading(false); return; }
      setRegDone(true);
    } catch (e) { setError("Erro de conexão."); }
    setLoading(false);
  };

  const box = { width: 400, padding: "40px 36px", background: "#ffffff", border: "1px solid #e9edef", borderRadius: 20, boxShadow: "0 2px 5px #0000001a, 0 8px 20px #00000012" };
  const wrap = { display: "flex", height: "100vh", width: "100vw", background: "#f0f2f5", alignItems: "center", justifyContent: "center", fontFamily: "'DM Sans','Segoe UI',sans-serif" };

  // ── TELA RESET ──
  if (screen === "reset") return (
    <div style={wrap}><div style={box}>
      <Logo />
      {rDone ? (
        <div style={{ textAlign: "center" }}>
          <div style={{ fontSize: 48, marginBottom: 12 }}>✅</div>
          <div style={{ fontSize: 18, fontWeight: 700, marginBottom: 8 }}>Senha redefinida!</div>
          <div style={{ fontSize: 13, color: "#667781", marginBottom: 24 }}>Agora você pode entrar com sua nova senha.</div>
          <button onClick={() => { setScreen("login"); window.history.replaceState({}, "", "/"); }} style={btn(true)}>Ir para o login →</button>
        </div>
      ) : (
        <>
          <div style={{ fontSize: 20, fontWeight: 700, textAlign: "center", marginBottom: 4 }}>🔐 Nova senha</div>
          <div style={{ fontSize: 13, color: "#667781", textAlign: "center", marginBottom: 28 }}>Digite sua nova senha</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
            <div><label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 6 }}>NOVA SENHA</label>
              <input type="password" value={rPw} onChange={e => setRPw(e.target.value)} placeholder="Mínimo 6 caracteres" style={inp()} /></div>
            <div><label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 6 }}>CONFIRMAR SENHA</label>
              <input type="password" value={rPw2} onChange={e => setRPw2(e.target.value)} onKeyDown={e => e.key === "Enter" && submitReset()} placeholder="Repita a senha" style={inp()} /></div>
            {error && <div style={{ background: "#f4433315", border: "1px solid #f4433333", borderRadius: 8, padding: "10px 14px", fontSize: 13, color: "#f44336" }}>❌ {error}</div>}
            <button onClick={submitReset} disabled={loading || !rPw || !rPw2} style={btn(!loading && rPw && rPw2)}>{loading ? "Salvando..." : "Redefinir senha →"}</button>
          </div>
        </>
      )}
    </div></div>
  );

  // ── TELA REGISTER ──
  // ── TELA TRIAL (auto-cadastro) ──
  if (screen === "trial") return (
    <div style={wrap}><div style={{ ...box, maxWidth: 460 }}>
      <Logo />
      {trialDone ? (
        <div style={{ textAlign: "center" }}>
          <div style={{ fontSize: 52, marginBottom: 12 }}>🚀</div>
          <div style={{ fontSize: 20, fontWeight: 800, marginBottom: 8 }}>Conta criada!</div>
          <div style={{ fontSize: 14, color: "#667781", marginBottom: 8 }}>
            Seu trial de <strong>7 dias grátis</strong> começou agora.
          </div>
          <div style={{ background: "#00a88415", border: "1px solid #00a88433", borderRadius: 10, padding: "12px 16px", marginBottom: 24, fontSize: 13, color: "#00a884" }}>
            ✅ Entrando automaticamente...
          </div>
        </div>
      ) : (
        <>
          <div style={{ fontSize: 22, fontWeight: 800, textAlign: "center", marginBottom: 4 }}>
            7 dias grátis 🎉
          </div>
          <div style={{ fontSize: 13, color: "#667781", textAlign: "center", marginBottom: 24 }}>
            Sem cartão de crédito. Cancele quando quiser.
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 13 }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
              <div>
                <label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 5 }}>EMPRESA *</label>
                <input value={trialCompany} onChange={e => setTrialCompany(e.target.value)} placeholder="Nome da empresa" style={inp()} />
              </div>
              <div>
                <label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 5 }}>SEU NOME *</label>
                <input value={trialName} onChange={e => setTrialName(e.target.value)} placeholder="João Silva" style={inp()} />
              </div>
            </div>
            <div>
              <label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 5 }}>EMAIL *</label>
              <input type="email" value={trialEmail} onChange={e => setTrialEmail(e.target.value)} placeholder="joao@empresa.com" style={inp()} />
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
              <div>
                <label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 5 }}>CELULAR</label>
                <input value={trialPhone} onChange={e => setTrialPhone(e.target.value)} placeholder="(11) 9xxxx-xxxx" style={inp()} />
              </div>
              <div>
                <label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 5 }}>SEGMENTO</label>
                <select value={trialSegment} onChange={e => setTrialSegment(e.target.value)} style={{ ...inp(), paddingRight: 8 }}>
                  <option value="academia">Academia / Fitness</option>
                  <option value="clinica">Clínica / Saúde</option>
                  <option value="comercio">Comércio</option>
                  <option value="servicos">Serviços</option>
                  <option value="agencia">Agência</option>
                  <option value="outros">Outros</option>
                </select>
              </div>
            </div>
            <div>
              <label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 5 }}>SENHA *</label>
              <input type="password" value={trialPw} onChange={e => setTrialPw(e.target.value)}
                onKeyDown={e => e.key === "Enter" && submitTrial()}
                placeholder="Mínimo 6 caracteres" style={inp()} />
            </div>
            {error && <div style={{ background: "#f4433315", border: "1px solid #f4433333", borderRadius: 8, padding: "10px 14px", fontSize: 13, color: "#f44336" }}>❌ {error}</div>}
            <button onClick={submitTrial}
              disabled={loading || !trialCompany.trim() || !trialName.trim() || !trialEmail.trim() || trialPw.length < 6}
              style={btn(!loading && trialCompany.trim() && trialName.trim() && trialEmail.trim() && trialPw.length >= 6)}>
              {loading ? "Criando conta..." : "Começar trial grátis →"}
            </button>
            <div style={{ background: "#f0f9f7", border: "1px solid #00a88420", borderRadius: 10, padding: "10px 14px", fontSize: 12, color: "#555" }}>
              🔒 <strong>7 dias grátis</strong> com acesso completo · Co-pilot IA incluído · Sem cartão de crédito
            </div>
          </div>
          <div style={{ marginTop: 18, textAlign: "center" }}>
            <span onClick={() => { setScreen("login"); setError(""); }} style={{ fontSize: 12, color: "#667781", cursor: "pointer" }}>Já tem conta? Entrar →</span>
          </div>
        </>
      )}
    </div></div>
  );

  if (screen === "register") return (
    <div style={wrap}><div style={box}>
      <Logo />
      {error && !invInfo ? (
        <div style={{ textAlign: "center" }}>
          <div style={{ fontSize: 40, marginBottom: 12 }}>❌</div>
          <div style={{ fontSize: 16, fontWeight: 700, marginBottom: 8, color: "#f44336" }}>{error}</div>
          <button onClick={() => setScreen("login")} style={{ ...btn(true), marginTop: 20 }}>Ir para o login</button>
        </div>
      ) : regDone ? (
        <div style={{ textAlign: "center" }}>
          <div style={{ fontSize: 48, marginBottom: 12 }}>🎉</div>
          <div style={{ fontSize: 18, fontWeight: 700, marginBottom: 8 }}>Conta criada!</div>
          <div style={{ fontSize: 13, color: "#667781", marginBottom: 24 }}>Bem-vindo ao CRM. Faça login para começar.</div>
          <button onClick={() => { setScreen("login"); window.history.replaceState({}, "", "/"); }} style={btn(true)}>Fazer login →</button>
        </div>
      ) : (
        <>
          <div style={{ fontSize: 20, fontWeight: 700, textAlign: "center", marginBottom: 4 }}>🎟️ Criar conta</div>
          <div style={{ fontSize: 13, color: "#667781", textAlign: "center", marginBottom: 4 }}>
            {invInfo ? <>Você foi convidado para <strong style={{ color: "#00a884" }}>{invInfo.tenant_name}</strong></> : "Carregando convite..."}
          </div>
          <div style={{ marginBottom: 24 }}/>
          <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
            <div><label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 6 }}>SEU NOME</label>
              <input value={regName} onChange={e => setRegName(e.target.value)} placeholder="João Silva" style={inp()} /></div>
            <div><label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 6 }}>EMAIL</label>
              <input type="email" value={regEmail} onChange={e => setRegEmail(e.target.value)} placeholder="seu@email.com" style={inp()} /></div>
            <div><label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 6 }}>SENHA</label>
              <input type="password" value={regPw} onChange={e => setRegPw(e.target.value)} onKeyDown={e => e.key === "Enter" && submitRegister()} placeholder="Mínimo 6 caracteres" style={inp()} /></div>
            {error && <div style={{ background: "#f4433315", border: "1px solid #f4433333", borderRadius: 8, padding: "10px 14px", fontSize: 13, color: "#f44336" }}>❌ {error}</div>}
            <button onClick={submitRegister} disabled={loading || !regName.trim() || !regEmail.trim() || regPw.length < 6 || !invInfo} style={btn(!loading && regName.trim() && regEmail.trim() && regPw.length >= 6 && invInfo)}>{loading ? "Criando..." : "Criar conta →"}</button>
          </div>
          <div style={{ marginTop: 20, textAlign: "center" }}>
            <span onClick={() => setScreen("login")} style={{ fontSize: 12, color: "#667781", cursor: "pointer" }}>Já tem conta? Entrar</span>
          </div>
        </>
      )}
    </div></div>
  );

  // ── TELA FORGOT ──
  if (screen === "forgot") return (
    <div style={wrap}><div style={box}>
      <Logo />
      {fDone ? (
        <div style={{ textAlign: "center" }}>
          <div style={{ fontSize: 48, marginBottom: 12 }}>{fWappSent ? "📲" : "📧"}</div>
          <div style={{ fontSize: 18, fontWeight: 700, marginBottom: 8 }}>
            {fWappSent ? "Link enviado pelo WhatsApp!" : "Verifique seu email"}
          </div>
          <div style={{ fontSize: 13, color: "#667781", marginBottom: 24 }}>
            {fWappSent
              ? "Enviamos o link de redefinição via WhatsApp. Verifique seu celular."
              : "Se o email estiver cadastrado, você receberá um link para redefinir a senha."}
          </div>
          {fResetUrl && (
            <div style={{ background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 10, padding: 16, marginBottom: 20, wordBreak: "break-all" }}>
              <div style={{ fontSize: 11, color: "#667781", marginBottom: 6 }}>📋 Link de recuperação (compartilhe manualmente):</div>
              <a href={fResetUrl} style={{ fontSize: 12, color: "#00a884" }}>{fResetUrl}</a>
            </div>
          )}
          <button onClick={() => { setScreen("login"); setFDone(false); setFEmail(""); setFPhone(""); setFResetUrl(""); setFWappSent(false); }} style={btn(true)}>Voltar ao login</button>
        </div>
      ) : (
        <>
          <div style={{ fontSize: 20, fontWeight: 700, textAlign: "center", marginBottom: 4 }}>🔑 Recuperar senha</div>
          <div style={{ fontSize: 13, color: "#667781", textAlign: "center", marginBottom: 20 }}>Escolha como receber o link</div>

          {/* Toggle email / whatsapp */}
          <div style={{ display: "flex", gap: 8, marginBottom: 24, background: "#f0f2f5", borderRadius: 12, padding: 4 }}>
            {[["email", "📧 Email"], ["whatsapp", "📲 WhatsApp"]].map(([m, label]) => (
              <button key={m} onClick={() => { setFMode(m); setError(""); }}
                style={{ flex: 1, padding: "10px 0", border: "none", borderRadius: 8, cursor: "pointer", fontWeight: 700, fontSize: 13,
                  background: fMode === m ? "#00a884" : "transparent",
                  color: fMode === m ? "#fff" : "#667781",
                  transition: "all .2s" }}>
                {label}
              </button>
            ))}
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
            {fMode === "email" ? (
              <div><label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 6 }}>EMAIL</label>
                <input type="email" value={fEmail} onChange={e => setFEmail(e.target.value)}
                  onKeyDown={e => e.key === "Enter" && submitForgot()}
                  placeholder="seu@email.com.br" autoFocus style={inp()} /></div>
            ) : (
              <div>
                <label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 6 }}>CELULAR (com DDD)</label>
                <input type="tel" value={fPhone} onChange={e => setFPhone(e.target.value)}
                  onKeyDown={e => e.key === "Enter" && submitForgotWhatsApp()}
                  placeholder="11 99999-9999" autoFocus style={inp()} />
                <div style={{ fontSize: 11, color: "#667781", marginTop: 5 }}>
                  📱 Você receberá o link no WhatsApp deste número
                </div>
              </div>
            )}
            {error && <div style={{ background: "#f4433315", border: "1px solid #f4433333", borderRadius: 8, padding: "10px 14px", fontSize: 13, color: "#f44336" }}>❌ {error}</div>}
            {fMode === "email"
              ? <button onClick={submitForgot} disabled={loading || !fEmail.trim()} style={btn(!loading && fEmail.trim())}>{loading ? "Enviando..." : "Enviar por email →"}</button>
              : <button onClick={submitForgotWhatsApp} disabled={loading || fPhone.replace(/\D/g,"").length < 10} style={btn(!loading && fPhone.replace(/\D/g,"").length >= 10)}>{loading ? "Enviando..." : "Enviar no WhatsApp →"}</button>
            }
          </div>
          <div style={{ marginTop: 20, textAlign: "center" }}>
            <span onClick={() => setScreen("login")} style={{ fontSize: 12, color: "#667781", cursor: "pointer" }}>← Voltar ao login</span>
          </div>
        </>
      )}
    </div></div>
  );

  // ── TELA LOGIN (default) ──
  return (
    <div style={wrap}>
      <div style={box}>
        <Logo />
        <div style={{ fontSize: 20, fontWeight: 700, textAlign: "center", marginBottom: 4, color: "#111b21" }}>Bem-vindo de volta</div>
        <div style={{ fontSize: 13, color: "#667781", textAlign: "center", marginBottom: 28 }}>Entre com seu email e senha</div>
        <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
          <div>
            <label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 6 }}>EMAIL</label>
            <input type="email" value={email} onChange={e => setEmail(e.target.value)} onKeyDown={e => e.key === "Enter" && submitLogin()} placeholder="seu@email.com.br" autoFocus style={inp()} />
          </div>
          <div>
            <label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 6 }}>SENHA</label>
            <div style={{ position: "relative" }}>
              <input type={showPw ? "text" : "password"} value={password} onChange={e => setPassword(e.target.value)} onKeyDown={e => e.key === "Enter" && submitLogin()} placeholder="••••••••" style={inp({ paddingRight: 40 })} />
              <span onClick={() => setShowPw(p => !p)} style={{ position: "absolute", right: 12, top: "50%", transform: "translateY(-50%)", cursor: "pointer", color: "#667781", fontSize: 16 }}>{showPw ? "🙈" : "👁"}</span>
            </div>
          </div>
          <div style={{ textAlign: "right", marginTop: -8 }}>
            <span onClick={() => setScreen("forgot")} style={{ fontSize: 12, color: "#667781", cursor: "pointer" }}>Esqueci minha senha</span>
          </div>
          {error && <div style={{ background: "#f4433315", border: "1px solid #f4433333", borderRadius: 8, padding: "10px 14px", fontSize: 13, color: "#f44336" }}>❌ {error}</div>}
          <button onClick={submitLogin} disabled={loading || !email.trim() || !password.trim()} style={btn(!loading && email.trim() && password.trim())}>
            {loading ? "Entrando..." : "Entrar →"}
          </button>
        </div>

        {/* Trial CTA */}
        <div style={{ marginTop: 20, background: "linear-gradient(135deg,#00a88410,#00a88425)", border: "1px solid #00a88433", borderRadius: 12, padding: "14px 18px", display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12 }}>
          <div>
            <div style={{ fontSize: 13, fontWeight: 700, color: "#00a884" }}>Novo por aqui?</div>
            <div style={{ fontSize: 11, color: "#667781", marginTop: 2 }}>7 dias grátis · Sem cartão</div>
          </div>
          <button onClick={() => { setError(""); setScreen("trial"); }}
            style={{ padding: "8px 16px", background: "#00a884", color: "#fff", border: "none", borderRadius: 8, fontSize: 13, fontWeight: 700, cursor: "pointer", whiteSpace: "nowrap", fontFamily: "inherit" }}>
            Testar grátis →
          </button>
        </div>

        {/* Divisor */}
        <div style={{ display: "flex", alignItems: "center", gap: 12, margin: "20px 0 16px" }}>
          <div style={{ flex: 1, height: 1, background: "#e9edef" }} />
          <span style={{ fontSize: 11, color: "#54656f", fontWeight: 600, whiteSpace: "nowrap" }}>ou continue com</span>
          <div style={{ flex: 1, height: 1, background: "#e9edef" }} />
        </div>

        {/* Social buttons */}
        <div style={{ display: "flex", flexDirection: "column", gap: 8, marginBottom: 20 }}>
          {[
            { provider: "google", label: "Continuar com Google", icon: (
              <svg width="18" height="18" viewBox="0 0 24 24"><path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z" fill="#4285F4"/><path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#34A853"/><path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l3.66-2.84z" fill="#FBBC05"/><path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" fill="#EA4335"/></svg>
            )},

          ].map(({ provider, label, icon }) => (
            <button key={provider} onClick={() => socialLogin(provider)} disabled={!!socialLoading}
              style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 10,
                padding: "12px 0", borderRadius: 10,
                border: "1px solid #e9edef",
                background: socialLoading === provider ? "#f0f2f5" : "#ffffff",
                color: "#111b21", fontSize: 14, fontWeight: 600, cursor: socialLoading ? "default" : "pointer",
                fontFamily: "inherit", opacity: socialLoading && socialLoading !== provider ? 0.4 : 1,
                transition: "all 0.15s", boxShadow: "0 1px 3px #0000000d" }}>
              {socialLoading === provider
                ? <span style={{ animation: "spin 1s linear infinite", display: "inline-block", fontSize: 16 }}>⏳</span>
                : icon}
              {socialLoading === provider ? "Redirecionando..." : label}
            </button>
          ))}
        </div>

        <div style={{ textAlign: "center", fontSize: 12, color: "#54656f" }}>CRM v1.0 · Estúdio Se7e</div>
      </div>
    </div>
  );
}


// ─── Licenses Panel ───────────────────────────────────────────────────────────
function LicensesPanel({ aHeaders, showToast }) {
  const [tenants, setTenants] = useState([]);
  const [loading, setLoading] = useState(true);
  const [totalMrr, setTotalMrr] = useState(0);
  const [showCreate, setShowCreate] = useState(false);
  const [cName, setCName] = useState(""); const [cEmail, setCEmail] = useState("");
  const [cPhone, setCPhone] = useState(""); const [cPlan, setCPlan] = useState("starter");
  const [creating, setCreating] = useState(false); const [createResult, setCreateResult] = useState(null);
  const [resendPhone, setResendPhone] = useState({}); const [sendingInvite, setSendingInvite] = useState({});

  const PLANS = [
    { id: "trial",    label: "Trial",    color: "#ff9800", price: "Grátis 7d",  features: ["3 atendentes","1 número","Sem IA"] },
    { id: "starter",  label: "Starter",  color: "#00a884", price: "R$ 149/mês", features: ["3 atendentes","1 número","Sem IA"] },
    { id: "pro",      label: "Pro",      color: "#00a884", price: "R$ 299/mês", features: ["8 atendentes","2 números","Co-pilot IA","Checkout PIX"] },
    { id: "business", label: "Business", color: "#7c4dff", price: "R$ 599/mês", features: ["Ilimitado","White-label","API própria"] },
  ];
  const planInfo = (id) => PLANS.find(p => p.id === id) || PLANS[1];

  const fetchTenants = async () => {
    setLoading(true);
    try {
      const r = await fetch(`${API_URL}/admin/tenants`, { headers: aHeaders });
      const d = await r.json();
      setTenants(d.tenants || []);
      setTotalMrr(d.total_mrr || 0);
    } catch (e) {}
    setLoading(false);
  };
  useEffect(() => { fetchTenants(); }, []);

  const changePlan = async (tenantId, plan) => {
    await fetch(`${API_URL}/admin/tenants/${tenantId}/plan`, { method: "PUT", headers: aHeaders, body: JSON.stringify({ plan }) });
    setTenants(prev => prev.map(t => t.id === tenantId ? { ...t, plan } : t));
    showToast(`✓ Plano → ${plan}`);
  };

  const toggleBlock = async (tenant) => {
    const block = !tenant.is_blocked;
    await fetch(`${API_URL}/admin/tenants/${tenant.id}/block`, { method: "PUT", headers: aHeaders, body: JSON.stringify({ blocked: block }) });
    setTenants(prev => prev.map(t => t.id === tenant.id ? { ...t, is_blocked: block } : t));
    showToast(block ? "🔴 Cliente suspenso" : "✅ Cliente reativado", block ? "#f44336" : "#00a884");
  };

  const resendInvite = async (tenantId) => {
    const phone = (resendPhone[tenantId] || "").replace(/\D/g, "");
    if (!phone) return showToast("Informe o telefone", "#f44336");
    setSendingInvite(p => ({ ...p, [tenantId]: true }));
    const r = await fetch(`${API_URL}/admin/tenants/${tenantId}/resend-invite`, { method: "POST", headers: aHeaders, body: JSON.stringify({ phone }) });
    const d = await r.json();
    setSendingInvite(p => ({ ...p, [tenantId]: false }));
    if (d.invite_url) { navigator.clipboard.writeText(d.invite_url).catch(() => {}); showToast(d.whatsapp_sent ? "📱 WhatsApp enviado + link copiado!" : "📋 Link copiado!"); }
  };

  const createTenant = async () => {
    if (!cName.trim() || !cEmail.trim() || creating) return;
    setCreating(true); setCreateResult(null);
    const r = await fetch(`${API_URL}/admin/tenants`, { method: "POST", headers: aHeaders,
      body: JSON.stringify({ name: cName.trim(), email: cEmail.trim(), plan: cPlan, phone: cPhone.replace(/\D/g,"") }) });
    const d = await r.json();
    setCreating(false);
    if (!d.ok) { showToast(d.detail || "Erro ao criar", "#f44336"); return; }
    setCreateResult(d);
    if (d.invite_url) navigator.clipboard.writeText(d.invite_url).catch(() => {});
    fetchTenants();
  };

  const inp = { width: "100%", padding: "9px 12px", background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 8, color: "#111b21", fontSize: 13, outline: "none", fontFamily: "inherit", boxSizing: "border-box" };
  const active = tenants.filter(t => !t.is_blocked);
  const blocked = tenants.filter(t => t.is_blocked);

  return (
    <div style={{ maxWidth: 940 }}>
      {/* MRR Header */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 12, marginBottom: 24 }}>
        {[
          { label: "MRR Total", value: `R$ ${totalMrr.toLocaleString("pt-BR")}`, color: "#00a884", icon: "💰" },
          { label: "Clientes ativos", value: active.length, color: "#00a884", icon: "✅" },
          { label: "Suspensos", value: blocked.length, color: "#f44336", icon: "🔴" },
          { label: "Total clientes", value: tenants.length, color: "#7c4dff", icon: "🏢" },
        ].map(c => (
          <div key={c.label} style={{ background: "#ffffff", border: `1px solid ${c.color}22`, borderRadius: 12, padding: "16px 18px" }}>
            <div style={{ fontSize: 20, marginBottom: 4 }}>{c.icon}</div>
            <div style={{ fontSize: 22, fontWeight: 800, color: c.color }}>{c.value}</div>
            <div style={{ fontSize: 11, color: "#667781", marginTop: 2 }}>{c.label}</div>
          </div>
        ))}
      </div>

      {/* Plan cards info */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 10, marginBottom: 24 }}>
        {PLANS.map(p => (
          <div key={p.id} style={{ background: "#ffffff", border: `1px solid ${p.color}33`, borderRadius: 12, padding: "14px 16px" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
              <span style={{ fontSize: 13, fontWeight: 700, color: p.color }}>{p.label}</span>
              <span style={{ fontSize: 11, color: "#667781" }}>{p.price}</span>
            </div>
            {p.features.map(f => <div key={f} style={{ fontSize: 11, color: "#667781", marginBottom: 2 }}>· {f}</div>)}
            <div style={{ marginTop: 8, fontSize: 12, fontWeight: 700, color: "#667781" }}>
              {tenants.filter(t => t.plan === p.id && !t.is_blocked).length} cliente(s)
            </div>
          </div>
        ))}
      </div>

      <div style={{ display: "flex", alignItems: "center", marginBottom: 16 }}>
        <div style={{ fontSize: 16, fontWeight: 700 }}>🏢 Clientes</div>
        <button onClick={() => { setShowCreate(s => !s); setCreateResult(null); }}
          style={{ marginLeft: "auto", padding: "9px 20px", borderRadius: 9, border: "none", background: "linear-gradient(135deg,#00a884,#017561)", color: "#000", fontSize: 13, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>
          + Novo cliente
        </button>
      </div>

      {/* Create form */}
      {showCreate && (
        <div style={{ background: "#ffffff", border: "1px solid #00a88433", borderRadius: 14, padding: 24, marginBottom: 20 }}>
          {createResult ? (
            <div style={{ textAlign: "center" }}>
              <div style={{ fontSize: 32, marginBottom: 8 }}>🎉</div>
              <div style={{ fontSize: 15, fontWeight: 700, marginBottom: 4 }}>Cliente criado com sucesso!</div>
              <div style={{ fontSize: 12, color: "#667781", marginBottom: 16 }}>{createResult.whatsapp_sent ? "✅ Link enviado via WhatsApp!" : "📋 Link copiado para área de transferência"}</div>
              <div style={{ background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 8, padding: "10px 14px", fontSize: 12, color: "#a78bfa", marginBottom: 16, wordBreak: "break-all" }}>{createResult.invite_url}</div>
              <div style={{ fontSize: 12, color: "#667781", marginBottom: 16 }}>Senha temporária: <strong style={{ color: "#111b21" }}>{createResult.temp_password}</strong></div>
              <div style={{ display: "flex", gap: 8, justifyContent: "center" }}>
                <button onClick={() => navigator.clipboard.writeText(createResult.invite_url).then(() => showToast("Copiado!"))}
                  style={{ padding: "8px 20px", borderRadius: 8, border: "1px solid #7c4dff44", background: "#7c4dff18", color: "#a78bfa", fontSize: 12, cursor: "pointer", fontFamily: "inherit" }}>📋 Copiar link</button>
                <button onClick={() => { setShowCreate(false); setCreateResult(null); setCName(""); setCEmail(""); setCPhone(""); setCPlan("starter"); }}
                  style={{ padding: "8px 20px", borderRadius: 8, border: "none", background: "#d1d7db", color: "#8696a0", fontSize: 12, cursor: "pointer", fontFamily: "inherit" }}>Fechar</button>
              </div>
            </div>
          ) : (
            <>
              <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 16 }}>Cadastrar novo cliente</div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, marginBottom: 12 }}>
                <div><label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 5 }}>NOME DA EMPRESA *</label>
                  <input value={cName} onChange={e => setCName(e.target.value)} placeholder="Academia Fitness XYZ" style={inp} /></div>
                <div><label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 5 }}>EMAIL DO ADMIN *</label>
                  <input type="email" value={cEmail} onChange={e => setCEmail(e.target.value)} placeholder="dono@empresa.com" style={inp} /></div>
                <div><label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 5 }}>WHATSAPP (com DDD)</label>
                  <input value={cPhone} onChange={e => setCPhone(e.target.value)} placeholder="11999998888" style={inp} /></div>
              </div>
              <div style={{ marginBottom: 16 }}>
                <label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 8 }}>PLANO</label>
                <div style={{ display: "flex", gap: 8 }}>
                  {PLANS.map(p => (
                    <div key={p.id} onClick={() => setCPlan(p.id)}
                      style={{ flex: 1, padding: "10px 12px", borderRadius: 8, border: `2px solid ${cPlan === p.id ? p.color : "#d1d7db"}`, background: cPlan === p.id ? p.color + "18" : "#f0f2f5", cursor: "pointer" }}>
                      <div style={{ fontSize: 12, fontWeight: 700, color: cPlan === p.id ? p.color : "#8696a0" }}>{p.label}</div>
                      <div style={{ fontSize: 11, color: "#667781" }}>{p.price}</div>
                    </div>
                  ))}
                </div>
              </div>
              <div style={{ display: "flex", gap: 8 }}>
                <button onClick={() => setShowCreate(false)} style={{ padding: "9px 20px", borderRadius: 8, border: "1px solid #e9edef", background: "transparent", color: "#667781", fontSize: 13, cursor: "pointer", fontFamily: "inherit" }}>Cancelar</button>
                <button onClick={createTenant} disabled={creating || !cName.trim() || !cEmail.trim()}
                  style={{ flex: 1, padding: "9px 0", borderRadius: 8, border: "none", background: (!creating && cName && cEmail) ? "linear-gradient(135deg,#00a884,#017561)" : "#e9edef", color: (!creating && cName && cEmail) ? "#000" : "#667781", fontSize: 13, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>
                  {creating ? "Criando..." : "🚀 Criar cliente" + (cPhone ? " + enviar WhatsApp" : " + copiar link")}
                </button>
              </div>
            </>
          )}
        </div>
      )}

      {/* Tenant list */}
      {loading ? <div style={{ color: "#667781", padding: 40, textAlign: "center" }}>Carregando...</div>
        : tenants.length === 0 ? (
          <div style={{ textAlign: "center", padding: 60, color: "#667781" }}>Nenhum cliente ainda. Crie o primeiro acima.</div>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {tenants.map(t => {
              const pi = planInfo(t.plan);
              const [showResend, setShowResend] = useState(false);
              return (
                <div key={t.id} style={{ background: "#ffffff", border: `1px solid ${t.is_blocked ? "#f4433322" : "#e9edef"}`, borderRadius: 12, padding: "14px 18px", opacity: t.is_blocked ? 0.7 : 1 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                    <div style={{ width: 38, height: 38, borderRadius: 10, background: pi.color + "22", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 16, flexShrink: 0 }}>🏢</div>
                    <div style={{ flex: 1 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                        <span style={{ fontSize: 14, fontWeight: 700 }}>{t.name}</span>
                        <span style={{ fontSize: 11, background: pi.color + "22", color: pi.color, padding: "1px 8px", borderRadius: 20, fontWeight: 700 }}>{pi.label}</span>
                        <span style={{ fontSize: 12, color: "#00a884", fontWeight: 700 }}>R$ {pi.price.replace("R$ ","").replace("/mês","")}<span style={{ color: "#667781", fontWeight: 400 }}>/mês</span></span>
                        {t.is_blocked && <span style={{ fontSize: 11, background: "#f4433322", color: "#f44336", padding: "1px 8px", borderRadius: 20 }}>🔴 Suspenso</span>}
                        <span style={{ fontSize: 11, color: "#667781" }}>{t.user_count || 0} usuário(s)</span>
                      </div>
                      <div style={{ fontSize: 11, color: "#667781", marginTop: 3 }}>Desde {new Date(t.created_at).toLocaleDateString("pt-BR")}</div>
                    </div>
                    <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                      <select value={t.plan || "starter"} onChange={e => changePlan(t.id, e.target.value)}
                        style={{ padding: "5px 8px", background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 7, color: "#111b21", fontSize: 12, outline: "none", fontFamily: "inherit", cursor: "pointer" }}>
                        {PLANS.map(p => <option key={p.id} value={p.id}>{p.label}</option>)}
                      </select>
                      <button onClick={() => setShowResend(s => !s)}
                        style={{ padding: "5px 10px", borderRadius: 7, border: "1px solid #7c4dff44", background: "transparent", color: "#a78bfa", fontSize: 12, cursor: "pointer", fontFamily: "inherit" }}>
                        📱 Convite
                      </button>
                      <button onClick={() => toggleBlock(t)}
                        style={{ padding: "5px 10px", borderRadius: 7, border: `1px solid ${t.is_blocked ? "#00a88433" : "#f4433333"}`, background: "transparent", color: t.is_blocked ? "#00a884" : "#f44336", fontSize: 12, cursor: "pointer", fontFamily: "inherit" }}>
                        {t.is_blocked ? "Reativar" : "Suspender"}
                      </button>
                    </div>
                  </div>
                  {showResend && (
                    <div style={{ marginTop: 12, display: "flex", gap: 8, alignItems: "center" }}>
                      <input value={resendPhone[t.id] || ""} onChange={e => setResendPhone(p => ({ ...p, [t.id]: e.target.value }))}
                        placeholder="WhatsApp do cliente (com DDD)"
                        style={{ flex: 1, padding: "7px 12px", background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 7, color: "#111b21", fontSize: 12, outline: "none", fontFamily: "inherit" }} />
                      <button onClick={() => resendInvite(t.id)} disabled={sendingInvite[t.id]}
                        style={{ padding: "7px 16px", borderRadius: 7, border: "none", background: "linear-gradient(135deg,#7c4dff,#5b21b6)", color: "#fff", fontSize: 12, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>
                        {sendingInvite[t.id] ? "Enviando..." : "📱 Enviar"}
                      </button>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
    </div>
  );
}

// ─── Admin Panel ──────────────────────────────────────────────────────────────
function AdminPanel({ auth, onLogout }) {
  const [tab, setTab] = useState("users");
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [editUser, setEditUser] = useState(null);
  const [toast, setToast] = useState(null);
  const [fName, setFName] = useState(""); const [fEmail, setFEmail] = useState(""); const [fPw, setFPw] = useState(""); const [fRole, setFRole] = useState("agent"); const [fColor, setFColor] = useState("#00a884"); const [fPerms, setFPerms] = useState("read_write"); const [saving, setSaving] = useState(false);
  const [fInstances, setFInstances] = useState([]); // allowed instance IDs — empty = all
  const [availableInstances, setAvailableInstances] = useState([]);

  useEffect(() => {
    fetch(`${API_URL}/whatsapp/tenant-instances?tenant_id=${TENANT_ID}`, { headers: aHeaders })
      .then(r => r.json()).then(d => setAvailableInstances(d.instances || [])).catch(() => {});
  }, []);
  const [showChangePw, setShowChangePw] = useState(false); const [curPw, setCurPw] = useState(""); const [newPw, setNewPw] = useState(""); const [changingPw, setChangingPw] = useState(false);

  const aHeaders = { ...headers, "Authorization": `Bearer ${auth.token}` };
  const showToast = (msg, color = "#00a884") => { setToast({ msg, color }); setTimeout(() => setToast(null), 3500); };

  const fetchUsers = async () => {
    setLoading(true);
    try { const r = await fetch(`${API_URL}/admin/users`, { headers: aHeaders }); const d = await r.json(); setUsers(d.users || []); } catch (e) {}
    setLoading(false);
  };
  useEffect(() => { fetchUsers(); }, []);

  const openCreate = () => { setEditUser(null); setFName(""); setFEmail(""); setFPw(""); setFRole("agent"); setFColor("#00a884"); setFPerms("read_write"); setFInstances([]); setShowForm(true); };
  const openEdit = (u) => { setEditUser(u); setFName(u.name); setFEmail(u.email); setFPw(""); setFRole(u.role); setFColor(u.avatar_color || "#00a884"); setFPerms(u.permissions || "read_write"); setFInstances(u.allowed_instances || []); setShowForm(true); };

  const saveUser = async () => {
    if (!fName.trim() || !fEmail.trim() || (!editUser && !fPw.trim())) return;
    setSaving(true);
    try {
      if (editUser) {
        const body = { name: fName, email: fEmail, role: fRole, avatar_color: fColor, permissions: fPerms, allowed_instances: fInstances };
        if (fPw.trim()) body.password = fPw;
        await fetch(`${API_URL}/admin/users/${editUser.id}`, { method: "PUT", headers: aHeaders, body: JSON.stringify(body) });
        showToast("✓ Usuário atualizado!");
      } else {
        await fetch(`${API_URL}/admin/users`, { method: "POST", headers: aHeaders, body: JSON.stringify({ tenant_id: auth.user.tenant_id, name: fName, email: fEmail, password: fPw, role: fRole, avatar_color: fColor, permissions: fPerms, allowed_instances: fInstances }) });
        showToast("✓ Usuário criado!");
      }
      setShowForm(false); fetchUsers();
    } catch (e) { showToast("Erro ao salvar", "#f44336"); }
    setSaving(false);
  };

  const toggleActive = async (u) => {
    await fetch(`${API_URL}/admin/users/${u.id}`, { method: "PUT", headers: aHeaders, body: JSON.stringify({ is_active: !u.is_active }) });
    showToast(u.is_active ? "Usuário desativado" : "Usuário reativado", u.is_active ? "#f44336" : "#00a884");
    fetchUsers();
  };

  const changePw = async () => {
    if (!curPw || newPw.length < 6 || changingPw) return;
    setChangingPw(true);
    try {
      const r = await fetch(`${API_URL}/auth/change-password`, { method: "POST", headers: aHeaders, body: JSON.stringify({ current_password: curPw, new_password: newPw }) });
      if (r.ok) { showToast("✓ Senha alterada!"); setShowChangePw(false); setCurPw(""); setNewPw(""); }
      else { const d = await r.json(); showToast(d.detail || "Erro", "#f44336"); }
    } catch (e) {}
    setChangingPw(false);
  };

  const COLORS = ["#00a884","#7c4dff","#00a884","#ff6d00","#f44336","#e91e63","#ffd600","#8bc34a"];
  const inp = { width: "100%", padding: "9px 12px", background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 8, color: "#111b21", fontSize: 13, outline: "none", fontFamily: "inherit", boxSizing: "border-box" };

  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden", position: "relative" }}>
      {toast && <div style={{ position: "fixed", top: 20, left: "50%", transform: "translateX(-50%)", zIndex: 9999, background: toast.color, color: "#000", padding: "11px 22px", borderRadius: 12, fontSize: 13, fontWeight: 700, boxShadow: "0 1px 3px #0000001a, 0 4px 12px #0000000f" }}>{toast.msg}</div>}
      <div style={{ padding: "10px 24px", borderBottom: "1px solid #e9edef", display: "flex", alignItems: "center", gap: 8 }}>
        <div style={{ display: "flex", gap: 3, background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 8, padding: 3 }}>
          {[["users","👥 Usuários"],["licenses","🏢 Licenças"],["account","👤 Minha conta"]].map(([id, label]) => (
            <button key={id} onClick={() => setTab(id)} style={{ padding: "5px 14px", borderRadius: 6, border: "none", background: tab === id ? "#e9edef" : "transparent", color: tab === id ? "#111b21" : "#667781", fontSize: 12, fontWeight: 600, cursor: "pointer", fontFamily: "inherit" }}>{label}</button>
          ))}
        </div>
      </div>
      <div style={{ flex: 1, overflowY: "auto", padding: 28 }}>
        {tab === "users" && (
          <div style={{ maxWidth: 860 }}>
            <div style={{ display: "flex", alignItems: "center", marginBottom: 20 }}>
              <div><div style={{ fontSize: 18, fontWeight: 700 }}>👥 Usuários</div><div style={{ fontSize: 12, color: "#667781", marginTop: 2 }}>Gerencie quem tem acesso ao CRM</div></div>
              <div style={{ marginLeft: "auto", display: "flex", gap: 8 }}>
                <button onClick={async () => {
                  const r = await fetch(`${API_URL}/auth/invite`, { method: "POST", headers: aHeaders, body: JSON.stringify({}) });
                  const d = await r.json();
                  if (d.invite_url) {
                    navigator.clipboard.writeText(d.invite_url).catch(() => {});
                    showToast("🎟️ Link copiado! " + d.code);
                  }
                }} style={{ padding: "9px 20px", borderRadius: 9, border: "1px solid #e9edef", background: "transparent", color: "#00a884", fontSize: 13, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>🎟️ Gerar convite</button>
                <button onClick={openCreate} style={{ padding: "9px 20px", borderRadius: 9, border: "none", background: "linear-gradient(135deg,#00a884,#017561)", color: "#000", fontSize: 13, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>+ Novo usuário</button>
              </div>
            </div>
            {loading ? <div style={{ color: "#667781", padding: 40, textAlign: "center" }}>Carregando...</div> : (
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                {users.map(u => (
                  <div key={u.id} style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 12, padding: "14px 18px", display: "flex", alignItems: "center", gap: 14, opacity: u.is_active ? 1 : 0.5 }}>
                    <Avatar name={u.name} size={40} color={u.avatar_color} />
                    <div style={{ flex: 1 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 2 }}>
                        <span style={{ fontSize: 14, fontWeight: 700 }}>{u.name}</span>
                        <span style={{ fontSize: 11, background: u.role === "admin" ? "#7c4dff22" : "#00a88422", color: u.role === "admin" ? "#a78bfa" : "#00a884", padding: "1px 8px", borderRadius: 20, fontWeight: 700 }}>{u.role === "admin" ? "Admin" : "Atendente"}</span>
                        {u.permissions && u.permissions !== "read_write" && <span style={{ fontSize: 10, background: "#e9edef", color: "#667781", padding: "1px 8px", borderRadius: 20 }}>{u.permissions === "read" ? "👁 Leitura" : u.permissions === "read_write_manage" ? "⚙️ Gestão" : u.permissions === "full" ? "🔑 Full" : ""}</span>}
                        {!u.is_active && <span style={{ fontSize: 11, background: "#f4433322", color: "#f44336", padding: "1px 8px", borderRadius: 20 }}>Inativo</span>}
                      </div>
                      <div style={{ fontSize: 12, color: "#667781" }}>{u.email}</div>
                      {u.last_login && <div style={{ fontSize: 11, color: "#54656f", marginTop: 2 }}>Último acesso: {new Date(u.last_login).toLocaleString("pt-BR")}</div>}
                      {u.allowed_instances?.length > 0 && (
                        <div style={{ display: "flex", gap: 4, flexWrap: "wrap", marginTop: 4 }}>
                          {u.allowed_instances.map(iid => {
                            const inst = availableInstances.find(i => i.id === iid);
                            return inst ? (
                              <span key={iid} style={{ fontSize: 10, background: "#00a88415", color: "#00a884", border: "1px solid #00a88433", padding: "1px 7px", borderRadius: 10, fontWeight: 700 }}>
                                📱 {inst.label || inst.phone}
                              </span>
                            ) : null;
                          })}
                        </div>
                      )}
                    </div>
                    <div style={{ display: "flex", gap: 6 }}>
                      <button onClick={() => openEdit(u)} style={{ padding: "6px 14px", borderRadius: 7, border: "1px solid #e9edef", background: "transparent", color: "#8696a0", fontSize: 12, cursor: "pointer", fontFamily: "inherit" }}>✏️ Editar</button>
                      {u.id !== auth.user.id && <button onClick={() => toggleActive(u)} style={{ padding: "6px 14px", borderRadius: 7, border: `1px solid ${u.is_active ? "#f4433333" : "#00a88433"}`, background: "transparent", color: u.is_active ? "#f44336" : "#00a884", fontSize: 12, cursor: "pointer", fontFamily: "inherit" }}>{u.is_active ? "Desativar" : "Reativar"}</button>}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
        {tab === "licenses" && (
          <LicensesPanel aHeaders={aHeaders} showToast={showToast} />
        )}
        {tab === "account" && (
          <div style={{ maxWidth: 480 }}>
            <div style={{ fontSize: 18, fontWeight: 700, marginBottom: 20 }}>👤 Minha conta</div>
            <div style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 14, padding: 24, marginBottom: 16 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: 20 }}>
                <Avatar name={auth.user.name} size={52} color={auth.user.avatar_color} />
                <div>
                  <div style={{ fontSize: 17, fontWeight: 700 }}>{auth.user.name}</div>
                  <div style={{ fontSize: 13, color: "#667781" }}>{auth.user.email}</div>
                  <span style={{ fontSize: 11, background: auth.user.role === "admin" ? "#7c4dff22" : "#00c85522", color: auth.user.role === "admin" ? "#a78bfa" : "#00a884", display: "inline-block", padding: "2px 10px", borderRadius: 20, marginTop: 4, fontWeight: 700 }}>{auth.user.role === "admin" ? "Administrador" : "Atendente"}</span>
                </div>
              </div>
              <button onClick={() => setShowChangePw(p => !p)} style={{ padding: "9px 20px", borderRadius: 8, border: "1px solid #e9edef", background: "transparent", color: "#8696a0", fontSize: 13, cursor: "pointer", fontFamily: "inherit" }}>🔑 Alterar senha</button>
              {showChangePw && (
                <div style={{ marginTop: 14, display: "flex", flexDirection: "column", gap: 10 }}>
                  <input type="password" value={curPw} onChange={e => setCurPw(e.target.value)} placeholder="Senha atual" style={inp} />
                  <input type="password" value={newPw} onChange={e => setNewPw(e.target.value)} placeholder="Nova senha (mín. 6 caracteres)" style={inp} />
                  <button onClick={changePw} disabled={changingPw || !curPw || newPw.length < 6} style={{ padding: "9px 0", borderRadius: 8, border: "none", background: (!changingPw && curPw && newPw.length >= 6) ? "linear-gradient(135deg,#00a884,#017561)" : "#e9edef", color: "#000", fontSize: 13, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>{changingPw ? "Salvando..." : "Salvar nova senha"}</button>
                </div>
              )}
            </div>
            <button onClick={onLogout} style={{ width: "100%", padding: "11px 0", borderRadius: 10, border: "1px solid #f4433333", background: "transparent", color: "#f44336", fontSize: 14, fontWeight: 600, cursor: "pointer", fontFamily: "inherit" }}>Sair da conta →</button>
          </div>
        )}
      </div>
      {showForm && (
        <div style={{ position: "fixed", inset: 0, background: "#00000055", zIndex: 100, display: "flex", alignItems: "center", justifyContent: "center" }}>
          <div style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 16, padding: 28, width: 420, maxWidth: "90vw" }}>
            <div style={{ display: "flex", alignItems: "center", marginBottom: 20 }}>
              <span style={{ fontSize: 16, fontWeight: 700 }}>{editUser ? "Editar usuário" : "Novo usuário"}</span>
              <span onClick={() => setShowForm(false)} style={{ marginLeft: "auto", cursor: "pointer", color: "#667781", fontSize: 20 }}>×</span>
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              <div><label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 5 }}>NOME</label><input value={fName} onChange={e => setFName(e.target.value)} placeholder="Nome completo" style={inp} /></div>
              <div><label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 5 }}>EMAIL</label><input type="email" value={fEmail} onChange={e => setFEmail(e.target.value)} placeholder="email@empresa.com" style={inp} /></div>
              <div><label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 5 }}>{editUser ? "NOVA SENHA (vazio = não alterar)" : "SENHA"}</label><input type="password" value={fPw} onChange={e => setFPw(e.target.value)} placeholder="••••••••" style={inp} /></div>
              <div><label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 5 }}>PAPEL</label>
                <select value={fRole} onChange={e => setFRole(e.target.value)} style={{ ...inp, cursor: "pointer" }}>
                  <option value="agent">Atendente</option>
                  <option value="admin">Administrador</option>
                </select>
              </div>
              <div>
                <label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 8 }}>PERMISSÕES</label>
                <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                  {[
                    { id: "read", label: "👁 Somente leitura", desc: "Visualiza conversas e mensagens" },
                    { id: "read_write", label: "✏️ Leitura + Escrita", desc: "Visualiza e responde mensagens" },
                    { id: "read_write_manage", label: "⚙️ Leitura + Escrita + Gestão", desc: "Atribui, etiqueta, move kanban" },
                    { id: "full", label: "🔑 Acesso total", desc: "Tudo + deleta conversas e contatos" },
                  ].map(p => (
                    <div key={p.id} onClick={() => setFPerms(p.id)}
                      style={{ display: "flex", gap: 10, padding: "8px 12px", borderRadius: 8, border: `1px solid ${fPerms === p.id ? "#00a88444" : "#d1d7db"}`, background: fPerms === p.id ? "#00a88410" : "transparent", cursor: "pointer" }}>
                      <div style={{ flex: 1 }}>
                        <div style={{ fontSize: 12, fontWeight: 700, color: fPerms === p.id ? "#00a884" : "#54656f" }}>{p.label}</div>
                        <div style={{ fontSize: 11, color: "#667781" }}>{p.desc}</div>
                      </div>
                      {fPerms === p.id && <span style={{ color: "#00a884", fontSize: 14, alignSelf: "center" }}>✓</span>}
                    </div>
                  ))}
                </div>
              </div>
              <div><label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 8 }}>COR DO AVATAR</label>
                <div style={{ display: "flex", gap: 8 }}>{COLORS.map(c => <div key={c} onClick={() => setFColor(c)} style={{ width: 28, height: 28, borderRadius: "50%", background: c, cursor: "pointer", border: fColor === c ? "3px solid #fff" : "3px solid transparent", boxSizing: "border-box" }} />)}</div>
              </div>
              {/* Instance access — only show if there are instances */}
              {availableInstances.length > 0 && fRole !== "admin" && (
                <div>
                  <label style={{ fontSize: 11, fontWeight: 700, color: "#667781", display: "block", marginBottom: 4 }}>ACESSO AOS NÚMEROS</label>
                  <div style={{ fontSize: 11, color: "#54656f", marginBottom: 8 }}>Vazio = acesso a todos os números. Selecione para restringir.</div>
                  <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                    {availableInstances.map(inst => {
                      const isSelected = fInstances.includes(inst.id);
                      return (
                        <div key={inst.id} onClick={() => setFInstances(prev => isSelected ? prev.filter(id => id !== inst.id) : [...prev, inst.id])}
                          style={{ display: "flex", alignItems: "center", gap: 10, padding: "9px 12px", borderRadius: 8, border: `1px solid ${isSelected ? "#00a88444" : "#d1d7db"}`, background: isSelected ? "#00a88410" : "transparent", cursor: "pointer" }}>
                          <span style={{ fontSize: 16 }}>{inst.connected ? "📱" : "📵"}</span>
                          <div style={{ flex: 1 }}>
                            <div style={{ fontSize: 12, fontWeight: 700, color: isSelected ? "#00a884" : "#54656f" }}>{inst.label || "Número"}</div>
                            <div style={{ fontSize: 11, color: "#667781" }}>{inst.phone ? `+${inst.phone}` : inst.instance_name}</div>
                          </div>
                          <div style={{ width: 18, height: 18, borderRadius: 4, border: `2px solid ${isSelected ? "#00a884" : "#54656f"}`, background: isSelected ? "#00a884" : "transparent", display: "flex", alignItems: "center", justifyContent: "center" }}>
                            {isSelected && <span style={{ fontSize: 10, color: "#000", fontWeight: 900 }}>✓</span>}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                  {fInstances.length > 0 && (
                    <div style={{ fontSize: 11, color: "#00a884", marginTop: 6 }}>
                      ✓ Acesso restrito a {fInstances.length} número{fInstances.length > 1 ? "s" : ""}
                    </div>
                  )}
                </div>
              )}
              <div style={{ display: "flex", gap: 8, marginTop: 4 }}>
                <button onClick={() => setShowForm(false)} style={{ flex: 1, padding: "10px 0", borderRadius: 8, border: "1px solid #e9edef", background: "transparent", color: "#667781", fontSize: 13, cursor: "pointer", fontFamily: "inherit" }}>Cancelar</button>
                <button onClick={saveUser} disabled={saving} style={{ flex: 2, padding: "10px 0", borderRadius: 8, border: "none", background: "linear-gradient(135deg,#00a884,#017561)", color: "#000", fontSize: 13, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>{saving ? "Salvando..." : (editUser ? "Salvar" : "Criar usuário")}</button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}


// ─── Onboarding Inteligente ───────────────────────────────────────────────────
function OnboardingView({ auth, aiCredits }) {
  const [step, setStep] = useState("intro"); // intro | analyzing | result | done
  const [days, setDays] = useState(90);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [editedPrompt, setEditedPrompt] = useState("");
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState("");
  const [progress, setProgress] = useState(0);

  const analyze = async () => {
    setLoading(true); setError(""); setStep("analyzing"); setProgress(0);
    
    // Simula progresso enquanto aguarda
    const progressInterval = setInterval(() => {
      setProgress(p => Math.min(p + Math.random() * 8, 90));
    }, 800);

    try {
      const r = await fetch(`${API_URL}/onboarding/analyze`, {
        method: "POST", headers,
        body: JSON.stringify({ tenant_id: TENANT_ID, days })
      });
      const d = await r.json();
      clearInterval(progressInterval);
      if (!r.ok) { setError(d.detail || "Erro na análise"); setStep("intro"); setLoading(false); return; }
      setProgress(100);
      setTimeout(() => {
        setResult(d);
        setStep("result");
      }, 500);
    } catch (e) {
      clearInterval(progressInterval);
      setError("Erro de conexão. Tente novamente.");
      setStep("intro");
    }
    setLoading(false);
  };

  const savePrompt = async () => {
    setSaving(true);
    try {
      // Prompt was already saved to DB by the analyze endpoint — just confirm
      setSaved(true);
      setTimeout(() => setStep("done"), 800);
    } catch (e) {}
    setSaving(false);
  };

  const inp = { width: "100%", padding: "9px 12px", background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 8, color: "#111b21", fontSize: 13, outline: "none", fontFamily: "inherit", boxSizing: "border-box" };

  return (
    <div style={{ flex: 1, overflowY: "auto", padding: 40 }}>
      <div style={{ maxWidth: 680, margin: "0 auto" }}>

        {/* ── INTRO ── */}
        {step === "intro" && (
          <>
            <div style={{ marginBottom: 32 }}>
              <div style={{ fontSize: 22, fontWeight: 700, marginBottom: 6 }}>🧠 Onboarding Inteligente</div>
              <div style={{ fontSize: 13, color: "#667781" }}>A IA lê seu histórico do WhatsApp e aprende como sua empresa funciona</div>
            </div>

            {/* How it works */}
            <div style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 14, padding: 24, marginBottom: 20 }}>
              <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 16 }}>Como funciona</div>
              <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
                {[
                  { n: "1", title: "Seleciona o período", desc: "Escolha quantos dias de histórico a IA vai analisar", icon: "📅" },
                  { n: "2", title: "IA analisa as conversas", desc: `Nossa IA lê até ${aiCredits?.plan === "business" ? "500" : "200"} conversas e identifica padrões do seu negócio`, icon: "🔍" },
                  { n: "3", title: "Prompt gerado automaticamente", desc: "Tom de voz, FAQ, produtos e regras da sua empresa — tudo automatico", icon: "✨" },
                  { n: "4", title: "Revise e ative", desc: "Edite se quiser e salve. Co-pilot começa a usar imediatamente", icon: "🚀" },
                ].map(s => (
                  <div key={s.n} style={{ display: "flex", gap: 14, alignItems: "flex-start" }}>
                    <div style={{ width: 32, height: 32, borderRadius: "50%", background: "#00a88420", border: "1px solid #00a88440", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14, flexShrink: 0 }}>{s.icon}</div>
                    <div>
                      <div style={{ fontSize: 13, fontWeight: 700, color: "#111b21", marginBottom: 2 }}>{s.title}</div>
                      <div style={{ fontSize: 12, color: "#667781" }}>{s.desc}</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Period selector */}
            <div style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 14, padding: 24, marginBottom: 20 }}>
              <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 6 }}>📅 Período de análise</div>
              <div style={{ fontSize: 12, color: "#667781", marginBottom: 16 }}>Mais dias = análise mais rica. Recomendamos 90 dias.</div>
              <div style={{ display: "flex", gap: 8 }}>
                {[30, 60, 90, 180].map(d => (
                  <button key={d} onClick={() => setDays(d)} style={{ flex: 1, padding: "10px 0", borderRadius: 9, border: `2px solid ${days === d ? "#00a884" : "#d1d7db"}`, background: days === d ? "#00a88415" : "#f0f2f5", color: days === d ? "#00a884" : "#667781", fontSize: 13, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>
                    {d} dias
                  </button>
                ))}
              </div>
            </div>

            {error && <div style={{ background: "#f4433315", border: "1px solid #f4433333", borderRadius: 10, padding: "12px 16px", fontSize: 13, color: "#f44336", marginBottom: 16 }}>❌ {error}</div>}

            {/* What we analyze — detailed card */}
            <div style={{ background: "linear-gradient(135deg, #00a88408, #7c4dff08)", border: "1px solid #00a88433", borderRadius: 14, padding: 20, marginBottom: 20 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
                <span style={{ fontSize: 18 }}>🔬</span>
                <span style={{ fontSize: 13, fontWeight: 800, color: "#111b21" }}>O que analisamos nos seus {days} dias de histórico</span>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginBottom: 14 }}>
                {[
                  { icon: "💬", label: "Até 300 conversas reais", sub: "com seus clientes" },
                  { icon: "📩", label: "Até 9.000 mensagens", sub: "lidas na íntegra pela IA" },
                  { icon: "🎯", label: "Tom de voz identificado", sub: "formal, descontraído, técnico..." },
                  { icon: "🛍️", label: "Produtos & serviços", sub: "mapeados automaticamente" },
                  { icon: "❓", label: "Perguntas frequentes", sub: "para o Co-pilot já saber responder" },
                  { icon: "🔄", label: "Fluxo de vendas", sub: "como você fecha e converte" },
                ].map(item => (
                  <div key={item.label} style={{ display: "flex", alignItems: "flex-start", gap: 8, padding: "8px 10px", background: "#ffffff", borderRadius: 9, border: "1px solid #e9edef" }}>
                    <span style={{ fontSize: 16, lineHeight: 1 }}>{item.icon}</span>
                    <div>
                      <div style={{ fontSize: 12, fontWeight: 700, color: "#111b21", lineHeight: 1.3 }}>{item.label}</div>
                      <div style={{ fontSize: 11, color: "#667781", marginTop: 2 }}>{item.sub}</div>
                    </div>
                  </div>
                ))}
              </div>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "10px 14px", background: "#7c4dff12", borderRadius: 9, border: "1px solid #7c4dff33" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                  <span style={{ fontSize: 14 }}>⚡</span>
                  <span style={{ fontSize: 12, color: "#7c4dff", fontWeight: 700 }}>Cada análise consome <strong>1.000 créditos</strong></span>
                </div>
                <span style={{ fontSize: 11, color: "#a78bfa", background: "#7c4dff18", padding: "2px 10px", borderRadius: 20, fontWeight: 600 }}>análise profunda</span>
              </div>
            </div>

            <button onClick={analyze} style={{ width: "100%", padding: "14px 0", borderRadius: 12, border: "none", background: "linear-gradient(135deg, #00a884, #017561)", color: "#000", fontSize: 15, fontWeight: 800, cursor: "pointer", fontFamily: "inherit" }}>
              🧠 Analisar meu histórico e gerar prompt →
            </button>
          </>
        )}

        {/* ── ANALYZING ── */}
        {step === "analyzing" && (
          <div style={{ textAlign: "center", paddingTop: 60 }}>
            <div style={{ fontSize: 48, marginBottom: 24 }}>🧠</div>
            <div style={{ fontSize: 20, fontWeight: 700, marginBottom: 8 }}>Analisando suas conversas...</div>
            <div style={{ fontSize: 13, color: "#667781", marginBottom: 40 }}>Nossa IA está lendo o histórico e aprendendo sobre seu negócio. Isso pode levar até 60 segundos.</div>
            
            {/* Progress bar */}
            <div style={{ background: "#e9edef", borderRadius: 20, height: 8, marginBottom: 12, overflow: "hidden" }}>
              <div style={{ height: "100%", borderRadius: 20, background: "linear-gradient(90deg, #00a884, #00a884)", width: `${progress}%`, transition: "width 0.8s ease" }} />
            </div>
            <div style={{ fontSize: 12, color: "#667781" }}>{Math.round(progress)}% concluído</div>

            <div style={{ marginTop: 40, display: "flex", flexDirection: "column", gap: 10, textAlign: "left", background: "#ffffff", border: "1px solid #e9edef", borderRadius: 12, padding: 20 }}>
              {[
                { label: "Buscando conversas...", done: progress > 15 },
                { label: "Lendo mensagens...", done: progress > 35 },
                { label: "Identificando padrões...", done: progress > 60 },
                { label: "Gerando prompt personalizado...", done: progress > 85 },
              ].map(s => (
                <div key={s.label} style={{ display: "flex", gap: 10, alignItems: "center", fontSize: 13, color: s.done ? "#00a884" : "#54656f" }}>
                  <span>{s.done ? "✓" : "⏳"}</span>
                  {s.label}
                </div>
              ))}
            </div>
          </div>
        )}

        {/* ── RESULT ── */}
        {step === "result" && result && (
          <>
            <div style={{ marginBottom: 24 }}>
              <div style={{ fontSize: 22, fontWeight: 700, marginBottom: 6 }}>✨ Prompt gerado!</div>
              <div style={{ fontSize: 13, color: "#667781" }}>
                Analisamos <strong style={{ color: "#00a884" }}>{result.conversations_analyzed} conversas</strong> dos últimos <strong style={{ color: "#00a884" }}>{result.days_analyzed} dias</strong>. Revise e salve.
              </div>
            </div>

            {/* Stats */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 10, marginBottom: 16 }}>
              {[
                { icon: "💬", label: "Conversas lidas", value: result.conversations_analyzed, color: "#00a884" },
                { icon: "📩", label: "Mensagens est.", value: `~${(result.conversations_analyzed * 25).toLocaleString("pt-BR")}`, color: "#00a884" },
                { icon: "📅", label: "Dias analisados", value: result.days_analyzed, color: "#7c4dff" },
                { icon: "⚡", label: "Créditos usados", value: "1.000", color: "#ff6d00" },
              ].map(s => (
                <div key={s.label} style={{ background: "#ffffff", border: `1px solid ${s.color}33`, borderRadius: 10, padding: "12px 10px", textAlign: "center" }}>
                  <div style={{ fontSize: 18, marginBottom: 4 }}>{s.icon}</div>
                  <div style={{ fontSize: 18, fontWeight: 800, color: s.color, lineHeight: 1 }}>{s.value}</div>
                  <div style={{ fontSize: 10, color: "#667781", marginTop: 4, lineHeight: 1.3 }}>{s.label}</div>
                </div>
              ))}
            </div>
            <div style={{ background: "linear-gradient(135deg, #00a88410, #7c4dff08)", border: "1px solid #00a88433", borderRadius: 10, padding: "10px 16px", marginBottom: 20, display: "flex", alignItems: "center", gap: 10 }}>
              <span style={{ fontSize: 20 }}>🏆</span>
              <div>
                <div style={{ fontSize: 12, fontWeight: 700, color: "#00a884" }}>Análise de qualidade profissional</div>
                <div style={{ fontSize: 11, color: "#667781", marginTop: 2 }}>Nossa IA leu cada mensagem, identificou padrões, mapeou produtos e aprendeu o tom do seu negócio — tudo para que o Co-pilot responda como você.</div>
              </div>
            </div>

            {/* Summary — protects real prompt */}
            <div style={{ background: "#ffffff", border: "1px solid #00a88433", borderRadius: 14, padding: 24, marginBottom: 20 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                <div style={{ fontSize: 14, fontWeight: 700 }}>🧠 O que a IA aprendeu sobre seu negócio</div>
                <span style={{ fontSize: 11, background: "#00a88422", color: "#00a884", padding: "2px 10px", borderRadius: 20, fontWeight: 700 }}>ativo</span>
              </div>
              <div style={{ fontSize: 12, color: "#667781", marginBottom: 16 }}>
                Seu Co-pilot foi configurado com base nas suas conversas. Abaixo um resumo do que ele aprendeu:
              </div>
              <div style={{ background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 10, padding: "16px 18px" }}>
                {(result.summary || "").split("\n").filter(l => l.trim()).map((line, i) => (
                  <div key={i} style={{ fontSize: 13, color: "#c8c8e0", marginBottom: 8, lineHeight: 1.5 }}>{line}</div>
                ))}
              </div>
              <div style={{ marginTop: 12, fontSize: 11, color: "#54656f", display: "flex", alignItems: "center", gap: 6 }}>
                🔒 O prompt completo é mantido de forma segura pela plataforma.
              </div>
            </div>

            <div style={{ display: "flex", gap: 10 }}>
              <button onClick={() => setStep("intro")} style={{ padding: "12px 20px", borderRadius: 10, border: "1px solid #e9edef", background: "transparent", color: "#667781", fontSize: 13, cursor: "pointer", fontFamily: "inherit" }}>← Refazer análise</button>
              <button onClick={savePrompt} disabled={saving} style={{ flex: 1, padding: "12px 0", borderRadius: 10, border: "none", background: saved ? "#00a884" : "linear-gradient(135deg,#00a884,#017561)", color: "#000", fontSize: 14, fontWeight: 800, cursor: "pointer", fontFamily: "inherit" }}>
                {saving ? "Ativando..." : saved ? "✓ Co-pilot ativado!" : "🚀 Ativar Co-pilot com este aprendizado →"}
              </button>
            </div>
          </>
        )}

        {/* ── DONE ── */}
        {step === "done" && (
          <div style={{ textAlign: "center", paddingTop: 60 }}>
            <div style={{ fontSize: 64, marginBottom: 24 }}>🎉</div>
            <div style={{ fontSize: 24, fontWeight: 800, marginBottom: 8 }}>Co-pilot configurado!</div>
            <div style={{ fontSize: 14, color: "#667781", marginBottom: 32 }}>Seu Co-pilot agora conhece sua empresa. Abra uma conversa no Inbox e clique em ✨ para ver a mágica.</div>
            <div style={{ background: "#00a88415", border: "1px solid #00a88433", borderRadius: 14, padding: 24, marginBottom: 32, textAlign: "left" }}>
              <div style={{ fontSize: 13, fontWeight: 700, color: "#00a884", marginBottom: 12 }}>O que mudou:</div>
              {["Co-pilot agora usa o prompt personalizado da sua empresa", "Sugestões de resposta muito mais precisas e no tom certo", "FAQ automático baseado nas suas perguntas reais", "Você pode refinar o prompt a qualquer momento em Configurações"].map(f => (
                <div key={f} style={{ display: "flex", gap: 8, fontSize: 13, color: "#8696a0", marginBottom: 8 }}><span style={{ color: "#00a884" }}>✓</span>{f}</div>
              ))}
            </div>
            <button onClick={() => setStep("intro")} style={{ padding: "10px 24px", borderRadius: 10, border: "1px solid #e9edef", background: "transparent", color: "#667781", fontSize: 13, cursor: "pointer", fontFamily: "inherit", marginRight: 10 }}>Refazer análise</button>
          </div>
        )}

      </div>
    </div>
  );
}


// ─── WhatsApp Connection Screen ───────────────────────────────────────────────
function WhatsAppScreen({ auth, T, theme }) {
  const [instances, setInstances] = useState([]);
  const [maxNumbers, setMaxNumbers] = useState(1);
  const [plan, setPlan] = useState("starter");
  const [activeInst, setActiveInst] = useState(null); // instance object being managed
  const [qrCode, setQrCode] = useState("");
  const [loadingQr, setLoadingQr] = useState(false);
  const [creating, setCreating] = useState(false);
  const [newLabel, setNewLabel] = useState("");
  const [showNewForm, setShowNewForm] = useState(false);
  const [deleting, setDeleting] = useState(null);
  const [confirmPhone, setConfirmPhone] = useState(null); // { inst } when showing modal

  // Sync state
  const [syncing, setSyncing] = useState(false);
  const [syncResult, setSyncResult] = useState(null);
  const [syncProgress, setSyncProgress] = useState(0);
  const [autoSyncInst, setAutoSyncInst] = useState(null); // inst that triggered auto-sync
  const [syncPhase, setSyncPhase] = useState("idle"); // idle | connecting | syncing | done | error
  const syncJobRef = useRef(null);
  const qrPollRef = useRef(null); // polling interval while QR is displayed

  const fetchInstances = async () => {
    try {
      const r = await fetch(`${API_URL}/whatsapp/tenant-instances?tenant_id=${TENANT_ID}`, { headers });
      const d = await r.json();
      setInstances(d.instances || []);
      setMaxNumbers(d.max_numbers || 1);
      setPlan(d.plan || "starter");
    } catch(e) {}
  };

  useEffect(() => {
    fetchInstances();
    const t = setInterval(fetchInstances, 12000);
    return () => clearInterval(t);
  }, []);

  // QR code for active instance — skip auto-fetch if phone already registered (soft lock)
  useEffect(() => {
    if (activeInst && !activeInst.connected && !activeInst.phone) {
      fetchQr(activeInst.instance_name);
    } else {
      setQrCode("");
    }
  }, [activeInst?.id]);

  const activeInstRef = useRef(activeInst);
  useEffect(() => { activeInstRef.current = activeInst; }, [activeInst]);

  const startAutoSync = (inst) => {
    setAutoSyncInst(inst);
    setSyncing(true); setSyncResult(null); setSyncProgress(5);
    setSyncPhase("syncing");
    fetch(`${API_URL}/whatsapp/sync`, {
      method: "POST", headers,
      body: JSON.stringify({ tenant_id: TENANT_ID, instance: inst.instance_name, async: true })
    }).then(r => r.json()).then(d => {
      if (d.job_id) {
        setSyncProgress(10);
        const poll = setInterval(async () => {
          try {
            const sr = await fetch(`${API_URL}/whatsapp/sync/status?job_id=${d.job_id}`, { headers });
            const sd = await sr.json();
            if (sd.progress) setSyncProgress(Math.min(sd.progress, 95));
            if (sd.status === "done" || sd.status === "error") {
              clearInterval(poll); setSyncProgress(100);
              setSyncResult({ ok: sd.status !== "error", ...sd });
              setSyncing(false);
              setSyncPhase(sd.status === "error" ? "error" : "done");
            }
          } catch(e) { clearInterval(poll); setSyncing(false); setSyncPhase("error"); }
        }, 2000);
        setTimeout(() => { clearInterval(poll); setSyncing(false); setSyncPhase("done"); }, 300000);
      } else {
        setSyncProgress(100); setSyncResult({ ok: true, ...d }); setSyncing(false);
        setSyncPhase("done");
      }
    }).catch(() => { setSyncing(false); setSyncPhase("error"); });
  };

  const triggerAutoSync = (instName, phone) => {
    // Número conectado — sem importação de histórico, mensagens carregam ao clicar
    setAutoSyncInst(null);
    setSyncPhase("connected_idle");
    fetchInstances();
  };

  const fetchQr = async (instName) => {
    setLoadingQr(true); setQrCode("");
    // Clear any existing QR poll
    if (qrPollRef.current) { clearInterval(qrPollRef.current); qrPollRef.current = null; }
    try {
      const r = await fetch(`${API_URL}/whatsapp/qrcode?instance=${instName}`, { headers });
      const d = await r.json();
      if (d.connected) {
        setQrCode("");
        fetchInstances();
        setActiveInst(prev => prev ? { ...prev, connected: true, phone: d.phone } : prev);
        triggerAutoSync(instName, d.phone);
      } else if (d.qr_code) {
        setQrCode(d.qr_code);
        // Poll every 4s using lightweight endpoint — just checks WORKING status
        if (qrPollRef.current) clearInterval(qrPollRef.current);
        qrPollRef.current = setInterval(async () => {
          try {
            const pr = await fetch(`${API_URL}/whatsapp/check-connected?instance=${instName}`, { headers });
            const pd = await pr.json();
            if (pd.connected) {
              clearInterval(qrPollRef.current); qrPollRef.current = null;
              setQrCode("");
              fetchInstances();
              setActiveInst(prev => prev ? { ...prev, connected: true, phone: pd.phone } : prev);
              triggerAutoSync(instName, pd.phone);
            }
          } catch(e) {}
        }, 4000);
      }
    } catch(e) {}
    setLoadingQr(false);
  };

  // Cleanup QR poll on unmount
  useEffect(() => () => { if (qrPollRef.current) clearInterval(qrPollRef.current); }, []);

  // Called when user clicks "Gerar QR Code" — soft-locks if inst already has a phone
  const handleGenerateQr = (inst) => {
    if (inst.phone) {
      setConfirmPhone({ inst, value: "" });
    } else {
      fetchQr(inst.instance_name);
    }
  };

  const createInstance = async () => {
    if (!newLabel.trim()) return;
    setCreating(true);
    try {
      const r = await fetch(`${API_URL}/whatsapp/create-instance`, {
        method: "POST", headers,
        body: JSON.stringify({ tenant_id: TENANT_ID, label: newLabel.trim() })
      });
      const d = await r.json();
      if (d.ok) {
        setShowNewForm(false); setNewLabel("");
        await fetchInstances();
        setActiveInst(d.instance);
      }
    } catch(e) {}
    setCreating(false);
  };

  const [confirmDelete, setConfirmDelete] = useState(null); // inst to delete

  const deleteInstance = async (inst) => {
    // Show our own modal instead of window.confirm
    setConfirmDelete({ inst, value: "" });
  };

  const confirmDeleteExecute = async () => {
    const inst = confirmDelete.inst;
    setConfirmDelete(null);
    setDeleting(inst.id);
    try {
      await fetch(`${API_URL}/whatsapp/delete-instance`, {
        method: "DELETE", headers,
        body: JSON.stringify({ tenant_id: TENANT_ID, instance_id: inst.id, instance_name: inst.instance_name, delete_history: true })
      });
      if (activeInst?.id === inst.id) setActiveInst(null);
      fetchInstances();
    } catch(e) {}
    setDeleting(null);
  };

  const disconnect = async (inst) => {
    if (!window.confirm(`Desconectar "${inst.label}"? O número ficará offline mas o histórico é mantido.`)) return;
    try {
      await fetch(`${API_URL}/whatsapp/disconnect`, {
        method: "POST", headers,
        body: JSON.stringify({ instance: inst.instance_name })
      });
      fetchInstances();
      setActiveInst(prev => prev?.id === inst.id ? { ...prev, connected: false, phone: "" } : prev);
    } catch(e) {}
  };

  const syncHistory = async (inst) => {
    setSyncing(true); setSyncResult(null); setSyncProgress(5);
    try {
      const r = await fetch(`${API_URL}/whatsapp/sync`, {
        method: "POST", headers,
        body: JSON.stringify({ tenant_id: TENANT_ID, instance: inst.instance_name, async: true })
      });
      const d = await r.json();
      if (d.job_id) {
        setSyncProgress(10);
        const poll = setInterval(async () => {
          try {
            const sr = await fetch(`${API_URL}/whatsapp/sync/status?job_id=${d.job_id}`, { headers });
            const sd = await sr.json();
            if (sd.progress) setSyncProgress(Math.min(sd.progress, 95));
            if (sd.status === "done" || sd.status === "error") {
              clearInterval(poll); setSyncProgress(100);
              setSyncResult({ ok: sd.status !== "error", ...sd });
              setSyncing(false);
            }
          } catch(e) { clearInterval(poll); setSyncing(false); }
        }, 2000);
        setTimeout(() => { clearInterval(poll); setSyncing(false); }, 300000);
      } else {
        setSyncProgress(100); setSyncResult({ ok: r.ok, ...d }); setSyncing(false);
      }
    } catch(e) { setSyncing(false); }
  };

  const canAdd = instances.length < maxNumbers;
  const PLAN_LABELS = { starter: "Starter", pro: "Pro", business: "Business", trial: "Trial", enterprise: "Enterprise" };

  return (
    <div style={{ flex: 1, overflowY: "auto", padding: 32 }}>
      <div style={{ maxWidth: 760, margin: "0 auto" }}>

        {/* Header */}
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 28 }}>
          <div>
            <div style={{ fontSize: 22, fontWeight: 700, marginBottom: 4 }}>📱 Números WhatsApp</div>
            <div style={{ fontSize: 13, color: "#667781" }}>
              {instances.length} de {maxNumbers} números usados · plano <span style={{ color: "#7c4dff", fontWeight: 700 }}>{PLAN_LABELS[plan]}</span>
            </div>
          </div>
          {canAdd ? (
            <button onClick={() => setShowNewForm(true)}
              style={{ padding: "9px 20px", borderRadius: 10, border: "none", background: "linear-gradient(135deg,#00a884,#017561)", color: "#000", fontSize: 13, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>
              + Adicionar número
            </button>
          ) : (
            <div style={{ padding: "9px 16px", borderRadius: 10, background: "#7c4dff15", border: "1px solid #7c4dff33", fontSize: 12, color: "#a78bfa", fontWeight: 600 }}>
              🔒 Limite atingido · <span style={{ textDecoration: "underline", cursor: "pointer" }}>Fazer upgrade</span>
            </div>
          )}
        </div>

        {/* Slot limit bar */}
        <div style={{ marginBottom: 24 }}>
          <div style={{ height: 4, background: "#e9edef", borderRadius: 4, overflow: "hidden" }}>
            <div style={{ height: "100%", width: `${(instances.length/maxNumbers)*100}%`, background: instances.length >= maxNumbers ? "#f44336" : "#00a884", borderRadius: 4, transition: "width 0.4s" }} />
          </div>
        </div>

        {/* New instance form */}
        {showNewForm && (
          <div style={{ background: "#ffffff", border: "1px solid #00a88433", borderRadius: 14, padding: 20, marginBottom: 20 }}>
            <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 12 }}>➕ Novo número</div>
            <div style={{ display: "flex", gap: 10 }}>
              <input value={newLabel} onChange={e => setNewLabel(e.target.value)}
                placeholder="Ex: Recepção, Vendas, Suporte..."
                onKeyDown={e => e.key === "Enter" && createInstance()}
                style={{ flex: 1, padding: "10px 14px", background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 9, color: "#111b21", fontSize: 13, outline: "none", fontFamily: "inherit" }} />
              <button onClick={createInstance} disabled={creating || !newLabel.trim()}
                style={{ padding: "10px 20px", borderRadius: 9, border: "none", background: creating || !newLabel.trim() ? "#e9edef" : "linear-gradient(135deg,#00a884,#017561)", color: creating || !newLabel.trim() ? "#667781" : "#000", fontSize: 13, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>
                {creating ? "Criando..." : "Criar"}
              </button>
              <button onClick={() => { setShowNewForm(false); setNewLabel(""); }}
                style={{ padding: "10px 14px", borderRadius: 9, border: "1px solid #e9edef", background: "transparent", color: "#667781", fontSize: 13, cursor: "pointer", fontFamily: "inherit" }}>
                Cancelar
              </button>
            </div>
          </div>
        )}

        {/* Instances grid */}
        {instances.length === 0 && !showNewForm && (
          <div style={{ textAlign: "center", padding: "60px 20px", background: "#ffffff", border: "1px dashed #252540", borderRadius: 16 }}>
            <div style={{ fontSize: 40, marginBottom: 12 }}>📵</div>
            <div style={{ fontSize: 15, fontWeight: 700, marginBottom: 8 }}>Nenhum número conectado</div>
            <div style={{ fontSize: 13, color: "#667781", marginBottom: 20 }}>Adicione um número de WhatsApp para começar a receber mensagens no Inbox.</div>
            <button onClick={() => setShowNewForm(true)}
              style={{ padding: "10px 24px", borderRadius: 10, border: "none", background: "linear-gradient(135deg,#00a884,#017561)", color: "#000", fontSize: 13, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>
              + Adicionar primeiro número
            </button>
          </div>
        )}

        <div style={{ display: "flex", flexDirection: "column", gap: 12, marginBottom: 24 }}>
          {instances.map(inst => {
            const isActive = activeInst?.id === inst.id;
            const statusColor = inst.connected ? "#00a884" : "#f44336";
            return (
              <div key={inst.id} style={{ background: "#ffffff", border: `1px solid ${isActive ? "#00a88444" : "#e9edef"}`, borderRadius: 14, overflow: "hidden" }}>
                {/* Instance header row */}
                <div style={{ display: "flex", alignItems: "center", gap: 14, padding: "16px 20px" }}>
                  <div style={{ width: 44, height: 44, borderRadius: 12, background: `${statusColor}15`, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 22, flexShrink: 0 }}>
                    {inst.connected ? "📱" : "📵"}
                  </div>
                  <div style={{ flex: 1 }}>
                    <div style={{ fontSize: 15, fontWeight: 700 }}>{inst.label || "Número"}</div>
                    <div style={{ display: "flex", alignItems: "center", gap: 6, marginTop: 2 }}>
                      <span style={{ width: 7, height: 7, borderRadius: "50%", background: statusColor, display: "inline-block", animation: !inst.connected ? "pulse 1.5s infinite" : "none" }} />
                      <span style={{ fontSize: 12, color: statusColor, fontWeight: 600 }}>
                        {inst.connected ? (inst.phone ? `+${inst.phone}` : "Conectado") : (inst.phone ? `Desconectado — era +${inst.phone}` : "Desconectado")}
                      </span>
                    </div>
                  </div>
                  <div style={{ display: "flex", gap: 8 }}>
                    {inst.connected && (
                      <button onClick={() => disconnect(inst)}
                        style={{ padding: "6px 14px", borderRadius: 8, border: "1px solid #f4433344", background: "#f4433315", color: "#f44336", fontSize: 12, cursor: "pointer", fontFamily: "inherit" }}>
                        Desconectar
                      </button>
                    )}
                    {!inst.connected && (
                      <button onClick={() => setActiveInst(isActive ? null : inst)}
                        style={{ padding: "6px 14px", borderRadius: 8, border: `1px solid ${isActive?"#00a88444":"#d1d7db"}`, background: isActive?"#00a88415":"transparent", color: isActive?"#00a884":"#8696a0", fontSize: 12, cursor: "pointer", fontFamily: "inherit" }}>
                        {isActive ? "▲ Fechar" : "▼ Conectar"}
                      </button>
                    )}
                    <button onClick={() => deleteInstance(inst)} disabled={deleting === inst.id}
                      style={{ padding: "6px 14px", borderRadius: 8, border: "1px solid #f4433344", background: "#f4433310", color: "#f44336", fontSize: 12, cursor: "pointer", fontFamily: "inherit", fontWeight: 600 }}>
                      {deleting === inst.id ? "..." : "🗑 Excluir"}
                    </button>
                  </div>
                </div>

                {/* Expanded panel */}
                {isActive && (() => {
                  /* ── CONNECTED: número online ── */
                  if (inst.connected) return (
                    <div style={{ borderTop: "1px solid #e9edef", padding: "28px 24px", display: "flex", flexDirection: "column", alignItems: "center", gap: 14, textAlign: "center" }}>
                      <div style={{ width: 56, height: 56, borderRadius: "50%", background: "linear-gradient(135deg,#00a884,#017561)", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 26 }}>✓</div>
                      <div style={{ fontSize: 17, fontWeight: 800, color: "#111b21" }}>Número conectado!</div>
                      <div style={{ fontSize: 13, color: "#667781", maxWidth: 320, lineHeight: 1.6 }}>
                        As mensagens chegam em tempo real.<br/>Clique em uma conversa para ver o histórico.
                      </div>
                      <button onClick={() => { setActiveInst(null); setView("inbox"); }}
                        style={{ marginTop: 8, padding: "11px 32px", borderRadius: 10, border: "none", background: "linear-gradient(135deg,#00a884,#017561)", color: "#fff", fontSize: 14, fontWeight: 800, cursor: "pointer", fontFamily: "inherit" }}>
                        → Ir para o Inbox
                      </button>
                    </div>
                  );

                  /* ── DEFAULT: QR Code panel ── */
                  return (
                    <div style={{ borderTop: "1px solid #e9edef", padding: 20 }}>
                      {!inst.connected ? (
                        /* QR Code panel */
                        <div style={{ display: "flex", gap: 32, alignItems: "flex-start", flexWrap: "wrap" }}>
                          <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 12 }}>
                            {qrCode ? (
                              <div style={{ background: "#fff", padding: 16, borderRadius: 16, boxShadow: "0 0 0 6px #00a88430, 0 0 0 10px #00a88418" }}>
                                <img src={qrCode} alt="QR Code" style={{ width: 260, height: 260, display: "block" }} />
                              </div>
                            ) : (
                              <div style={{ width: 292, height: 292, background: "#f0f2f5", border: "2px dashed #d1d7db", borderRadius: 16, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", gap: 10 }}>
                                <span style={{ fontSize: 48 }}>📷</span>
                                <span style={{ fontSize: 13, color: "#667781" }}>{loadingQr ? "Gerando QR Code..." : "Clique em Gerar QR Code"}</span>
                              </div>
                            )}
                            <button onClick={() => handleGenerateQr(inst)} disabled={loadingQr}
                              style={{ width: 292, padding: "13px 0", borderRadius: 10, border: "none", background: loadingQr ? "#e9edef" : "linear-gradient(135deg,#00a884,#017561)", color: loadingQr ? "#667781" : "#fff", fontSize: 14, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>
                              {loadingQr ? "⏳ Gerando..." : qrCode ? "🔄 Novo QR Code" : "📷 Gerar QR Code"}
                            </button>
                            {qrCode && <span style={{ fontSize: 11, color: "#ff6d00", fontWeight: 600 }}>⏱ QR Code expira em ~60 segundos</span>}
                          </div>
                          <div style={{ flex: 1, minWidth: 200 }}>
                            <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 12, color: "#54656f" }}>Como conectar:</div>
                            {["Abra o WhatsApp no celular", "Menu (⋮) → Dispositivos conectados", "Toque em Adicionar dispositivo", "Aponte a câmera para o QR Code ✅"].map((step, i) => (
                              <div key={i} style={{ display: "flex", gap: 10, alignItems: "center", marginBottom: 12 }}>
                                <div style={{ width: 26, height: 26, borderRadius: "50%", background: "#00a88420", border: "1px solid #00a88440", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 12, fontWeight: 800, color: "#00a884", flexShrink: 0 }}>{i+1}</div>
                                <span style={{ fontSize: 13, color: "#54656f" }}>{step}</span>
                              </div>
                            ))}
                            <div style={{ marginTop: 16, padding: "10px 14px", background: "#fff8e1", border: "1px solid #fcd34d", borderRadius: 10, fontSize: 12, color: "#92400e" }}>
                              💡 Após escanear, o histórico será importado automaticamente
                            </div>
                          </div>
                        </div>
                      ) : null}
                    </div>
                  );
                })()}
              </div>
            );
          })}
        </div>

        {/* Info cards */}
        <div style={{ background: T.card, border: `1px solid ${T.border}`, borderRadius: 14, padding: 20 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: "#667781", marginBottom: 12, letterSpacing: 1 }}>ℹ️ INFORMAÇÕES IMPORTANTES</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {[
              { icon: "⚠️", title: "API não oficial", desc: "Pode desconectar 2-3x por ano — basta gerar novo QR Code.", color: "#ff6d00" },
              { icon: "💾", title: "Dados seguros", desc: "Mensagens salvas mesmo quando desconectado. Nenhum dado é perdido.", color: "#00a884" },
              { icon: "⚡", title: "Reconexão rápida", desc: "Menos de 2 minutos para reconectar — só gerar novo QR Code.", color: "#00a884" },
            ].map(item => (
              <div key={item.title} style={{ display: "flex", gap: 12, padding: "10px 14px", background: "#f0f2f5", borderRadius: 10 }}>
                <span style={{ fontSize: 16, flexShrink: 0 }}>{item.icon}</span>
                <div>
                  <div style={{ fontSize: 12, fontWeight: 700, color: item.color, marginBottom: 2 }}>{item.title}</div>
                  <div style={{ fontSize: 11, color: "#667781" }}>{item.desc}</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
      <style>{`@keyframes spin { to { transform: rotate(360deg); } } @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} } @keyframes shimmer { 0%{background-position:-200% 0} 100%{background-position:200% 0} }`}</style>

      {/* ── Confirm Delete Instance Modal ────────────────────── */}
      {confirmDelete && (
        <div style={{ position: "fixed", inset: 0, background: "#000000cc", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 9999 }}
          onClick={() => setConfirmDelete(null)}>
          <div style={{ background: "#ffffff", border: "1px solid #f4433344", borderRadius: 18, padding: 32, maxWidth: 460, width: "90%" }}
            onClick={e => e.stopPropagation()}>
            <div style={{ fontSize: 36, textAlign: "center", marginBottom: 12 }}>🗑️</div>
            <div style={{ fontSize: 17, fontWeight: 800, textAlign: "center", marginBottom: 8 }}>Remover número?</div>
            <div style={{ background: "#f4433318", border: "1px solid #f4433344", borderRadius: 10, padding: "14px 16px", marginBottom: 20 }}>
              <div style={{ fontSize: 13, fontWeight: 700, color: "#f44336", marginBottom: 8 }}>🚨 Ação irreversível — os seguintes dados serão apagados:</div>
              <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
                {[
                  "📵 Número desconectado do WhatsApp",
                  "💬 Todas as conversas deste número",
                  "📩 Todo o histórico de mensagens",
                  "✅ Todas as tarefas vinculadas",
                ].map(item => (
                  <div key={item} style={{ fontSize: 12, color: "#54656f" }}>{item}</div>
                ))}
              </div>
            </div>
            <div style={{ fontSize: 13, color: "#8696a0", textAlign: "center", marginBottom: 20 }}>
              Número: <strong style={{ color: "#111b21" }}>{confirmDelete.inst.label}</strong>
              {confirmDelete.inst.phone && <span style={{ color: "#667781" }}> (+{confirmDelete.inst.phone})</span>}
            </div>
            <div style={{ display: "flex", gap: 10 }}>
              <button onClick={() => setConfirmDelete(null)}
                style={{ flex: 1, padding: "11px 0", borderRadius: 10, border: "1px solid #e9edef", background: "transparent", color: "#8696a0", fontSize: 13, cursor: "pointer", fontFamily: "inherit" }}>
                Cancelar
              </button>
              <button onClick={confirmDeleteExecute}
                style={{ flex: 1, padding: "11px 0", borderRadius: 10, border: "none", background: "linear-gradient(135deg,#f44336,#b71c1c)", color: "#fff", fontSize: 13, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>
                🗑️ Sim, remover tudo
              </button>
            </div>
          </div>
        </div>
      )}

      {/* ── Confirm Phone Modal (soft lock) ─────────────────────── */}
      {confirmPhone && (
        <div style={{ position: "fixed", inset: 0, background: "#000000cc", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 9999 }}
          onClick={() => setConfirmPhone(null)}>
          <div style={{ background: "#ffffff", border: "1px solid #f4433344", borderRadius: 18, padding: 32, maxWidth: 460, width: "90%" }}
            onClick={e => e.stopPropagation()}>
            <div style={{ fontSize: 36, textAlign: "center", marginBottom: 12 }}>⚠️</div>
            <div style={{ fontSize: 17, fontWeight: 800, textAlign: "center", marginBottom: 8 }}>Trocar número?</div>
            <div style={{ fontSize: 13, color: "#54656f", textAlign: "center", marginBottom: 6, lineHeight: 1.6 }}>
              Ao conectar um número diferente, <strong style={{ color: "#f44336" }}>todo o histórico de conversas</strong> deste número será perdido permanentemente.
            </div>
            <div style={{ background: "#f4433315", border: "1px solid #f4433333", borderRadius: 10, padding: "12px 16px", marginBottom: 20 }}>
              <div style={{ fontSize: 12, color: "#f44336", fontWeight: 700, marginBottom: 4 }}>🚨 Ação irreversível</div>
              <div style={{ fontSize: 12, color: "#667781" }}>
                Todas as <strong style={{ color: "#111b21" }}>conversas, mensagens e contatos</strong> vinculados ao número
                <strong style={{ color: "#111b21" }}> +{confirmPhone.inst.phone}</strong> serão apagados do sistema.
              </div>
            </div>
            <div style={{ marginBottom: 6 }}>
              <label style={{ fontSize: 12, fontWeight: 700, color: "#8696a0", display: "block", marginBottom: 6 }}>
                Digite o número atual para confirmar:
              </label>
              <div style={{ fontSize: 11, color: "#667781", marginBottom: 8 }}>
                Ex: {confirmPhone.inst.phone}
              </div>
              <input
                autoFocus
                value={confirmPhone.value || ""}
                onChange={e => setConfirmPhone(prev => ({ ...prev, value: e.target.value.replace(/\D/g, "") }))}
                placeholder={`${confirmPhone.inst.phone}`}
                style={{ width: "100%", padding: "10px 14px", background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 8, color: "#111b21", fontSize: 15, outline: "none", boxSizing: "border-box", letterSpacing: 2, fontFamily: "monospace" }}
              />
              {confirmPhone.value && confirmPhone.value !== String(confirmPhone.inst.phone) && (
                <div style={{ fontSize: 11, color: "#f44336", marginTop: 4 }}>❌ Número incorreto</div>
              )}
              {confirmPhone.value && confirmPhone.value === String(confirmPhone.inst.phone) && (
                <div style={{ fontSize: 11, color: "#00a884", marginTop: 4 }}>✅ Número confirmado</div>
              )}
            </div>
            <div style={{ display: "flex", gap: 10, marginTop: 20 }}>
              <button onClick={() => setConfirmPhone(null)}
                style={{ flex: 1, padding: "11px 0", borderRadius: 10, border: "1px solid #e9edef", background: "transparent", color: "#8696a0", fontSize: 13, cursor: "pointer", fontFamily: "inherit" }}>
                Cancelar
              </button>
              <button
                disabled={confirmPhone.value !== String(confirmPhone.inst.phone)}
                onClick={() => {
                  const inst = confirmPhone.inst;
                  setConfirmPhone(null);
                  fetchQr(inst.instance_name);
                }}
                style={{ flex: 1, padding: "11px 0", borderRadius: 10, border: "none", background: confirmPhone.value === String(confirmPhone.inst.phone) ? "linear-gradient(135deg,#f44336,#b71c1c)" : "#e9edef", color: confirmPhone.value === String(confirmPhone.inst.phone) ? "#fff" : "#54656f", fontSize: 13, fontWeight: 700, cursor: confirmPhone.value === String(confirmPhone.inst.phone) ? "pointer" : "not-allowed", fontFamily: "inherit", transition: "all 0.2s" }}>
                ⚠️ Sim, trocar número
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Leads Board (por etiqueta) ───────────────────────────────────────────────

// ─── Broadcasts / Disparos View ──────────────────────────────────────────────
function BroadcastsView({ conversations, labels, agents, kanbanCols, instanceFilter, instances }) {
  const [tab, setTab] = useState("new"); // new | queue | scheduled
  const [broadcasts, setBroadcasts] = useState([]);
  const [scheduledMsgs, setScheduledMsgs] = useState([]);
  const [loading, setLoading] = useState(false);

  // New broadcast form state
  const [bName, setBName] = useState("");
  const [bMessage, setBMessage] = useState("");
  const [bIntervalMin, setBIntervalMin] = useState(60);
  const [bIntervalMax, setBIntervalMax] = useState(120);
  const [bScheduledAt, setBScheduledAt] = useState("");
  const [bFilter, setBFilter] = useState("manual"); // manual | inativos | label | kanban | status | csv
  const [bInactiveDays, setBInactiveDays] = useState(7); // 3 | 7 | 15
  const [bAiPersonalize, setBAiPersonalize] = useState(false); // gerar msg IA por contato
  const [bFilterValue, setBFilterValue] = useState("");
  const [bRecipients, setBRecipients] = useState([]); // [{phone,name}]
  const [csvText, setCsvText] = useState("");
  const [creating, setCreating] = useState(false);
  const [aiObjective, setAiObjective] = useState("");
  const [loadingAI, setLoadingAI] = useState(false);

  // Scheduled form
  const [sPhone, setSPhone] = useState("");
  const [sName, setSName] = useState("");
  const [sMessage, setSMessage] = useState("");
  const [sDate, setSDate] = useState("");
  const [sRecurrence, setSRecurrence] = useState("");
  const [sConvId, setSConvId] = useState("");
  const [creatingSched, setCreatingSched] = useState(false);

  const [selectedBroadcast, setSelectedBroadcast] = useState(null);
  const [toast, setToast] = useState(null); // {msg, color}

  const showToast = (msg, color = "#00a884") => {
    setToast({ msg, color });
    setTimeout(() => setToast(null), 4000);
  };

  const fetchBroadcasts = async () => {
    setLoading(true);
    try {
      const r = await fetch(`${API_URL}/broadcasts?tenant_id=${TENANT_ID}`, { headers });
      const d = await r.json();
      setBroadcasts(d.broadcasts || []);
    } catch (e) {}
    setLoading(false);
  };
  const fetchScheduled = async () => {
    try {
      const r = await fetch(`${API_URL}/scheduled-messages?tenant_id=${TENANT_ID}`, { headers });
      const d = await r.json();
      setScheduledMsgs(d.scheduled_messages || []);
    } catch (e) {}
  };

  useEffect(() => {
    if (tab === "queue") fetchBroadcasts();
    if (tab === "scheduled") fetchScheduled();
  }, [tab]);

  // Build recipients from filter
  const buildRecipients = () => {
    if (bFilter === "manual") return bRecipients;
    if (bFilter === "csv") {
      return csvText.split("\n").map(line => {
        const [phone, name] = line.split(",").map(s => s.trim());
        return phone ? { phone: phone.replace(/\D/g, ""), name: name || "" } : null;
      }).filter(Boolean);
    }
    let convs = conversations;
    if (bFilter === "inativos") {
      const cutoff = new Date(Date.now() - bInactiveDays * 24 * 60 * 60 * 1000);
      convs = convs.filter(c => {
        if (!c.last_message_at) return false;
        const last = new Date(c.last_message_at);
        return last < cutoff && c.status !== "resolved";
      });
    }
    if (bFilter === "label" && bFilterValue) convs = convs.filter(c => (c.labels || []).some(l => l.id === bFilterValue));
    if (bFilter === "kanban" && bFilterValue) convs = convs.filter(c => c.kanban_stage === bFilterValue);
    if (bFilter === "status" && bFilterValue) convs = convs.filter(c => c.status === bFilterValue);
    return convs.map(c => ({ phone: c.contacts?.phone?.replace(/\D/g, "") || "", name: c.contacts?.name || "", contact_id: c.contact_id, conversation_id: c.id, last_message_at: c.last_message_at })).filter(r => r.phone);
  };

  const previewRecipients = buildRecipients();

  const suggestWithAI = async () => {
    if (!aiObjective.trim() || loadingAI) return;
    setLoadingAI(true);
    try {
      const r = await fetch(`${API_URL}/broadcasts/suggest-message`, { method: "POST", headers, body: JSON.stringify({ tenant_id: TENANT_ID, objective: aiObjective }) });
      const d = await r.json();
      setBMessage(d.suggestion || "");
    } catch (e) {}
    setLoadingAI(false);
  };

  const createBroadcast = async () => {
    const recs = buildRecipients();
    if (!bName.trim() || recs.length === 0 || creating) return;
    if (!bAiPersonalize && !bMessage.trim()) { alert("Digite a mensagem ou ative a personalização por IA."); return; }
    if (bIntervalMin < 60) { alert("⚠️ Intervalo mínimo é 60 segundos para evitar ban!"); return; }
    if (bAiPersonalize && recs.length > 20) {
      if (!window.confirm(`A IA vai gerar ${recs.length} mensagens únicas. Isso pode levar alguns minutos. Continuar?`)) return;
    }
    setCreating(true);
    try {
      await fetch(`${API_URL}/broadcasts`, { method: "POST", headers, body: JSON.stringify({
        tenant_id: TENANT_ID, name: bName, message: bMessage || "(IA personaliza)",
        interval_min: bIntervalMin, interval_max: bIntervalMax,
        scheduled_at: bScheduledAt || null, recipients: recs,
        ai_personalize: bAiPersonalize
      })});
      setBName(""); setBMessage(""); setBIntervalMin(60); setBIntervalMax(120); setBScheduledAt(""); setBRecipients([]); setCsvText(""); setAiObjective(""); setBAiPersonalize(false); setBInactiveDays(7);
      setTab("queue"); fetchBroadcasts();
      showToast(bAiPersonalize ? "🤖 Disparo com IA iniciado! A IA vai personalizar cada mensagem ✓" : bScheduledAt ? "📅 Disparo agendado! Veja na aba Fila ✓" : "🚀 Disparo iniciado! Acompanhe na Fila ✓");
    } catch (e) {}
    setCreating(false);
  };

  const cancelBroadcast = async (id) => {
    showToast("🚫 Cancelando disparo...", "#ff9800", true);
    await fetch(`${API_URL}/broadcasts/${id}/cancel`, { method: "PUT", headers });
    showToast("✓ Disparo cancelado", "#667781");
    fetchBroadcasts();
  };

  const createScheduled = async () => {
    if (!sPhone.trim() || !sMessage.trim() || !sDate || creatingSched) return;
    setCreatingSched(true);
    try {
      await fetch(`${API_URL}/scheduled-messages`, { method: "POST", headers, body: JSON.stringify({
        tenant_id: TENANT_ID, contact_phone: sPhone.replace(/\D/g, ""), contact_name: sName,
        message: sMessage, scheduled_at: sDate, recurrence: sRecurrence || null,
        conversation_id: sConvId || null
      })});
      setSPhone(""); setSName(""); setSMessage(""); setSDate(""); setSRecurrence(""); setSConvId("");
      await fetchScheduled();
      setTab("scheduled");
      showToast("📅 Mensagem agendada! Veja em Agendamentos ✓");
    } catch (e) {}
    setCreatingSched(false);
  };

  const deleteScheduled = async (id) => {
    showToast("🗑 Removendo...", "#667781", true);
    await fetch(`${API_URL}/scheduled-messages/${id}`, { method: "DELETE", headers });
    showToast("✓ Mensagem removida", "#667781");
    setScheduledMsgs(prev => prev.filter(m => m.id !== id));
  };

  const STATUS_COLORS = { pending: "#ffd600", sending: "#00a884", done: "#00a884", cancelled: "#667781", failed: "#f44336", scheduled: "#7c4dff" };
  const STATUS_LABELS = { pending: "⏳ Aguardando", sending: "📤 Enviando", done: "✅ Concluído", cancelled: "🚫 Cancelado", failed: "❌ Falhou", scheduled: "📅 Agendado" };
  const RECURRENCE_LABELS = { daily: "Diário", weekly: "Semanal", monthly: "Mensal" };

  const inputStyle = { width: "100%", padding: "9px 12px", background: "#ffffff", border: "1px solid #e9edef", borderRadius: 8, color: "#111b21", fontSize: 13, outline: "none", fontFamily: "inherit", boxSizing: "border-box" };
  const labelStyle = { fontSize: 11, fontWeight: 700, color: "#667781", marginBottom: 6, display: "block" };

  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden", position: "relative" }}>
      {/* Toast */}
      {toast && (
        <div style={{ position: "absolute", top: 16, left: "50%", transform: "translateX(-50%)", zIndex: 999, background: toast.color, color: "#000", padding: "12px 24px", borderRadius: 12, fontSize: 13, fontWeight: 700, boxShadow: "0 1px 3px #0000001a, 0 4px 12px #0000000f", display: "flex", alignItems: "center", gap: 10, whiteSpace: "nowrap" }}>
          {toast.msg}
          <span onClick={() => setToast(null)} style={{ cursor: "pointer", opacity: 0.6, fontSize: 16 }}>×</span>
        </div>
      )}
      {/* Header tabs */}
      <div style={{ padding: "10px 24px", borderBottom: "1px solid #e9edef", display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
        <div style={{ display: "flex", gap: 3, background: "#ffffff", border: "1px solid #e9edef", borderRadius: 9, padding: 3 }}>
          {[["new","✏️ Novo disparo"],["queue","📋 Fila"],["scheduled","📅 Agendamentos"]].map(([id, label]) => (
            <button key={id} onClick={() => setTab(id)} style={{ padding: "6px 16px", borderRadius: 7, border: "none", background: tab === id ? "#e9edef" : "transparent", color: tab === id ? "#111b21" : "#667781", fontSize: 12, fontWeight: 600, cursor: "pointer", fontFamily: "inherit" }}>{label}</button>
          ))}
        </div>
        <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginLeft: "auto" }}>
          <span style={{ background: "#f4433322", border: "1px solid #f4433344", borderRadius: 20, padding: "4px 10px", fontSize: 11, color: "#f44336", fontWeight: 600 }}>⚠️ Intervalo mín. 60s</span>
          <span style={{ background: "#ff6d0022", border: "1px solid #ff6d0044", borderRadius: 20, padding: "4px 10px", fontSize: 11, color: "#ff6d00", fontWeight: 600 }}>⚠️ Contatos sem histórico = risco de ban</span>
        </div>
      </div>

      <div style={{ flex: 1, overflowY: "auto", padding: "20px 24px" }}>

        {/* ── NEW BROADCAST ── */}
        {tab === "new" && (
          <div style={{ display: "grid", gridTemplateColumns: "1fr 380px", gap: 20, maxWidth: 1100 }}>
            {/* Left: form */}
            <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
              {/* Name */}
              <div>
                <label style={labelStyle}>NOME DO DISPARO</label>
                <input value={bName} onChange={e => setBName(e.target.value)} placeholder="Ex: Promoção de Janeiro" style={inputStyle} />
              </div>

              {/* AI message helper */}
              <div style={{ background: "#130f1f", border: "1px solid #7c4dff33", borderRadius: 12, padding: 16 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
                  <span>✨</span><span style={{ fontSize: 13, fontWeight: 700, color: "#a78bfa" }}>Sugestão de mensagem com IA</span>
                </div>
                <div style={{ display: "flex", gap: 8, marginBottom: 8 }}>
                  <input value={aiObjective} onChange={e => setAiObjective(e.target.value)} onKeyDown={e => e.key === "Enter" && suggestWithAI()} placeholder="Ex: Relembrar alunos inativos, promoção de plano anual..." style={{ ...inputStyle, flex: 1 }} />
                  <button onClick={suggestWithAI} disabled={loadingAI || !aiObjective.trim()} style={{ padding: "9px 16px", borderRadius: 8, border: "none", background: aiObjective.trim() ? "linear-gradient(135deg,#7c4dff,#5b21b6)" : "#e9edef", color: aiObjective.trim() ? "#fff" : "#667781", fontSize: 12, fontWeight: 700, cursor: "pointer", fontFamily: "inherit", flexShrink: 0 }}>{loadingAI ? "⏳" : "✨ Gerar"}</button>
                </div>
                <div style={{ fontSize: 11, color: "#667781" }}>Use {"{nome}"} para personalizar com o nome do contato</div>
              </div>

              {/* Message */}
              <div>
                <label style={labelStyle}>MENSAGEM <span style={{ color: "#667781", fontWeight: 400 }}>— use {"{nome}"} para personalizar</span></label>
                <textarea value={bMessage} onChange={e => setBMessage(e.target.value)} placeholder="Olá {nome}, temos uma novidade especial para você..." rows={5} style={{ ...inputStyle, resize: "vertical", lineHeight: 1.6 }} />
                <div style={{ display: "flex", gap: 8, marginTop: 6 }}>
                  {["{nome}", "{telefone}"].map(v => (
                    <span key={v} onClick={() => setBMessage(m => m + v)} style={{ fontSize: 11, background: "#e9edef", color: "#8696a0", padding: "2px 8px", borderRadius: 6, cursor: "pointer", fontFamily: "monospace" }}>{v}</span>
                  ))}
                </div>
              </div>

              {/* Interval config */}
              <div style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 12, padding: 16 }}>
                <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 4 }}>⏱ Intervalo entre mensagens</div>
                <div style={{ fontSize: 12, color: "#667781", marginBottom: 14 }}>Enviar de X a Y segundos entre cada mensagem. Nunca abaixo de 60s.</div>
                <div style={{ display: "flex", gap: 16, alignItems: "center" }}>
                  <div style={{ flex: 1 }}>
                    <label style={labelStyle}>MÍNIMO (segundos)</label>
                    <input type="number" min={60} max={600} value={bIntervalMin} onChange={e => setBIntervalMin(Math.max(60, parseInt(e.target.value) || 60))} style={inputStyle} />
                  </div>
                  <div style={{ color: "#667781", paddingTop: 20 }}>→</div>
                  <div style={{ flex: 1 }}>
                    <label style={labelStyle}>MÁXIMO (segundos)</label>
                    <input type="number" min={bIntervalMin} max={3600} value={bIntervalMax} onChange={e => setBIntervalMax(Math.max(bIntervalMin, parseInt(e.target.value) || 120))} style={inputStyle} />
                  </div>
                </div>
                {bIntervalMin < 60 && <div style={{ marginTop: 8, color: "#f44336", fontSize: 11, fontWeight: 600 }}>⚠️ Mínimo de 60 segundos para evitar ban no WhatsApp!</div>}
                <div style={{ marginTop: 10, fontSize: 11, color: "#667781" }}>Com {previewRecipients.length} destinatários e intervalo de ~{Math.round((bIntervalMin + bIntervalMax)/2)}s, o disparo levará ~{Math.round(previewRecipients.length * (bIntervalMin + bIntervalMax)/2 / 60)} minutos.</div>
              </div>

              {/* Schedule */}
              <div>
                <label style={labelStyle}>AGENDAR PARA (opcional — deixe vazio para enviar agora)</label>
                <input type="datetime-local" value={bScheduledAt} onChange={e => setBScheduledAt(e.target.value)} style={{ ...inputStyle, colorScheme: "dark" }} />
              </div>
            </div>

            {/* Right: recipients */}
            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              <div style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 12, padding: 16, flex: 1 }}>
                <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 12 }}>👥 Destinatários</div>

                {/* Filter selector */}
                <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginBottom: 14 }}>
                  {[["manual","✋ Manual"],["inativos","💤 Inativos"],["label","🏷 Etiqueta"],["kanban","🗂 Kanban"],["status","● Status"],["csv","📄 CSV"]].map(([id, label]) => (
                    <button key={id} onClick={() => { setBFilter(id); setBFilterValue(""); }} style={{ padding: "4px 12px", borderRadius: 20, border: `1px solid ${bFilter === id ? "#00a88444" : "#d1d7db"}`, background: bFilter === id ? "#00a88415" : "transparent", color: bFilter === id ? "#00a884" : "#667781", fontSize: 11, fontWeight: 600, cursor: "pointer", fontFamily: "inherit" }}>{label}</button>
                  ))}
                </div>

                {/* Filter content */}
                {bFilter === "label" && (
                  <select value={bFilterValue} onChange={e => setBFilterValue(e.target.value)} style={{ ...inputStyle, marginBottom: 10 }}>
                    <option value="">Selecione uma etiqueta...</option>
                    {labels.map(l => <option key={l.id} value={l.id}>{l.name}</option>)}
                  </select>
                )}
                {bFilter === "kanban" && (
                  <select value={bFilterValue} onChange={e => setBFilterValue(e.target.value)} style={{ ...inputStyle, marginBottom: 10 }}>
                    <option value="">Selecione coluna do Kanban...</option>
                    {kanbanCols.map(c => <option key={c.id} value={c.id}>{c.label}</option>)}
                  </select>
                )}
                {bFilter === "status" && (
                  <select value={bFilterValue} onChange={e => setBFilterValue(e.target.value)} style={{ ...inputStyle, marginBottom: 10 }}>
                    <option value="">Selecione status...</option>
                    <option value="open">Abertos</option>
                    <option value="pending">Pendentes</option>
                    <option value="resolved">Resolvidos</option>
                  </select>
                )}
                {bFilter === "inativos" && (
                  <div style={{ marginBottom: 10 }}>
                    <div style={{ fontSize: 11, fontWeight: 700, color: "#667781", marginBottom: 8 }}>SEM CONTATO HÁ:</div>
                    <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
                      {[[3,"3 dias"],[7,"7 dias"],[15,"15+ dias"]].map(([d, label]) => (
                        <button key={d} onClick={() => setBInactiveDays(d)}
                          style={{ flex: 1, padding: "8px 0", borderRadius: 8, border: `1px solid ${bInactiveDays === d ? "#00a88444" : "#d1d7db"}`,
                            background: bInactiveDays === d ? "#00a88415" : "transparent",
                            color: bInactiveDays === d ? "#00a884" : "#667781",
                            fontSize: 12, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>
                          {label}
                        </button>
                      ))}
                    </div>
                    <div onClick={() => setBAiPersonalize(p => !p)}
                      style={{ display: "flex", alignItems: "center", gap: 10, padding: "12px 14px",
                        background: bAiPersonalize ? "#7c4dff15" : "#f0f2f5",
                        border: `1px solid ${bAiPersonalize ? "#7c4dff44" : "transparent"}`,
                        borderRadius: 10, cursor: "pointer", marginBottom: 4 }}>
                      <div style={{ width: 36, height: 20, borderRadius: 10,
                        background: bAiPersonalize ? "#7c4dff" : "#d1d7db",
                        position: "relative", transition: "background 0.2s", flexShrink: 0 }}>
                        <div style={{ width: 16, height: 16, borderRadius: "50%", background: "#fff",
                          position: "absolute", top: 2, left: bAiPersonalize ? 18 : 2, transition: "left 0.2s" }} />
                      </div>
                      <div>
                        <div style={{ fontSize: 12, fontWeight: 700, color: bAiPersonalize ? "#7c4dff" : "#111b21" }}>
                          ✨ IA personaliza mensagem por contato
                        </div>
                        <div style={{ fontSize: 11, color: "#667781" }}>
                          A IA vai ler o histórico de cada conversa e gerar uma mensagem única usando o prompt da sua empresa
                        </div>
                      </div>
                    </div>
                    {bAiPersonalize && (
                      <div style={{ background: "#7c4dff10", border: "1px solid #7c4dff22", borderRadius: 8, padding: "8px 12px", fontSize: 11, color: "#7c4dff" }}>
                        🤖 O campo "Mensagem" será ignorado. A IA vai gerar uma mensagem personalizada para cada contato antes de enviar.
                      </div>
                    )}
                  </div>
                )}
                {bFilter === "csv" && (
                  <div style={{ marginBottom: 10 }}>
                    <div style={{ fontSize: 11, color: "#667781", marginBottom: 6 }}>Cole aqui: <code style={{ color: "#8696a0" }}>55119999999, Nome</code> (um por linha)</div>
                    <textarea value={csvText} onChange={e => setCsvText(e.target.value)} rows={5} placeholder={"5511999999999, João Silva\n5511888888888, Maria"} style={{ ...inputStyle, resize: "vertical", fontFamily: "monospace", fontSize: 12 }} />
                  </div>
                )}
                {bFilter === "manual" && (
                  <div style={{ marginBottom: 10 }}>
                    <div style={{ fontSize: 11, color: "#667781", marginBottom: 8 }}>Selecione conversas:</div>
                    <div style={{ maxHeight: 200, overflowY: "auto", display: "flex", flexDirection: "column", gap: 4 }}>
                      {conversations.filter(c => c.contacts?.phone).map(conv => {
                        const checked = bRecipients.some(r => r.phone === conv.contacts.phone?.replace(/\D/g,""));
                        return (
                          <div key={conv.id} onClick={() => {
                            const phone = conv.contacts.phone?.replace(/\D/g,"");
                            const name = conv.contacts.name || "";
                            setBRecipients(prev => checked ? prev.filter(r => r.phone !== phone) : [...prev, { phone, name }]);
                          }} style={{ display: "flex", alignItems: "center", gap: 8, padding: "7px 10px", borderRadius: 8, cursor: "pointer", background: checked ? "#00a88410" : "transparent", border: `1px solid ${checked ? "#00a88433" : "transparent"}` }}>
                            <div style={{ width: 16, height: 16, borderRadius: 4, border: `2px solid ${checked ? "#00a884" : "#54656f"}`, background: checked ? "#00a884" : "transparent", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>{checked && <span style={{ color: "#000", fontSize: 10, fontWeight: 900 }}>✓</span>}</div>
                            <Avatar name={displayName(conv.contacts.name, conv.contacts.phone)} size={20} phone={conv.contacts.phone} instanceFilter={instanceFilter} />
                            <div style={{ flex: 1, minWidth: 0 }}>
                              <div style={{ fontSize: 12, fontWeight: 600, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{displayName(conv.contacts.name, conv.contacts.phone)}</div>
                              <div style={{ fontSize: 10, color: "#667781" }}>{formatPhone(conv.contacts.phone)}</div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}

                {/* Recipients preview */}
                <div style={{ padding: "10px 12px", background: "#f0f2f5", borderRadius: 8, marginBottom: 14 }}>
                  <div style={{ fontSize: 12, fontWeight: 700, color: previewRecipients.length > 0 ? "#00a884" : "#667781" }}>
                    {previewRecipients.length > 0 ? `✓ ${previewRecipients.length} destinatário${previewRecipients.length !== 1 ? "s" : ""} selecionado${previewRecipients.length !== 1 ? "s" : ""}` : "Nenhum destinatário selecionado"}
                  </div>
                  {previewRecipients.slice(0, 3).map((r, i) => (
                    <div key={i} style={{ fontSize: 11, color: "#667781", marginTop: 3 }}>• {r.name || r.phone}</div>
                  ))}
                  {previewRecipients.length > 3 && <div style={{ fontSize: 11, color: "#667781", marginTop: 3 }}>... e mais {previewRecipients.length - 3}</div>}
                </div>

                <button
                  onClick={createBroadcast}
                  disabled={creating || !bName.trim() || (!bAiPersonalize && !bMessage.trim()) || previewRecipients.length === 0}
                  style={{ width: "100%", padding: "11px 0", borderRadius: 9, border: "none", background: (!creating && bName.trim() && (bAiPersonalize || bMessage.trim()) && previewRecipients.length > 0) ? (bAiPersonalize ? "linear-gradient(135deg,#7c4dff,#5e35b1)" : "linear-gradient(135deg,#00a884,#017561)") : "#e9edef", color: (!creating && bName.trim() && (bAiPersonalize || bMessage.trim()) && previewRecipients.length > 0) ? "#fff" : "#667781", fontSize: 14, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}
                >{creating ? (bAiPersonalize ? "🤖 Gerando mensagens..." : "Criando...") : bAiPersonalize ? `✨ Iniciar disparo com IA` : bScheduledAt ? `📅 Agendar disparo` : `🚀 Iniciar disparo agora`}</button>
              </div>
            </div>
          </div>
        )}

        {/* ── QUEUE ── */}
        {tab === "queue" && (
          <div style={{ maxWidth: 900 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
              <span style={{ fontSize: 13, fontWeight: 700 }}>Histórico de disparos</span>
              <button onClick={fetchBroadcasts} style={{ padding: "5px 14px", borderRadius: 7, border: "1px solid #e9edef", background: "transparent", color: "#667781", fontSize: 12, cursor: "pointer", fontFamily: "inherit" }}>↻ Atualizar</button>
            </div>
            {loading ? <div style={{ textAlign: "center", color: "#667781", padding: 40 }}>Carregando...</div>
              : broadcasts.length === 0 ? (
                <div style={{ textAlign: "center", padding: 60 }}>
                  <div style={{ fontSize: 40, marginBottom: 12 }}>📢</div>
                  <div style={{ fontSize: 15, fontWeight: 700, color: "#667781" }}>Nenhum disparo ainda</div>
                  <div style={{ fontSize: 12, color: "#54656f", marginTop: 4 }}>Crie seu primeiro disparo na aba "Novo disparo"</div>
                </div>
              ) : broadcasts.map(b => {
                const pct = b.total_recipients > 0 ? Math.round((b.sent_count / b.total_recipients) * 100) : 0;
                const color = STATUS_COLORS[b.status] || "#667781";
                return (
                  <div key={b.id} style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 12, padding: "16px 20px", marginBottom: 12 }}>
                    <div style={{ display: "flex", alignItems: "flex-start", gap: 12, marginBottom: 10 }}>
                      <div style={{ flex: 1 }}>
                        <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 4 }}>{b.name}</div>
                        <div style={{ fontSize: 12, color: "#667781", lineHeight: 1.5, overflow: "hidden", display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical" }}>{b.message}</div>
                      </div>
                      <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 6, flexShrink: 0 }}>
                        <span style={{ background: color + "22", color, fontSize: 11, fontWeight: 700, padding: "3px 10px", borderRadius: 20 }}>{STATUS_LABELS[b.status]}</span>
                        {(b.status === "pending" || b.status === "sending" || b.status === "scheduled") && (
                          <button onClick={() => cancelBroadcast(b.id)} style={{ padding: "3px 10px", borderRadius: 6, border: "1px solid #f4433344", background: "transparent", color: "#f44336", fontSize: 11, cursor: "pointer", fontFamily: "inherit" }}>🚫 Cancelar</button>
                        )}
                      </div>
                    </div>
                    <div style={{ background: "#f0f2f5", borderRadius: 8, overflow: "hidden", height: 6, marginBottom: 8 }}>
                      <div style={{ height: "100%", background: `linear-gradient(90deg, ${color}, ${color}88)`, width: `${pct}%`, transition: "width 0.5s" }} />
                    </div>
                    <div style={{ display: "flex", gap: 16, fontSize: 11, color: "#667781" }}>
                      <span>📤 {b.sent_count}/{b.total_recipients} enviados</span>
                      {b.failed_count > 0 && <span style={{ color: "#f44336" }}>❌ {b.failed_count} falharam</span>}
                      <span>⏱ {b.interval_min}-{b.interval_max}s entre msgs</span>
                      {b.scheduled_at && <span>📅 {new Date(b.scheduled_at).toLocaleString("pt-BR")}</span>}
                      {b.finished_at && <span>✓ {new Date(b.finished_at).toLocaleString("pt-BR")}</span>}
                    </div>
                  </div>
                );
              })}
          </div>
        )}

        {/* ── SCHEDULED ── */}
        {tab === "scheduled" && (
          <div style={{ display: "grid", gridTemplateColumns: "1fr 340px", gap: 20, maxWidth: 1000 }}>
            {/* List */}
            <div>
              <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 16 }}>
                <span style={{ fontSize: 13, fontWeight: 700 }}>Mensagens agendadas</span>
                <button onClick={fetchScheduled} style={{ padding: "5px 14px", borderRadius: 7, border: "1px solid #e9edef", background: "transparent", color: "#667781", fontSize: 12, cursor: "pointer", fontFamily: "inherit" }}>↻</button>
              </div>
              {scheduledMsgs.length === 0 ? (
                <div style={{ textAlign: "center", padding: 40 }}>
                  <div style={{ fontSize: 36, marginBottom: 10 }}>📅</div>
                  <div style={{ fontSize: 14, color: "#667781" }}>Nenhum agendamento ainda</div>
                </div>
              ) : scheduledMsgs.map(m => {
                const isPast = new Date(m.scheduled_at) < new Date();
                return (
                  <div key={m.id} style={{ background: "#ffffff", border: `1px solid ${isPast && m.status === "pending" ? "#f4433633" : "#e9edef"}`, borderRadius: 10, padding: "13px 16px", marginBottom: 10 }}>
                    <div style={{ display: "flex", gap: 10, alignItems: "flex-start" }}>
                      <div style={{ flex: 1 }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 5 }}>
                          <Avatar name={m.contact_name || m.contact_phone} size={24} />
                          <span style={{ fontSize: 13, fontWeight: 600 }}>{m.contact_name || m.contact_phone}</span>
                          <span style={{ fontSize: 11, color: "#667781" }}>{m.contact_phone}</span>
                          {m.recurrence && <span style={{ fontSize: 10, background: "#7c4dff22", color: "#a78bfa", padding: "1px 7px", borderRadius: 10 }}>🔁 {RECURRENCE_LABELS[m.recurrence]}</span>}
                        </div>
                        <div style={{ fontSize: 12, color: "#8696a0", marginBottom: 6, lineHeight: 1.5 }}>{m.message}</div>
                        <div style={{ fontSize: 11, color: isPast && m.status === "pending" ? "#f44336" : "#667781" }}>
                          📅 {new Date(m.scheduled_at).toLocaleString("pt-BR")}
                          {isPast && m.status === "pending" && " — VENCIDA"}
                        </div>
                      </div>
                      <button onClick={() => deleteScheduled(m.id)} style={{ padding: "4px 10px", borderRadius: 6, border: "1px solid #33333344", background: "transparent", color: "#667781", fontSize: 12, cursor: "pointer", fontFamily: "inherit", flexShrink: 0 }}>🗑</button>
                    </div>
                  </div>
                );
              })}
            </div>

            {/* New scheduled form */}
            <div style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 12, padding: 18, alignSelf: "flex-start" }}>
              <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 14 }}>+ Novo agendamento</div>
              <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                <div>
                  <label style={labelStyle}>CONVERSA (opcional)</label>
                  <select value={sConvId} onChange={e => {
                    setSConvId(e.target.value);
                    const conv = conversations.find(c => c.id === e.target.value);
                    if (conv) { setSPhone(conv.contacts?.phone || ""); setSName(conv.contacts?.name || ""); }
                  }} style={{ ...inputStyle }}>
                    <option value="">Selecione ou preencha manualmente...</option>
                    {conversations.map(c => <option key={c.id} value={c.id}>{c.contacts?.name || c.contacts?.phone}</option>)}
                  </select>
                </div>
                <div>
                  <label style={labelStyle}>TELEFONE *</label>
                  <input value={sPhone} onChange={e => setSPhone(e.target.value)} placeholder="5511999999999" style={inputStyle} />
                </div>
                <div>
                  <label style={labelStyle}>NOME</label>
                  <input value={sName} onChange={e => setSName(e.target.value)} placeholder="Nome do contato" style={inputStyle} />
                </div>
                <div>
                  <label style={labelStyle}>MENSAGEM *</label>
                  <textarea value={sMessage} onChange={e => setSMessage(e.target.value)} rows={3} placeholder="Sua mensagem..." style={{ ...inputStyle, resize: "none", lineHeight: 1.5 }} />
                </div>
                <div>
                  <label style={labelStyle}>DATA E HORA *</label>
                  <input type="datetime-local" value={sDate} onChange={e => setSDate(e.target.value)} style={{ ...inputStyle, colorScheme: "dark" }} />
                </div>
                <div>
                  <label style={labelStyle}>RECORRÊNCIA</label>
                  <select value={sRecurrence} onChange={e => setSRecurrence(e.target.value)} style={inputStyle}>
                    <option value="">Sem recorrência (envio único)</option>
                    <option value="daily">🔁 Diário</option>
                    <option value="weekly">🔁 Semanal</option>
                    <option value="monthly">🔁 Mensal</option>
                  </select>
                </div>
                <button onClick={createScheduled} disabled={creatingSched || !sPhone.trim() || !sMessage.trim() || !sDate} style={{ padding: "10px 0", borderRadius: 8, border: "none", background: (!creatingSched && sPhone.trim() && sMessage.trim() && sDate) ? "linear-gradient(135deg,#00a884,#017561)" : "#e9edef", color: (!creatingSched && sPhone.trim() && sMessage.trim() && sDate) ? "#000" : "#667781", fontSize: 13, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>{creatingSched ? "Salvando..." : "📅 Agendar mensagem"}</button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// ─── Global Tasks View ───────────────────────────────────────────────────────
function GlobalTasksView({ pendingTasksMap, conversations, agents, onSelectConv, onRefresh }) {
  const [tab, setTab] = useState("open"); // "open" | "done"
  const [openTasks, setOpenTasks] = useState([]);
  const [doneTasks, setDoneTasks] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selectedTask, setSelectedTask] = useState(null);
  const [filterAgent, setFilterAgent] = useState("");
  const [filterOverdue, setFilterOverdue] = useState(false);

  const fetchOpen = async () => {
    try {
      const r = await fetch(`${API_URL}/tasks?tenant_id=${TENANT_ID}`, { headers });
      const d = await r.json();
      setOpenTasks((d.tasks || []).filter(t => !t.done));
    } catch (e) {}
  };
  const fetchDone = async () => {
    try {
      const r = await fetch(`${API_URL}/tasks/completed?tenant_id=${TENANT_ID}&days=7`, { headers });
      const d = await r.json();
      setDoneTasks(d.tasks || []);
    } catch (e) {}
  };
  const fetchAll = async () => {
    setLoading(true);
    await Promise.all([fetchOpen(), fetchDone()]);
    setLoading(false);
  };
  useEffect(() => { fetchAll(); }, []);

  const [completing, setCompleting] = useState(null); // taskId being completed
  const completeTask = async (taskId) => {
    setCompleting(taskId);
    try {
      await fetch(`${API_URL}/tasks/${taskId}/done`, { method: "PUT", headers });
      const done = openTasks.find(t => t.id === taskId);
      setOpenTasks(prev => prev.filter(t => t.id !== taskId));
      if (done) setDoneTasks(prev => [{ ...done, done: true, done_at: new Date().toISOString() }, ...prev]);
      setSelectedTask(null);
      if (onRefresh) onRefresh();
    } catch (e) {}
    setCompleting(null);
  };

  const getConv = (convId) => conversations.find(c => c.id === convId) || null;
  const isOverdue = (due) => due && new Date(due) < new Date();
  const overdueCount = openTasks.filter(t => isOverdue(t.due_at)).length;

  const filteredOpen = openTasks
    .filter(t => !filterAgent || t.assigned_to === filterAgent)
    .filter(t => !filterOverdue || isOverdue(t.due_at))
    .sort((a, b) => {
      const aOver = isOverdue(a.due_at) ? 0 : 1;
      const bOver = isOverdue(b.due_at) ? 0 : 1;
      if (aOver !== bOver) return aOver - bOver;
      if (a.due_at && b.due_at) return new Date(a.due_at) - new Date(b.due_at);
      return 0;
    });

  const filteredDone = doneTasks
    .filter(t => !filterAgent || t.assigned_to === filterAgent);

  const renderCard = (task, isDone) => {
    const conv = getConv(task.conversation_id);
    const overdue = !isDone && isOverdue(task.due_at);
    const assignedName = task.users?.name || agents.find(a => a.id === task.assigned_to)?.name;
    const contactName = conv?.contacts?.name || conv?.contacts?.phone || task.conversations?.contacts?.name || task.conversations?.contacts?.phone;
    const contactPhone = conv?.contacts?.phone || task.conversations?.contacts?.phone;
    return (
      <div
        key={task.id}
        onClick={() => !isDone && setSelectedTask(task)}
        style={{ background: isDone ? "#f0f2f5" : "#ffffff", border: `1px solid ${isDone ? "#e9edef" : overdue ? "#f4433633" : "#e9edef"}`, borderRadius: 12, padding: "14px 16px", cursor: isDone ? "default" : "pointer", transition: "all 0.15s", position: "relative", opacity: isDone ? 0.7 : 1 }}
        onMouseEnter={e => { if (!isDone) { e.currentTarget.style.borderColor = overdue ? "#f4433666" : "#d1d7db"; e.currentTarget.style.background = "#f0f2f5"; }}}
        onMouseLeave={e => { if (!isDone) { e.currentTarget.style.borderColor = overdue ? "#f4433633" : "#e9edef"; e.currentTarget.style.background = "#ffffff"; }}}
      >
        {/* Status badge */}
        {isDone
          ? <div style={{ position: "absolute", top: 10, right: 12, background: "#00a88422", color: "#00a884", fontSize: 10, fontWeight: 700, padding: "2px 7px", borderRadius: 10 }}>✓ CONCLUÍDA</div>
          : overdue && <div style={{ position: "absolute", top: 10, right: 12, background: "#f4433322", color: "#f44336", fontSize: 10, fontWeight: 700, padding: "2px 7px", borderRadius: 10 }}>⚠ VENCIDA</div>
        }
        <div style={{ fontSize: 13, fontWeight: 700, color: isDone ? "#8696a0" : "#111b21", marginBottom: 6, paddingRight: 70, textDecoration: isDone ? "line-through" : "none" }}>{task.title}</div>
        {task.description && <div style={{ fontSize: 12, color: "#667781", marginBottom: 10, lineHeight: 1.5, overflow: "hidden", display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical" }}>{task.description}</div>}
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 10 }}>
          {isDone && task.done_at && <span style={{ fontSize: 11, color: "#00a884" }}>✓ {new Date(task.done_at).toLocaleDateString("pt-BR", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" })}</span>}
          {!isDone && task.due_at && <span style={{ fontSize: 11, color: overdue ? "#f44336" : "#8696a0" }}>📅 {new Date(task.due_at).toLocaleDateString("pt-BR", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" })}</span>}
          {assignedName && <span style={{ fontSize: 11, color: isDone ? "#667781" : "#00a884" }}>👤 {assignedName}</span>}
        </div>
        {(conv || contactName) && (
          <div
            onClick={e => { e.stopPropagation(); if (conv) onSelectConv(conv); }}
            style={{ display: "flex", alignItems: "center", gap: 8, padding: "7px 10px", background: "#e9edef", borderRadius: 8, cursor: conv ? "pointer" : "default" }}
            onMouseEnter={e => { if (conv) e.currentTarget.style.background = "#d1d7db"; }}
            onMouseLeave={e => { if (conv) e.currentTarget.style.background = "#e9edef"; }}
          >
            <Avatar name={contactName} size={20} />
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontSize: 11, fontWeight: 600, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", color: "#54656f" }}>{contactName}</div>
              <div style={{ fontSize: 10, color: "#667781" }}>{contactPhone}</div>
            </div>
            {conv && <span style={{ fontSize: 10, color: "#667781", flexShrink: 0 }}>→ ver conversa</span>}
          </div>
        )}
        {!isDone && <div style={{ marginTop: 8, fontSize: 10, color: "#667781" }}>Clique para ver detalhes →</div>}
      </div>
    );
  };

  return (
    <>
      <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
        {/* Header */}
        <div style={{ padding: "12px 24px", borderBottom: "1px solid #e9edef", display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
          {/* Sub-tabs */}
          <div style={{ display: "flex", gap: 4, background: "#ffffff", borderRadius: 8, padding: 3, border: "1px solid #e9edef" }}>
            <button onClick={() => setTab("open")} style={{ padding: "5px 14px", borderRadius: 6, border: "none", background: tab === "open" ? "#e9edef" : "transparent", color: tab === "open" ? "#111b21" : "#667781", fontSize: 12, fontWeight: 600, cursor: "pointer", fontFamily: "inherit", display: "flex", alignItems: "center", gap: 6 }}>
              ⏳ Em aberto
              {openTasks.length > 0 && <span style={{ background: overdueCount > 0 ? "#f44336" : "#00a884", color: "#000", fontSize: 10, fontWeight: 800, padding: "1px 6px", borderRadius: 10 }}>{openTasks.length}</span>}
            </button>
            <button onClick={() => { setTab("done"); fetchDone(); }} style={{ padding: "5px 14px", borderRadius: 6, border: "none", background: tab === "done" ? "#e9edef" : "transparent", color: tab === "done" ? "#111b21" : "#667781", fontSize: 12, fontWeight: 600, cursor: "pointer", fontFamily: "inherit", display: "flex", alignItems: "center", gap: 6 }}>
              ✅ Concluídas
              {doneTasks.length > 0 && <span style={{ background: "#25254060", color: "#667781", fontSize: 10, fontWeight: 800, padding: "1px 6px", borderRadius: 10 }}>{doneTasks.length}</span>}
            </button>
          </div>
          {tab === "open" && overdueCount > 0 && <span style={{ background: "#f4433322", color: "#f44336", fontSize: 11, fontWeight: 700, padding: "2px 10px", borderRadius: 20 }}>⚠ {overdueCount} vencida{overdueCount > 1 ? "s" : ""}</span>}
          {tab === "done" && <span style={{ fontSize: 11, color: "#667781" }}>Últimos 7 dias</span>}
          <div style={{ marginLeft: "auto", display: "flex", gap: 8, alignItems: "center" }}>
            {tab === "open" && <button onClick={() => setFilterOverdue(f => !f)} style={{ padding: "5px 12px", borderRadius: 7, border: `1px solid ${filterOverdue ? "#f4433344" : "#d1d7db"}`, background: filterOverdue ? "#f4433315" : "transparent", color: filterOverdue ? "#f44336" : "#667781", fontSize: 12, fontWeight: 600, cursor: "pointer", fontFamily: "inherit" }}>⚠ Vencidas</button>}
            <select value={filterAgent} onChange={e => setFilterAgent(e.target.value)} style={{ padding: "5px 10px", background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 7, color: filterAgent ? "#111b21" : "#667781", fontSize: 12, outline: "none", fontFamily: "inherit", cursor: "pointer" }}>
              <option value="">Todos os atendentes</option>
              {agents.map(a => <option key={a.id} value={a.id}>{a.name}</option>)}
            </select>
            <button onClick={fetchAll} style={{ padding: "5px 12px", borderRadius: 7, border: "1px solid #e9edef", background: "transparent", color: "#667781", fontSize: 12, cursor: "pointer", fontFamily: "inherit" }}>↻</button>
          </div>
        </div>

        {/* Content */}
        <div style={{ flex: 1, overflowY: "auto", padding: "20px 24px" }}>
          {loading ? (
            <div style={{ textAlign: "center", color: "#667781", padding: 40 }}>Carregando...</div>
          ) : tab === "open" ? (
            filteredOpen.length === 0 ? (
              <div style={{ textAlign: "center", padding: 60 }}>
                <div style={{ fontSize: 48, marginBottom: 12 }}>🎉</div>
                <div style={{ fontSize: 16, fontWeight: 700, color: "#667781", marginBottom: 6 }}>Nenhuma tarefa pendente!</div>
                <div style={{ fontSize: 13, color: "#54656f" }}>Todas as tarefas foram concluídas.</div>
              </div>
            ) : (
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))", gap: 12 }}>
                {filteredOpen.map(t => renderCard(t, false))}
              </div>
            )
          ) : (
            filteredDone.length === 0 ? (
              <div style={{ textAlign: "center", padding: 60 }}>
                <div style={{ fontSize: 48, marginBottom: 12 }}>📋</div>
                <div style={{ fontSize: 16, fontWeight: 700, color: "#667781", marginBottom: 6 }}>Nenhuma tarefa concluída</div>
                <div style={{ fontSize: 13, color: "#54656f" }}>Nos últimos 7 dias ainda não há registros.</div>
              </div>
            ) : (
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))", gap: 12 }}>
                {filteredDone.map(t => renderCard(t, true))}
              </div>
            )
          )}
        </div>
      </div>
      {selectedTask && (
        <TaskDetailModal
          task={selectedTask}
          agents={agents}
          onClose={() => setSelectedTask(null)}
          onComplete={completeTask}
        />
      )}
    </>
  );
}

function LeadsBoard({ conversations, kanbanCols, labels, onSelectConv, onManageLabels, onMoveLabel, instanceFilter, instances }) {
  const [dragging, setDragging] = useState(null);       // { convId, fromLabelId }
  const [dragOver, setDragOver] = useState(null);       // label.id or "unlabeled"

  const unlabeled = conversations.filter(c => !c.labels || c.labels.length === 0);

  const allLabelIds = new Set(labels.map(l => l.id));
  const extraLabels = [];
  conversations.forEach(conv => {
    (conv.labels || []).forEach(l => {
      if (!allLabelIds.has(l.id)) { allLabelIds.add(l.id); extraLabels.push(l); }
    });
  });
  const allLabels = [...labels, ...extraLabels];

  const handleDrop = (targetLabelId) => {
    if (!dragging) return;
    const conv = conversations.find(c => c.id === dragging.convId);
    if (conv) onMoveLabel(conv, dragging.fromLabelId, targetLabelId === "unlabeled" ? null : allLabels.find(l => l.id === targetLabelId));
    setDragging(null);
    setDragOver(null);
  };

  const renderCard = (conv, colLabel) => (
    <div
      key={conv.id}
      draggable
      onDragStart={(e) => { e.stopPropagation(); setDragging({ convId: conv.id, fromLabelId: colLabel?.id || null }); }}
      onDragEnd={() => { setDragging(null); setDragOver(null); }}
      onClick={() => !dragging && onSelectConv(conv)}
      style={{ background: "#f0f2f5", border: `1px solid ${dragging?.convId === conv.id ? (colLabel?.color || "#667781") + "55" : "#d1d7db"}`, borderRadius: 10, padding: "11px 13px", cursor: "grab", opacity: dragging?.convId === conv.id ? 0.4 : 1, transition: "border-color 0.15s" }}
      onMouseEnter={e => { if (dragging?.convId !== conv.id) e.currentTarget.style.borderColor = (colLabel?.color || "#667781") + "55"; }}
      onMouseLeave={e => { if (dragging?.convId !== conv.id) e.currentTarget.style.borderColor = "#d1d7db"; }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
        <Avatar name={displayName(conv.contacts?.name, conv.contacts?.phone)} size={26} phone={conv.contacts?.phone} instanceFilter={instanceFilter} />
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 12, fontWeight: 600, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{displayName(conv.contacts?.name, conv.contacts?.phone)}</div>
          <div style={{ fontSize: 11, color: "#667781" }}>{formatPhone(conv.contacts?.phone)}</div>
        </div>
        <span style={{ fontSize: 10, color: "#667781", flexShrink: 0 }}>{timeAgo(conv.last_message_at)}</span>
      </div>
      <div style={{ display: "flex", gap: 4, flexWrap: "wrap", marginBottom: 4 }}>
        {(conv.labels || []).filter(l => l.id !== colLabel?.id).map(l => <LabelChip key={l.id} label={l} />)}
        <KanbanBadge stage={conv.kanban_stage} columns={kanbanCols} />
      </div>
      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        <StatusDot status={conv.status} />
        <span style={{ fontSize: 10, color: "#667781" }}>{conv.status === "open" ? "Aberto" : conv.status === "pending" ? "Pendente" : "Resolvido"}</span>
        {conv.assigned_agent && <span style={{ fontSize: 10, color: "#667781", marginLeft: "auto" }}>👤 {conv.assigned_agent}</span>}
      </div>
    </div>
  );

  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <div style={{ padding: "14px 24px", borderBottom: "1px solid #e9edef", display: "flex", alignItems: "center", gap: 12 }}>
        <span style={{ fontSize: 15, fontWeight: 700 }}>🏷 Leads por Etiqueta</span>
        <span style={{ fontSize: 12, color: "#667781" }}>Arraste para mover entre etiquetas</span>
        {instanceFilter && instances && (() => {
          const inst = instances.find(i => i.instance_name === instanceFilter);
          return inst ? (
            <div style={{ display: "flex", alignItems: "center", gap: 5, padding: "3px 10px", borderRadius: 20, background: "#00a88415", border: "1px solid #00a88430" }}>
              <span style={{ width: 6, height: 6, borderRadius: "50%", background: inst.connected ? "#00a884" : "#f44336", display: "inline-block" }} />
              <span style={{ fontSize: 11, fontWeight: 700, color: "#00a884" }}>📱 {inst.label || inst.instance_name}</span>
            </div>
          ) : null;
        })()}
        <div style={{ marginLeft: "auto", display: "flex", gap: 8, alignItems: "center" }}>
          <span style={{ fontSize: 12, color: "#667781", background: "#e9edef", padding: "4px 12px", borderRadius: 20 }}>{conversations.length} total</span>
          <button onClick={onManageLabels} style={{ padding: "6px 14px", borderRadius: 7, border: "1px solid #e9edef", background: "transparent", color: "#8696a0", fontSize: 12, fontWeight: 600, cursor: "pointer", fontFamily: "inherit" }}>⚙️ Gerenciar etiquetas</button>
        </div>
      </div>
      <div style={{ flex: 1, display: "flex", gap: 16, padding: "20px 24px", overflowX: "auto", overflowY: "hidden" }}>
        {/* Sem etiqueta — sempre primeiro */}
        <div
          onDragOver={e => { e.preventDefault(); setDragOver("unlabeled"); }}
          onDragLeave={() => setDragOver(null)}
          onDrop={() => handleDrop("unlabeled")}
          style={{ width: 270, flexShrink: 0, display: "flex", flexDirection: "column", background: dragOver === "unlabeled" ? "#e9edef" : "#ffffff", border: `1px solid ${dragOver === "unlabeled" ? "#55555566" : "#e9edef"}`, borderRadius: 12, overflow: "hidden", transition: "all 0.15s" }}
        >
          <div style={{ padding: "12px 14px", borderBottom: "2px solid #33333322", display: "flex", alignItems: "center", gap: 8 }}>
            <div style={{ width: 10, height: 10, borderRadius: "50%", background: "#667781", flexShrink: 0 }} />
            <span style={{ fontWeight: 700, fontSize: 13, color: "#667781", flex: 1 }}>Sem etiqueta</span>
            <span style={{ background: "#33333322", color: "#667781", fontSize: 11, fontWeight: 700, padding: "2px 8px", borderRadius: 20 }}>{unlabeled.length}</span>
          </div>
          <div style={{ flex: 1, overflowY: "auto", padding: "10px 8px", display: "flex", flexDirection: "column", gap: 8 }}>
            {unlabeled.length === 0
              ? <div style={{ border: `2px dashed ${dragOver === "unlabeled" ? "#55555588" : "#55555522"}`, borderRadius: 8, padding: 20, textAlign: "center", color: "#667781", fontSize: 12, transition: "all 0.15s" }}>{dragOver === "unlabeled" ? "➕ Soltar aqui" : "Nenhum lead"}</div>
              : unlabeled.map(conv => renderCard(conv, null))
            }
          </div>
        </div>

        {allLabels.map(label => {
          const cards = conversations.filter(c => (c.labels || []).some(l => l.id === label.id));
          const isOver = dragOver === label.id;
          return (
            <div
              key={label.id}
              onDragOver={e => { e.preventDefault(); setDragOver(label.id); }}
              onDragLeave={() => setDragOver(null)}
              onDrop={() => handleDrop(label.id)}
              style={{ width: 270, flexShrink: 0, display: "flex", flexDirection: "column", background: isOver ? "#e9edef" : "#ffffff", border: `1px solid ${isOver ? label.color + "66" : "#e9edef"}`, borderRadius: 12, overflow: "hidden", transition: "all 0.15s" }}            >
              <div style={{ padding: "12px 14px", borderBottom: `2px solid ${label.color}44`, display: "flex", alignItems: "center", gap: 8 }}>
                <div style={{ width: 10, height: 10, borderRadius: "50%", background: label.color, flexShrink: 0 }} />
                <span style={{ fontWeight: 700, fontSize: 13, color: label.color, flex: 1 }}>{label.name}</span>
                <span style={{ background: label.color + "22", color: label.color, fontSize: 11, fontWeight: 700, padding: "2px 8px", borderRadius: 20 }}>{cards.length}</span>
              </div>
              <div style={{ flex: 1, overflowY: "auto", padding: "10px 8px", display: "flex", flexDirection: "column", gap: 8 }}>
                {cards.length === 0
                  ? <div style={{ border: `2px dashed ${label.color}${isOver ? "88" : "22"}`, borderRadius: 8, padding: 20, textAlign: "center", color: isOver ? label.color : "#667781", fontSize: 12, transition: "all 0.15s" }}>
                      {isOver ? "➕ Soltar aqui" : "Nenhum lead"}
                    </div>
                  : cards.map(conv => renderCard(conv, label))
                }
                {cards.length > 0 && isOver && (
                  <div style={{ border: `2px dashed ${label.color}88`, borderRadius: 8, padding: 12, textAlign: "center", color: label.color, fontSize: 12 }}>➕ Soltar aqui</div>
                )}
              </div>
            </div>
          );
        })}


      </div>
    </div>
  );
}

// ─── Task Detail Modal ────────────────────────────────────────────────────────
function TaskDetailModal({ task, agents, onClose, onComplete }) {
  const [updates, setUpdates] = useState([]);
  const [newUpdate, setNewUpdate] = useState("");
  const [sending, setSending] = useState(false);
  const [loading, setLoading] = useState(true);
  useEffect(() => { fetchUpdates(); }, [task.id]);
  const fetchUpdates = async () => {
    try { const r = await fetch(`${API_URL}/tasks/${task.id}/updates`, { headers }); const d = await r.json(); setUpdates(d.updates || []); } catch (e) {}
    setLoading(false);
  };
  const sendUpdate = async () => {
    if (!newUpdate.trim() || sending) return;
    setSending(true);
    try { await fetch(`${API_URL}/tasks/${task.id}/updates`, { method: "POST", headers, body: JSON.stringify({ content: newUpdate.trim(), created_by: "Atendente" }) }); setNewUpdate(""); await fetchUpdates(); } catch (e) {}
    setSending(false);
  };
  const isOverdue = task.due_at && new Date(task.due_at) < new Date();
  const assignedName = task.users?.name || agents.find(a => a.id === task.assigned_to)?.name;
  return (
    <div style={{ position: "fixed", inset: 0, background: "#00000055", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 300 }} onClick={onClose}>
      <div onClick={e => e.stopPropagation()} style={{ background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 16, width: 500, maxHeight: "85vh", display: "flex", flexDirection: "column", boxShadow: "0 2px 5px #0000001a, 0 8px 20px #00000012" }}>
        <div style={{ padding: "20px 24px 16px", borderBottom: `1px solid ${T.border}` }}>
          <div style={{ display: "flex", alignItems: "flex-start", gap: 12 }}>
            <div style={{ flex: 1 }}>
              <div style={{ fontSize: 16, fontWeight: 700, color: "#111b21", marginBottom: 6 }}>{task.title}</div>
              {task.description && <div style={{ fontSize: 13, color: "#8696a0", lineHeight: 1.6 }}>{task.description}</div>}
            </div>
            <span onClick={onClose} style={{ cursor: "pointer", color: "#667781", fontSize: 20, lineHeight: 1, flexShrink: 0 }}>×</span>
          </div>
          <div style={{ display: "flex", gap: 12, marginTop: 12, flexWrap: "wrap" }}>
            {task.due_at && <span style={{ fontSize: 12, color: isOverdue ? "#f44336" : "#8696a0", display: "flex", alignItems: "center", gap: 4 }}>📅 {new Date(task.due_at).toLocaleDateString("pt-BR", { day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit" })}{isOverdue && <span style={{ background: "#f4433322", color: "#f44336", fontSize: 10, fontWeight: 700, padding: "1px 6px", borderRadius: 10 }}>VENCIDA</span>}</span>}
            {assignedName && <span style={{ fontSize: 12, color: "#00a884" }}>👤 {assignedName}</span>}
          </div>
        </div>
        <div style={{ flex: 1, overflowY: "auto", padding: "16px 24px" }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: "#667781", marginBottom: 12 }}>ATUALIZAÇÕES</div>
          {loading ? <div style={{ color: "#667781", fontSize: 13, textAlign: "center", padding: 16 }}>Carregando...</div>
            : updates.length === 0 ? <div style={{ color: "#667781", fontSize: 13, textAlign: "center", padding: 16 }}>Nenhuma atualização ainda.</div>
            : updates.map((u, i) => (
              <div key={u.id || i} style={{ display: "flex", gap: 10, marginBottom: 14 }}>
                <div style={{ display: "flex", flexDirection: "column", alignItems: "center" }}>
                  <Avatar name={u.created_by || "?"} size={28} />
                  {i < updates.length - 1 && <div style={{ width: 2, flex: 1, background: "#e9edef", marginTop: 4 }} />}
                </div>
                <div style={{ flex: 1, paddingBottom: 4 }}>
                  <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 4 }}>
                    <span style={{ fontSize: 12, fontWeight: 600, color: "#111b21" }}>{u.created_by || "Atendente"}</span>
                    <span style={{ fontSize: 11, color: "#667781" }}>{timeAgo(u.created_at)}</span>
                  </div>
                  <div style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 8, padding: "10px 12px", fontSize: 13, color: "#54656f", lineHeight: 1.5 }}>{u.content}</div>
                </div>
              </div>
            ))}
        </div>
        <div style={{ padding: "14px 24px", borderTop: "1px solid #e9edef" }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: "#667781", marginBottom: 8 }}>NOVA ATUALIZAÇÃO</div>
          <div style={{ display: "flex", gap: 8, marginBottom: 10 }}>
            <textarea value={newUpdate} onChange={e => setNewUpdate(e.target.value)} onKeyDown={e => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); sendUpdate(); } }} placeholder="Descreva o que foi feito, próximos passos..." rows={2} style={{ flex: 1, padding: "9px 12px", background: "#ffffff", border: "1px solid #e9edef", borderRadius: 9, color: "#111b21", fontSize: 13, outline: "none", resize: "none", fontFamily: "inherit", lineHeight: 1.5 }} />
            <button onClick={sendUpdate} disabled={!newUpdate.trim() || sending} style={{ padding: "0 16px", borderRadius: 9, border: "none", background: newUpdate.trim() ? "linear-gradient(135deg, #00a884, #017561)" : "#e9edef", color: newUpdate.trim() ? "#000" : "#667781", fontSize: 13, fontWeight: 700, cursor: newUpdate.trim() ? "pointer" : "not-allowed", fontFamily: "inherit", flexShrink: 0 }}>{sending ? "..." : "↑"}</button>
          </div>
          <div style={{ display: "flex", gap: 8 }}>
            <button onClick={onClose} style={{ flex: 1, padding: "9px 0", borderRadius: 9, border: "1px solid #e9edef", background: "transparent", color: "#667781", fontSize: 13, fontWeight: 600, cursor: "pointer", fontFamily: "inherit" }}>✕ Fechar</button>
            <button onClick={() => onComplete(task.id)} style={{ flex: 2, padding: "9px 0", borderRadius: 9, border: "1px solid #00a88444", background: "#00a88410", color: "#00a884", fontSize: 13, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>✅ Marcar como concluída</button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Tasks Panel ──────────────────────────────────────────────────────────────
function TasksPanel({ convId, agents, onClose, onTaskDone }) {
  const [tasks, setTasks] = useState([]);
  const [loading, setLoading] = useState(true);
  const [title, setTitle] = useState("");
  const [description, setDescription] = useState("");
  const [dueAt, setDueAt] = useState("");
  const [assignedTo, setAssignedTo] = useState("");
  const [creating, setCreating] = useState(false);
  const [selectedTask, setSelectedTask] = useState(null);
  const [taskError, setTaskError] = useState("");
  const fetchTasks = async () => {
    try {
      // Try conversation-scoped endpoint first, fallback to /tasks filtered
      const r = await fetch(`${API_URL}/tasks?tenant_id=${TENANT_ID}&conversation_id=${convId}`, { headers });
      if (r.ok) {
        const d = await r.json();
        const list = d.tasks || d || [];
        setTasks(Array.isArray(list) ? list.filter(t => t.conversation_id === convId) : []);
      }
    } catch (e) {}
    setLoading(false);
  };
  useEffect(() => { fetchTasks(); }, [convId]);
  const createTask = async () => {
    if (!title.trim() || creating) return;
    setCreating(true);
    setTaskError("");
    try {
      const payload = { tenant_id: TENANT_ID, conversation_id: convId, title: title.trim(), description: description.trim() || null, assigned_to: assignedTo || null, due_at: dueAt || null };
      const resp = await fetch(`${API_URL}/tasks`, { method: "POST", headers, body: JSON.stringify(payload) });
      if (resp.ok) {
        setTitle(""); setDescription(""); setDueAt(""); setAssignedTo("");
        await fetchTasks();
      } else {
        const err = await resp.json().catch(() => ({}));
        const msg = err.detail || err.message || `Erro ${resp.status}`;
        setTaskError(msg);
        console.error("Task create error:", resp.status, err);
      }
    } catch (e) {
      setTaskError("Erro de conexão: " + e.message);
      console.error("Task create exception:", e);
    }
    setCreating(false);
  };
  const completeTask = async (taskId) => {
    try { await fetch(`${API_URL}/tasks/${taskId}/done`, { method: "PUT", headers }); setTasks(prev => prev.filter(t => t.id !== taskId)); setSelectedTask(null); if (onTaskDone) onTaskDone(); } catch (e) {}
  };
  const isOverdue = (due) => due && new Date(due) < new Date();
  return (
    <>
      <div style={{ width: 300, flexShrink: 0, borderLeft: "1px solid #e9edef", background: "#ffffff", display: "flex", flexDirection: "column" }}>
        <div style={{ padding: "14px 16px", borderBottom: "1px solid #e9edef", display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ fontSize: 14, fontWeight: 700 }}>✅ Tarefas</span>
          <span style={{ background: "#00a88422", color: "#00a884", fontSize: 11, fontWeight: 700, padding: "1px 7px", borderRadius: 20 }}>{tasks.length}</span>
          <span onClick={onClose} style={{ marginLeft: "auto", cursor: "pointer", color: "#667781", fontSize: 18, lineHeight: 1 }}>×</span>
        </div>
        <div style={{ padding: "14px 16px", borderBottom: `1px solid ${T.border}` }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: "#667781", marginBottom: 8 }}>NOVA TAREFA</div>
          <input value={title} onChange={e => setTitle(e.target.value)} placeholder="Título da tarefa *" style={{ width: "100%", padding: "8px 10px", background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 7, color: "#111b21", fontSize: 12, outline: "none", boxSizing: "border-box", fontFamily: "inherit", marginBottom: 6 }} />
          <textarea value={description} onChange={e => setDescription(e.target.value)} placeholder="Descrição (opcional)..." rows={2} style={{ width: "100%", padding: "8px 10px", background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 7, color: "#111b21", fontSize: 12, outline: "none", boxSizing: "border-box", fontFamily: "inherit", resize: "none", marginBottom: 6, lineHeight: 1.5 }} />
          <input type="datetime-local" value={dueAt} onChange={e => setDueAt(e.target.value)} style={{ width: "100%", padding: "7px 8px", background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 7, color: "#8696a0", fontSize: 11, outline: "none", fontFamily: "inherit", colorScheme: "dark", boxSizing: "border-box", marginBottom: 6 }} />
          <select value={assignedTo} onChange={e => setAssignedTo(e.target.value)} style={{ width: "100%", padding: "7px 10px", background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 7, color: assignedTo ? "#111b21" : "#667781", fontSize: 12, outline: "none", marginBottom: 10, fontFamily: "inherit", boxSizing: "border-box" }}>
            <option value="">Responsável (opcional)</option>
            {agents.map(a => <option key={a.id} value={a.id}>{a.name}</option>)}
          </select>
          <button onClick={createTask} disabled={!title.trim() || creating} style={{ width: "100%", padding: "8px 0", borderRadius: 7, border: "none", background: title.trim() ? "linear-gradient(135deg, #00a884, #017561)" : "#e9edef", color: title.trim() ? "#000" : "#667781", fontSize: 12, fontWeight: 700, cursor: title.trim() ? "pointer" : "not-allowed", fontFamily: "inherit" }}>{creating ? "Criando..." : "+ Criar tarefa"}</button>
          {taskError && <div style={{ background: "#f4433315", border: "1px solid #f4433333", borderRadius: 7, padding: "8px 10px", marginTop: 8, fontSize: 11, color: "#f44336" }}>❌ {taskError}</div>}
        </div>
        <div style={{ flex: 1, overflowY: "auto", padding: "10px 12px" }}>
          {loading ? <div style={{ textAlign: "center", color: "#667781", fontSize: 12, padding: 16 }}>Carregando...</div>
            : tasks.length === 0 ? <div style={{ textAlign: "center", padding: 24 }}><div style={{ fontSize: 28, marginBottom: 8 }}>📋</div><div style={{ fontSize: 12, color: "#667781" }}>Nenhuma tarefa ainda</div></div>
            : tasks.map(task => (
              <div key={task.id} onClick={() => setSelectedTask(task)} style={{ background: "#f0f2f5", border: `1px solid ${isOverdue(task.due_at) ? "#f4433644" : "#d1d7db"}`, borderRadius: 9, padding: "10px 12px", marginBottom: 8, cursor: "pointer" }} onMouseEnter={e => e.currentTarget.style.borderColor = "#00a88444"} onMouseLeave={e => e.currentTarget.style.borderColor = isOverdue(task.due_at) ? "#f4433644" : "#d1d7db"}>
                <div style={{ fontSize: 12, fontWeight: 600, color: "#111b21", marginBottom: 4 }}>{task.title}</div>
                {task.description && <div style={{ fontSize: 11, color: "#667781", marginBottom: 6, lineHeight: 1.4, overflow: "hidden", display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical" }}>{task.description}</div>}
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                  {task.due_at && <span style={{ fontSize: 10, color: isOverdue(task.due_at) ? "#f44336" : "#8696a0" }}>📅 {new Date(task.due_at).toLocaleDateString("pt-BR", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" })}</span>}
                  {task.users?.name && <span style={{ fontSize: 10, color: "#00a884" }}>👤 {task.users.name}</span>}
                </div>
                <div style={{ marginTop: 6, fontSize: 10, color: "#667781" }}>Clique para ver detalhes →</div>
              </div>
            ))}
        </div>
      </div>
      {selectedTask && <TaskDetailModal task={selectedTask} agents={agents} onClose={() => setSelectedTask(null)} onComplete={completeTask} />}
    </>
  );
}

// ─── Column Manager ───────────────────────────────────────────────────────────
function ColumnManagerModal({ columns, onChange, onClose }) {
  const [cols, setCols] = useState(columns.map(c => ({ ...c })));
  const [editingId, setEditingId] = useState(null);
  const [pickingColorFor, setPickingColorFor] = useState(null);
  const [newLabel, setNewLabel] = useState("");
  const update = (id, patch) => setCols(prev => prev.map(c => c.id === id ? { ...c, ...patch } : c));
  const addCol = () => { if (!newLabel.trim()) return; setCols(prev => [...prev, { id: uid(), label: newLabel.trim(), color: PALETTE[prev.length % PALETTE.length] }]); setNewLabel(""); };
  const save = () => { onChange(cols); saveColumns(cols); onClose(); };
  return (
    <div style={{ position: "fixed", inset: 0, background: "#00000055", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 200 }} onClick={onClose}>
      <div onClick={e => e.stopPropagation()} onMouseDown={e => e.preventDefault()} style={{ background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 14, padding: 24, width: 420, maxHeight: "85vh", overflowY: "auto", boxShadow: "0 2px 5px #0000001a, 0 8px 20px #00000012" }}>
        <div style={{ fontWeight: 700, fontSize: 16, marginBottom: 4 }}>⚙️ Gerenciar Colunas</div>
        <div style={{ fontSize: 12, color: "#667781", marginBottom: 20 }}>Crie, renomeie ou delete colunas do Kanban</div>
        <div style={{ display: "flex", flexDirection: "column", gap: 8, marginBottom: 20 }}>
          {cols.map(col => (
            <div key={col.id} style={{ position: "relative" }}>
              <div style={{ display: "flex", alignItems: "center", gap: 10, background: "#ffffff", border: "1px solid #e9edef", borderRadius: 10, padding: "10px 14px" }}>
                <div onClick={() => setPickingColorFor(pickingColorFor === col.id ? null : col.id)} style={{ width: 22, height: 22, borderRadius: "50%", background: col.color, cursor: "pointer", flexShrink: 0, border: "1px solid #e9edef" }} />
                {editingId === col.id ? <input autoFocus value={col.label} onChange={e => update(col.id, { label: e.target.value })} onBlur={() => setEditingId(null)} onKeyDown={e => e.key === "Enter" && setEditingId(null)} style={{ flex: 1, background: "#e9edef", border: "1px solid #00a88444", borderRadius: 6, color: "#111b21", fontSize: 13, padding: "4px 10px", outline: "none", fontFamily: "inherit" }} />
                  : <span onClick={() => setEditingId(col.id)} style={{ flex: 1, fontSize: 13, fontWeight: 600, cursor: "text", color: col.color }}>{col.label}</span>}
                <span onClick={() => setEditingId(col.id)} style={{ fontSize: 14, cursor: "pointer", opacity: 0.4 }}>✏️</span>
                {cols.length > 1 && <span onClick={() => setCols(prev => prev.filter(c => c.id !== col.id))} style={{ fontSize: 14, cursor: "pointer", opacity: 0.4 }}>🗑</span>}
              </div>
              {pickingColorFor === col.id && (
                <div style={{ position: "absolute", top: "110%", left: 0, zIndex: 300, background: "#e9edef", border: "1px solid #e9edef", borderRadius: 10, padding: 12, display: "flex", flexWrap: "wrap", gap: 8, width: 200, boxShadow: "0 1px 3px #0000001a, 0 4px 12px #0000000f" }}>
                  {PALETTE.map(c => <div key={c} onClick={() => { update(col.id, { color: c }); setPickingColorFor(null); }} style={{ width: 28, height: 28, borderRadius: "50%", background: c, cursor: "pointer", border: col.color === c ? "3px solid #fff" : "2px solid transparent" }} />)}
                </div>
              )}
            </div>
          ))}
        </div>
        <div style={{ marginBottom: 20 }}>
          <div style={{ fontSize: 12, fontWeight: 600, color: "#667781", marginBottom: 8 }}>Nova coluna</div>
          <div style={{ display: "flex", gap: 8 }}>
            <input value={newLabel} onChange={e => setNewLabel(e.target.value)} onKeyDown={e => e.key === "Enter" && addCol()} placeholder="Nome da coluna..." style={{ flex: 1, padding: "8px 12px", background: "#ffffff", border: "1px solid #e9edef", borderRadius: 8, color: "#111b21", fontSize: 13, outline: "none", fontFamily: "inherit" }} />
            <button onClick={addCol} disabled={!newLabel.trim()} style={{ padding: "8px 16px", borderRadius: 8, border: "none", background: newLabel.trim() ? "linear-gradient(135deg, #00a884, #017561)" : "#e9edef", color: newLabel.trim() ? "#000" : "#667781", fontSize: 13, fontWeight: 700, cursor: newLabel.trim() ? "pointer" : "not-allowed", fontFamily: "inherit" }}>+ Criar</button>
          </div>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <button onClick={onClose} style={{ flex: 1, padding: "9px 0", borderRadius: 8, border: "1px solid #e9edef", background: "transparent", color: "#667781", fontSize: 13, cursor: "pointer", fontFamily: "inherit" }}>Cancelar</button>
          <button onClick={save} style={{ flex: 2, padding: "9px 0", borderRadius: 8, border: "none", background: "linear-gradient(135deg, #00a884, #017561)", color: "#000", fontSize: 13, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>💾 Salvar colunas</button>
        </div>
      </div>
    </div>
  );
}

function AssignModal({ conversation, agents, onAssign, onClose }) {
  return (
    <div style={{ position: "fixed", inset: 0, background: "#00000080", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 100 }} onClick={onClose}>
      <div onClick={e => e.stopPropagation()} style={{ background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 12, padding: 20, width: 300, boxShadow: "0 2px 5px #0000001a, 0 8px 16px #0000000f" }}>
        <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 16 }}>Atribuir conversa</div>
        {agents.length === 0 ? <div style={{ color: "#667781", fontSize: 13, textAlign: "center", padding: 12 }}>Nenhum atendente encontrado</div>
          : agents.map(agent => (
            <div key={agent.id} onClick={() => onAssign(agent)} style={{ display: "flex", alignItems: "center", gap: 12, padding: "10px 12px", borderRadius: 8, cursor: "pointer" }} onMouseEnter={e => e.currentTarget.style.background = "#e9edef"} onMouseLeave={e => e.currentTarget.style.background = "transparent"}>
              <Avatar name={agent.name} size={32} />
              <div><div style={{ fontSize: 13, fontWeight: 600 }}>{agent.name}</div><div style={{ fontSize: 11, color: "#667781" }}>{agent.role === "admin" ? "Admin" : "Atendente"}</div></div>
              {conversation.assigned_to === agent.id && <span style={{ marginLeft: "auto", color: "#00a884", fontSize: 16 }}>✓</span>}
            </div>
          ))}
        <button onClick={onClose} style={{ marginTop: 12, width: "100%", padding: "8px 0", borderRadius: 8, border: "1px solid #e9edef", background: "transparent", color: "#667781", fontSize: 13, cursor: "pointer", fontFamily: "inherit" }}>Cancelar</button>
      </div>
    </div>
  );
}

function LabelPickerModal({ conversation, labels, onToggle, onClose, onManage }) {
  const convLabels = conversation.labels || [];
  return (
    <div style={{ position: "fixed", inset: 0, background: "#00000080", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 100 }} onClick={onClose}>
      <div onClick={e => e.stopPropagation()} style={{ background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 12, padding: 20, width: 280, boxShadow: "0 2px 5px #0000001a, 0 8px 16px #0000000f" }}>
        <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 16 }}>Adicionar etiqueta</div>
        <div style={{ display: "flex", flexDirection: "column", gap: 8, marginBottom: 14 }}>
          {labels.map(label => {
            const active = convLabels.some(l => l.id === label.id);
            return <div key={label.id} onClick={() => onToggle(label)} style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 12px", borderRadius: 8, cursor: "pointer", border: `1px solid ${active ? label.color : "#d1d7db"}`, background: active ? label.color + "11" : "transparent" }}>
              <div style={{ width: 10, height: 10, borderRadius: "50%", background: label.color, flexShrink: 0 }} />
              <span style={{ fontSize: 13, color: active ? label.color : "#54656f", fontWeight: active ? 600 : 400 }}>{label.name}</span>
              {active && <span style={{ marginLeft: "auto", color: label.color }}>✓</span>}
            </div>;
          })}
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <button onClick={onManage} style={{ flex: 1, padding: "7px 0", borderRadius: 8, border: "1px solid #e9edef", background: "transparent", color: "#667781", fontSize: 12, cursor: "pointer", fontFamily: "inherit" }}>⚙️ Gerenciar</button>
          <button onClick={onClose} style={{ flex: 1, padding: "7px 0", borderRadius: 8, border: "1px solid #e9edef", background: "transparent", color: "#667781", fontSize: 12, cursor: "pointer", fontFamily: "inherit" }}>Fechar</button>
        </div>
      </div>
    </div>
  );
}

function KanbanBoard({ conversations, columns, onMoveCard, onSelectConv, onManageCols, instanceFilter }) {
  const [dragging, setDragging] = useState(null);
  const [dragOver, setDragOver] = useState(null);
  const getStage = (conv) => columns.find(c => c.id === conv.kanban_stage) ? conv.kanban_stage : columns[0]?.id;
  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <div style={{ padding: "14px 24px", borderBottom: "1px solid #e9edef", display: "flex", alignItems: "center", gap: 12 }}>
        <span style={{ fontSize: 15, fontWeight: 700 }}>Kanban de Conversas</span>
        <span style={{ fontSize: 12, color: "#667781" }}>Arraste para mover entre colunas</span>
        <button onClick={onManageCols} style={{ marginLeft: "auto", padding: "6px 14px", borderRadius: 7, border: "1px solid #e9edef", background: "transparent", color: "#8696a0", fontSize: 12, fontWeight: 600, cursor: "pointer", fontFamily: "inherit" }}>⚙️ Gerenciar colunas</button>
      </div>
      <div style={{ flex: 1, display: "flex", gap: 16, padding: "20px 24px", overflowX: "auto", overflowY: "hidden" }}>
        {columns.map(col => {
          const cards = conversations.filter(c => getStage(c) === col.id);
          const isOver = dragOver === col.id;
          return (
            <div key={col.id} onDragOver={e => { e.preventDefault(); setDragOver(col.id); }} onDragLeave={() => setDragOver(null)} onDrop={() => { if (dragging) { const conv = conversations.find(c => c.id === dragging); if (conv) onMoveCard(conv, col.id); } setDragging(null); setDragOver(null); }} style={{ width: 270, flexShrink: 0, display: "flex", flexDirection: "column", background: isOver ? "#e9edef" : "#ffffff", border: `1px solid ${isOver ? col.color + "55" : "#e9edef"}`, borderRadius: 12, overflow: "hidden", transition: "all 0.15s" }}>
              <div style={{ padding: "12px 14px", borderBottom: "1px solid #e9edef", display: "flex", alignItems: "center", gap: 8 }}>
                <div style={{ width: 10, height: 10, borderRadius: "50%", background: col.color }} />
                <span style={{ fontWeight: 700, fontSize: 13, color: "#54656f", flex: 1 }}>{col.label}</span>
                <span style={{ background: col.color + "22", color: col.color, fontSize: 11, fontWeight: 700, padding: "2px 8px", borderRadius: 20 }}>{cards.length}</span>
              </div>
              <div style={{ flex: 1, overflowY: "auto", padding: "10px 8px", display: "flex", flexDirection: "column", gap: 8 }}>
                {cards.length === 0 && <div style={{ border: `2px dashed ${col.color}22`, borderRadius: 8, padding: 20, textAlign: "center", color: "#667781", fontSize: 12 }}>Arraste para cá</div>}
                {cards.map(conv => (
                  <div key={conv.id} draggable onDragStart={() => setDragging(conv.id)} onDragEnd={() => { setDragging(null); setDragOver(null); }} onClick={() => onSelectConv(conv)} style={{ background: "#f0f2f5", border: `1px solid ${dragging === conv.id ? col.color + "55" : "#d1d7db"}`, borderRadius: 10, padding: "11px 13px", cursor: "grab", opacity: dragging === conv.id ? 0.4 : 1 }} onMouseEnter={e => e.currentTarget.style.borderColor = col.color + "44"} onMouseLeave={e => e.currentTarget.style.borderColor = "#d1d7db"}>
                    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
                      <Avatar name={displayName(conv.contacts?.name, conv.contacts?.phone)} size={26} phone={conv.contacts?.phone} instanceFilter={instanceFilter} />
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{ fontSize: 12, fontWeight: 600, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{displayName(conv.contacts?.name, conv.contacts?.phone)}</div>
                        <div style={{ fontSize: 11, color: "#667781" }}>{timeAgo(conv.last_message_at)}</div>
                      </div>
                      {conv.unread_count > 0 && <span style={{ background: col.color, color: "#000", fontSize: 10, fontWeight: 700, padding: "1px 5px", borderRadius: 10 }}>{conv.unread_count}</span>}
                    </div>
                    {conv.labels?.length > 0 && <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>{conv.labels.map(l => <LabelChip key={l.id} label={l} />)}</div>}
                    {conv.assigned_agent && <div style={{ marginTop: 6, display: "flex", alignItems: "center", gap: 5 }}><Avatar name={conv.assigned_agent} size={14} /><span style={{ fontSize: 10, color: "#667781" }}>{conv.assigned_agent}</span></div>}
                  </div>
                ))}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}


// ─── Reports View ─────────────────────────────────────────────────────────────
function ReportsView({ auth, T = { app: "#f0f2f5", card: "#ffffff", border: "#e9edef", text: "#111b21", text2: "#667781" } }) {
  const [tab, setTab] = useState("mensagens");
  const [days, setDays] = useState(30);
  const [data, setData] = useState({});
  const [loading, setLoading] = useState(false);

  const fetchReport = async (type) => {
    setLoading(true);
    try {
      const r = await fetch(`${API_URL}/reports/${type}?tenant_id=${TENANT_ID}&days=${days}`, { headers });
      const d = await r.json();
      setData(prev => ({ ...prev, [type]: d }));
    } catch(e) {}
    setLoading(false);
  };

  useEffect(() => {
    fetchReport("messages");
    fetchReport("agents");
    fetchReport("broadcasts");
    fetchReport("credits");
  }, [days]);

  const TABS = [
    { id: "mensagens", label: "💬 Mensagens" },
    { id: "atendentes", label: "👥 Atendentes" },
    { id: "disparos", label: "📢 Disparos" },
    { id: "creditos", label: "⚡ Créditos IA" },
  ];

  const WEEKDAYS = ["Seg","Ter","Qua","Qui","Sex","Sáb","Dom"];
  const maxDay = Math.max(1, ...(data.messages?.by_weekday || []).map(d => d.count));
  const maxHour = Math.max(1, ...(data.messages?.by_hour || []).map(h => h.count));

  const Bar = ({ value, max, color = "#00a884", height = 40 }) => (
    <div style={{ width: "100%", height, background: T.border, borderRadius: 4, overflow: "hidden", display: "flex", alignItems: "flex-end" }}>
      <div style={{ width: "100%", height: `${Math.max(4, value/max*100)}%`, background: color, borderRadius: 4, transition: "height 0.4s" }} />
    </div>
  );

  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden", background: T.app }}>
      {/* Sub header */}
      <div style={{ borderBottom: `1px solid ${T.border}`, padding: "0 24px", display: "flex", alignItems: "center", gap: 4, background: T.card, flexShrink: 0 }}>
        {TABS.map(t => (
          <button key={t.id} onClick={() => setTab(t.id)}
            style={{ padding: "10px 14px", border: "none", borderBottom: `2px solid ${tab===t.id?"#00a884":"transparent"}`,
              background: "transparent", color: tab===t.id?"#00a884":"#667781", fontSize: 12, fontWeight: 600,
              cursor: "pointer", fontFamily: "inherit" }}>
            {t.label}
          </button>
        ))}
        <div style={{ flex: 1 }} />
        <div style={{ display: "flex", gap: 6 }}>
          {[7,30,90].map(d => (
            <button key={d} onClick={() => setDays(d)}
              style={{ padding: "5px 12px", borderRadius: 6, border: `1px solid ${days===d?"#00a884":"#e9edef"}`,
                background: days===d?"#00a88415":"transparent", color: days===d?"#00a884":"#667781",
                fontSize: 11, fontWeight: 600, cursor: "pointer", fontFamily: "inherit" }}>
              {d}d
            </button>
          ))}
        </div>
      </div>

      <div style={{ flex: 1, overflowY: "auto", padding: 24 }}>
        {loading && <div style={{ textAlign: "center", padding: 40, color: "#667781", fontSize: 13 }}>Carregando...</div>}

        {/* ── MENSAGENS ── */}
        {tab === "mensagens" && data.messages && (
          <>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 10, marginBottom: 20 }}>
              {[
                { label: "Total mensagens", value: data.messages.total?.toLocaleString("pt-BR"), color: "#e8e8ff" },
                { label: "Recebidas", value: data.messages.inbound?.toLocaleString("pt-BR"), color: "#00a884" },
                { label: "Enviadas", value: data.messages.outbound?.toLocaleString("pt-BR"), color: "#7c4dff" },
                { label: "Média diária", value: Math.round((data.messages.total||0)/days).toLocaleString("pt-BR"), color: "#ff9800" },
              ].map(k => (
                <div key={k.label} style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 12, padding: "14px 16px" }}>
                  <div style={{ fontSize: 10, color: "#667781", marginBottom: 6, letterSpacing: 1 }}>{k.label.toUpperCase()}</div>
                  <div style={{ fontSize: 22, fontWeight: 800, color: k.color }}>{k.value}</div>
                </div>
              ))}
            </div>

            {/* Por dia */}
            <div style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 14, padding: 20, marginBottom: 16 }}>
              <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 16 }}>📅 Mensagens por dia</div>
              <div style={{ display: "flex", alignItems: "flex-end", gap: 3, height: 80 }}>
                {(data.messages.by_day || []).slice(-30).map((d, i) => {
                  const maxD = Math.max(1, ...(data.messages.by_day||[]).map(x=>x.count));
                  const pct = d.count/maxD*100;
                  const isWeekend = new Date(d.date).getDay() % 6 === 0;
                  return (
                    <div key={i} title={`${d.date}: ${d.count} msgs`} style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", gap: 4 }}>
                      <div style={{ width: "100%", height: 64, display: "flex", alignItems: "flex-end" }}>
                        <div style={{ width: "100%", height: `${Math.max(4, pct)}%`, background: isWeekend?"#7c4dff":"#00a884", borderRadius: "3px 3px 0 0", opacity: 0.8 }} />
                      </div>
                    </div>
                  );
                })}
              </div>
              <div style={{ fontSize: 10, color: "#54656f", marginTop: 8 }}>
                <span style={{ color: "#00a884" }}>■</span> Dias úteis &nbsp;
                <span style={{ color: "#7c4dff" }}>■</span> Fins de semana
              </div>
            </div>

            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
              {/* Por hora */}
              <div style={{ background: T.card, border: `1px solid ${T.border}`, borderRadius: 14, padding: 20 }}>
                <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 16 }}>🕐 Pico de mensagens por hora</div>
                <div style={{ display: "flex", alignItems: "flex-end", gap: 2, height: 80 }}>
                  {(data.messages.by_hour || []).map((h, i) => {
                    const pct = h.count/maxHour*100;
                    const isDay = h.hour >= 8 && h.hour <= 18;
                    return (
                      <div key={i} style={{ flex: 1 }} title={`${h.hour}h: ${h.count}`}>
                        <div style={{ height: 64, display: "flex", alignItems: "flex-end" }}>
                          <div style={{ width: "100%", height: `${Math.max(3, pct)}%`, background: isDay?"#00a884":"#54656f", borderRadius: "2px 2px 0 0" }} />
                        </div>
                        {h.hour % 6 === 0 && <div style={{ fontSize: 8, color: "#54656f", textAlign: "center" }}>{h.hour}h</div>}
                      </div>
                    );
                  })}
                </div>
              </div>

              {/* Por dia da semana */}
              <div style={{ background: T.card, border: `1px solid ${T.border}`, borderRadius: 14, padding: 20 }}>
                <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 16 }}>📊 Por dia da semana</div>
                <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                  {(data.messages.by_weekday || []).map((d, i) => (
                    <div key={i} style={{ display: "flex", alignItems: "center", gap: 10 }}>
                      <span style={{ fontSize: 11, color: "#667781", width: 28 }}>{d.label}</span>
                      <div style={{ flex: 1, height: 16, background: "#0f0f1e", borderRadius: 4, overflow: "hidden" }}>
                        <div style={{ height: "100%", width: `${Math.max(4, d.count/maxDay*100)}%`, background: i>=5?"#7c4dff":"#00a884", borderRadius: 4, transition: "width 0.4s" }} />
                      </div>
                      <span style={{ fontSize: 11, color: "#667781", width: 30, textAlign: "right" }}>{d.count}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </>
        )}

        {/* ── ATENDENTES ── */}
        {tab === "atendentes" && data.agents && (
          <>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(3,1fr)", gap: 10, marginBottom: 20 }}>
              {[
                { label: "Total atendentes", value: data.agents.agents?.length, color: "#e8e8ff" },
                { label: "Total conversas", value: data.agents.agents?.reduce((a,x)=>a+x.total_convs,0), color: "#00a884" },
                { label: "Resolvidas", value: data.agents.agents?.reduce((a,x)=>a+x.resolved,0), color: "#7c4dff" },
              ].map(k => (
                <div key={k.label} style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 12, padding: "14px 16px" }}>
                  <div style={{ fontSize: 10, color: "#667781", marginBottom: 6, letterSpacing: 1 }}>{k.label.toUpperCase()}</div>
                  <div style={{ fontSize: 22, fontWeight: 800, color: k.color }}>{k.value}</div>
                </div>
              ))}
            </div>
            <div style={{ background: T.card, border: `1px solid ${T.border}`, borderRadius: 14, padding: 20 }}>
              <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 16 }}>Ranking de atendentes</div>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                <thead>
                  <tr>{["Atendente","Função","Conversas","Resolvidas","Msgs enviadas","Taxa resolução"].map(h => (
                    <th key={h} style={{ textAlign: "left", padding: "8px 12px", fontSize: 10, color: "#667781", letterSpacing: 1, borderBottom: `1px solid ${T.border}` }}>{h.toUpperCase()}</th>
                  ))}</tr>
                </thead>
                <tbody>
                  {(data.agents.agents || []).map((a, i) => {
                    const taxa = a.total_convs > 0 ? Math.round(a.resolved/a.total_convs*100) : 0;
                    return (
                      <tr key={a.id} style={{ borderBottom: "1px solid #0f0f1e" }}>
                        <td style={{ padding: "12px", fontWeight: 700, color: "#e8e8ff" }}>
                          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                            <div style={{ width: 28, height: 28, borderRadius: "50%", background: `hsl(${i*60},50%,30%)`, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 12, fontWeight: 800, color: "#fff" }}>
                              {a.name?.[0]?.toUpperCase()}
                            </div>
                            {a.name}
                          </div>
                        </td>
                        <td style={{ padding: "12px" }}><span style={{ background: a.role==="admin"?"#7c4dff22":"#e9edef", color: a.role==="admin"?"#a78bfa":"#667781", padding: "2px 8px", borderRadius: 20, fontSize: 11 }}>{a.role}</span></td>
                        <td style={{ padding: "12px", fontWeight: 700, color: "#00a884" }}>{a.total_convs}</td>
                        <td style={{ padding: "12px", color: "#7c4dff" }}>{a.resolved}</td>
                        <td style={{ padding: "12px", color: "#8696a0" }}>{a.msgs_sent}</td>
                        <td style={{ padding: "12px" }}>
                          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                            <div style={{ flex: 1, height: 6, background: "#0f0f1e", borderRadius: 3 }}>
                              <div style={{ height: "100%", width: `${taxa}%`, background: taxa>70?"#00a884":taxa>40?"#ff9800":"#f44336", borderRadius: 3 }} />
                            </div>
                            <span style={{ fontSize: 12, fontWeight: 700, color: taxa>70?"#00a884":taxa>40?"#ff9800":"#f44336", minWidth: 35 }}>{taxa}%</span>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </>
        )}

        {/* ── DISPAROS ── */}
        {tab === "disparos" && data.broadcasts && (
          <>
            {/* Totais agregados */}
            {data.broadcasts.broadcasts?.length > 0 && (() => {
              const all = data.broadcasts.broadcasts;
              const totalSent = all.reduce((a,b) => a + (b.sent||0), 0);
              const totalReplied = all.reduce((a,b) => a + (b.replied||0), 0);
              const totalFailed = all.reduce((a,b) => a + (b.failed||0), 0);
              const avgRoi = totalSent > 0 ? Math.round(totalReplied/totalSent*100) : 0;
              return (
                <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 10, marginBottom: 20 }}>
                  {[
                    { label: "Total disparos", value: all.length, color: "#e8e8ff" },
                    { label: "Mensagens enviadas", value: totalSent.toLocaleString("pt-BR"), color: "#7c4dff" },
                    { label: "Conversas geradas", value: totalReplied.toLocaleString("pt-BR"), color: "#00a884" },
                    { label: "ROI médio", value: `${avgRoi}%`, color: avgRoi>10?"#00a884":avgRoi>5?"#ff9800":"#f44336" },
                  ].map(k => (
                    <div key={k.label} style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 12, padding: "14px 16px" }}>
                      <div style={{ fontSize: 10, color: "#667781", marginBottom: 6, letterSpacing: 1 }}>{k.label.toUpperCase()}</div>
                      <div style={{ fontSize: 22, fontWeight: 800, color: k.color }}>{k.value}</div>
                    </div>
                  ))}
                </div>
              );
            })()}

            {data.broadcasts.broadcasts?.length === 0 && (
              <div style={{ textAlign: "center", padding: 60, color: "#667781", fontSize: 13 }}>
                Nenhum disparo realizado ainda.
              </div>
            )}

            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              {(data.broadcasts.broadcasts || []).map(b => {
                const roiColor = b.roi_pct > 10 ? "#00a884" : b.roi_pct > 4 ? "#ff9800" : "#f44336";
                return (
                  <div key={b.id} style={{ background: T.card, border: `1px solid ${T.border}`, borderRadius: 14, padding: 20 }}>
                    {/* Header */}
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 16 }}>
                      <div style={{ flex: 1 }}>
                        {b.name && <div style={{ fontSize: 12, color: "#667781", marginBottom: 4, fontWeight: 600 }}>📢 {b.name}</div>}
                        <div style={{ fontSize: 13, color: "#8696a0", marginBottom: 6, lineHeight: 1.5 }}>"{b.message || "(sem mensagem)"}"</div>
                        <div style={{ fontSize: 11, color: "#54656f" }}>{b.created_at ? new Date(b.created_at).toLocaleDateString("pt-BR", { day:"2-digit", month:"short", year:"numeric", hour:"2-digit", minute:"2-digit" }) : "—"}</div>
                      </div>
                      <span style={{ background: b.status==="completed"?"#00a88420":b.status==="running"?"#7c4dff20":"#ff980020", color: b.status==="completed"?"#00a884":b.status==="running"?"#a78bfa":"#ff9800", fontSize: 10, fontWeight: 700, padding: "4px 12px", borderRadius: 20, whiteSpace: "nowrap", marginLeft: 12 }}>
                        {b.status === "completed" ? "✓ Concluído" : b.status === "running" ? "⏳ Rodando" : b.status}
                      </span>
                    </div>

                    {/* Métricas */}
                    <div style={{ display: "grid", gridTemplateColumns: "repeat(5,1fr)", gap: 8, marginBottom: 16 }}>
                      {[
                        { label: "Enviados", value: b.sent, color: "#7c4dff", icon: "📤" },
                        { label: "Entregues", value: b.delivered, color: "#00a884", icon: "✓" },
                        { label: "Falhas", value: b.failed, color: b.failed > 0 ? "#f44336" : "#54656f", icon: "✗" },
                        { label: "Responderam", value: b.replied, color: b.replied > 0 ? "#00a884" : "#667781", icon: "💬" },
                        { label: "ROI", value: `${b.roi_pct}%`, color: roiColor, icon: "📈" },
                      ].map(s => (
                        <div key={s.label} style={{ background: "#f0f2f5", borderRadius: 10, padding: "10px 12px", textAlign: "center" }}>
                          <div style={{ fontSize: 16, marginBottom: 4 }}>{s.icon}</div>
                          <div style={{ fontSize: 18, fontWeight: 800, color: s.color, marginBottom: 2 }}>{s.value}</div>
                          <div style={{ fontSize: 9, color: "#54656f", letterSpacing: 1 }}>{s.label.toUpperCase()}</div>
                        </div>
                      ))}
                    </div>

                    {/* Barra de conversão visual */}
                    <div style={{ marginBottom: b.replied_sample?.length > 0 ? 12 : 0 }}>
                      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, color: "#54656f", marginBottom: 4 }}>
                        <span>Taxa de conversão em conversa</span>
                        <span style={{ color: roiColor, fontWeight: 700 }}>{b.replied} de {b.delivered} entregues</span>
                      </div>
                      <div style={{ height: 8, background: "#0f0f1e", borderRadius: 4, overflow: "hidden" }}>
                        <div style={{ height: "100%", width: `${Math.min(100, b.roi_pct)}%`, background: `linear-gradient(90deg, ${roiColor}88, ${roiColor})`, borderRadius: 4, transition: "width 0.5s" }} />
                      </div>
                    </div>

                    {/* Quem respondeu (sample) */}
                    {b.replied_sample?.length > 0 && (
                      <div style={{ marginTop: 12, padding: "10px 14px", background: "#00a88408", border: "1px solid #00a88422", borderRadius: 8 }}>
                        <div style={{ fontSize: 10, color: "#00a884", fontWeight: 700, marginBottom: 6, letterSpacing: 1 }}>💬 RESPONDERAM</div>
                        <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                          {b.replied_sample.map((name, i) => (
                            <span key={i} style={{ background: "#00a88415", color: "#00a884", fontSize: 11, padding: "2px 10px", borderRadius: 20 }}>{name}</span>
                          ))}
                          {b.replied > b.replied_sample.length && (
                            <span style={{ color: "#54656f", fontSize: 11 }}>+{b.replied - b.replied_sample.length} outros</span>
                          )}
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </>
        )}

        {/* ── CRÉDITOS IA ── */}
        {tab === "creditos" && data.credits && (
          <>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 10, marginBottom: 20 }}>
              {[
                { label: "Usados no período", value: data.credits.total_used, color: "#e8e8ff" },
                { label: "Restantes", value: data.credits.credits_remaining, color: "#00a884" },
                { label: "Limite do plano", value: data.credits.credits_limit, color: "#7c4dff" },
                { label: "Plano", value: (data.credits.plan || "—").toUpperCase(), color: "#7c4dff" },
              ].map(k => (
                <div key={k.label} style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 12, padding: "14px 16px" }}>
                  <div style={{ fontSize: 10, color: "#667781", marginBottom: 6, letterSpacing: 1 }}>{k.label.toUpperCase()}</div>
                  <div style={{ fontSize: 22, fontWeight: 800, color: k.color }}>{k.value?.toLocaleString?.("pt-BR") ?? k.value}</div>
                </div>
              ))}
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
              <div style={{ background: T.card, border: `1px solid ${T.border}`, borderRadius: 14, padding: 20 }}>
                <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 16 }}>⚡ Consumo por atendente</div>
                {(data.credits.by_agent || []).length === 0 && <div style={{ color: "#667781", fontSize: 12 }}>Nenhum uso registrado.</div>}
                {(data.credits.by_agent || []).map((a, i) => {
                  const maxA = Math.max(1, ...(data.credits.by_agent||[]).map(x=>x.used));
                  return (
                    <div key={i} style={{ marginBottom: 10 }}>
                      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4, fontSize: 12 }}>
                        <span style={{ color: "#8696a0" }}>{a.name}</span>
                        <span style={{ fontWeight: 700, color: "#00a884" }}>{a.used} créditos</span>
                      </div>
                      <div style={{ height: 6, background: "#0f0f1e", borderRadius: 3 }}>
                        <div style={{ height: "100%", width: `${a.used/maxA*100}%`, background: `hsl(${i*50+140},60%,40%)`, borderRadius: 3 }} />
                      </div>
                    </div>
                  );
                })}
              </div>
              <div style={{ background: T.card, border: `1px solid ${T.border}`, borderRadius: 14, padding: 20 }}>
                <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 16 }}>📅 Uso por dia</div>
                <div style={{ display: "flex", alignItems: "flex-end", gap: 3, height: 100 }}>
                  {(data.credits.by_day || []).slice(-30).map((d, i) => {
                    const maxC = Math.max(1, ...(data.credits.by_day||[]).map(x=>x.count));
                    return (
                      <div key={i} title={`${d.date}: ${d.count}`} style={{ flex: 1, height: "100%", display: "flex", alignItems: "flex-end" }}>
                        <div style={{ width: "100%", height: `${Math.max(4, d.count/maxC*100)}%`, background: "#7c4dff", borderRadius: "2px 2px 0 0", opacity: 0.8 }} />
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

// ─── Dashboard de Sócios ───────────────────────────────────────────────────────
const SOCIOS_EMAILS = ["matheusdgs08@gmail.com", "socio2@email.com", "socio3@email.com"];

function DashboardSocios({ auth, clientes_reais }) {
  const [activeTab, setActiveTab] = useState("overview");
  const [clientes, setClientes] = useState(clientes_reais || []);
  const [showAdd, setShowAdd] = useState(false);
  const [novoCli, setNovoCli] = useState({ nome: "", plano: "pro", creditos_extras: 0 });
  const [crescimento, setCrescimento] = useState(4);

  useEffect(() => {
    if (clientes_reais?.length) setClientes(clientes_reais);
  }, [clientes_reais]);

  const CONFIG_S = {
    planos: {
      starter:  { nome: "Starter",  preco: 99,  cor: "#6b7280", creditos: 0,   atendentes: 2  },
      pro:      { nome: "Pro",      preco: 149, cor: "#00a884", creditos: 100, atendentes: 5  },
      business: { nome: "Business", preco: 299, cor: "#7c4dff", creditos: 500, atendentes: 15 },
    },
    pacotes: [
      { nome: "Básico", creditos: 500,  preco: 29 },
      { nome: "Pro",    creditos: 1000, preco: 49 },
      { nome: "Max",    creditos: 2000, preco: 89 },
    ],
    custo_credito: 0.0007,
  };

  const fmt = (v) => v.toLocaleString("pt-BR", { style: "currency", currency: "BRL", maximumFractionDigits: 0 });
  const fmtN = (v) => v.toLocaleString("pt-BR");

  const ativos    = clientes.filter(c => c.status === "ativo");
  const suspensos = clientes.filter(c => c.status !== "ativo");

  const mrr_base     = ativos.reduce((a, c) => a + (CONFIG_S.planos[c.plano]?.preco || 0), 0);
  const mrr_creditos = ativos.reduce((a, c) => a + ((c.creditos_extras || 0) / 500) * 29, 0);
  const mrr_total    = mrr_base + mrr_creditos;
  const custo_api    = ativos.reduce((a, c) => a + ((CONFIG_S.planos[c.plano]?.creditos || 0) + 200) * CONFIG_S.custo_credito, 0);
  const infra_fixa   = 200 + ativos.length * 8;
  const pagarme_fee  = mrr_total * 0.035;
  const custo_total  = custo_api + infra_fixa + pagarme_fee;
  const lucro        = mrr_total - custo_total;
  const margem       = mrr_total > 0 ? (lucro / mrr_total * 100) : 0;
  const lucro_socio  = lucro / 3;
  const arr          = mrr_total * 12;

  const planMix = Object.entries(CONFIG_S.planos).map(([k, pl]) => ({
    name: pl.nome, value: ativos.filter(c => c.plano === k).length, color: pl.cor,
  })).filter(p => p.value > 0);

  const projection = Array.from({ length: 12 }, (_, i) => {
    const n    = Math.round(crescimento * (i + 1));
    const mix  = { starter: Math.round(n*0.25), pro: Math.round(n*0.55), business: Math.round(n*0.20) };
    let mrr = 0, custo = 0;
    Object.entries(mix).forEach(([p, q]) => {
      const pl = CONFIG_S.planos[p];
      mrr   += pl.preco * q + q * 0.2 * 49;
      custo += (pl.creditos + 200) * CONFIG_S.custo_credito * q;
    });
    const infra   = 200 + n * 8;
    const pagarme = mrr * 0.035;
    const luc     = mrr - custo - infra - pagarme;
    return { mes: `M${i+1}`, clientes: n, MRR: Math.round(mrr), Custos: Math.round(custo+infra+pagarme), Lucro: Math.round(luc) };
  });

  const KPI = ({ label, value, sub, color="#00a884", big=false }) => (
    <div style={{ background:"#07070f", border:"1px solid #0f0f1e", borderRadius:12, padding:"16px 18px" }}>
      <div style={{ fontSize:10, color:"#2a2a4a", fontWeight:700, letterSpacing:1.5, marginBottom:6, textTransform:"uppercase" }}>{label}</div>
      <div style={{ fontSize: big?26:20, fontWeight:900, color, letterSpacing:-0.5 }}>{value}</div>
      {sub && <div style={{ fontSize:11, color:"#2a2a4a", marginTop:4 }}>{sub}</div>}
    </div>
  );

  const tabs = [
    { id:"overview",   label:"📊 Visão Geral" },
    { id:"clientes",   label:"👥 Clientes" },
    { id:"financeiro", label:"💰 Financeiro" },
    { id:"projecao",   label:"🚀 Projeção" },
  ];

  return (
    <div style={{ flex:1, display:"flex", flexDirection:"column", background:"#f0f2f5", overflowY:"auto" }}>
      {/* Sub-header */}
      <div style={{ borderBottom:"1px solid #0f0f1e", padding:"0 28px", display:"flex", gap:4, background:"#07070f", flexShrink:0 }}>
        {tabs.map(t => (
          <button key={t.id} onClick={() => setActiveTab(t.id)}
            style={{ padding:"10px 14px", border:"none", borderBottom:`2px solid ${activeTab===t.id?"#00a884":"transparent"}`,
              background:"transparent", color:activeTab===t.id?"#00a884":"#54656f", fontSize:12, fontWeight:600,
              cursor:"pointer", fontFamily:"inherit" }}>
            {t.label}
          </button>
        ))}
        <div style={{ flex:1 }} />
        <div style={{ display:"flex", alignItems:"center", gap:6, fontSize:11, color:"#2a2a4a" }}>
          🔒 Acesso restrito · Sócios
        </div>
      </div>

      <div style={{ padding:"24px 28px", flex:1 }}>

        {/* ── OVERVIEW ── */}
        {activeTab === "overview" && (
          <>
            <div style={{ display:"grid", gridTemplateColumns:"repeat(4,1fr)", gap:10, marginBottom:14 }}>
              <KPI label="MRR Total"    value={fmt(mrr_total)}   sub={`Base ${fmt(mrr_base)} + IA ${fmt(mrr_creditos)}`} big />
              <KPI label="Lucro Líquido" value={fmt(lucro)}      sub={`Margem ${margem.toFixed(0)}%`} color={margem>60?"#00a884":margem>35?"#ff9800":"#f44336"} big />
              <KPI label="ARR"          value={fmt(arr)}         sub="Projeção anual" color="#7c4dff" big />
              <KPI label="Por sócio/mês" value={fmt(lucro_socio)} sub="33% · 3 sócios" color="#00a884" big />
            </div>
            <div style={{ display:"grid", gridTemplateColumns:"repeat(4,1fr)", gap:10, marginBottom:20 }}>
              <KPI label="Clientes Ativos"  value={ativos.length}    sub={`${suspensos.length} suspensos`} color="#e8e8ff" />
              <KPI label="Ticket Médio"     value={ativos.length>0?fmt(mrr_total/ativos.length):"—"} sub="por cliente/mês" color="#ff9800" />
              <KPI label="Custo IA/mês"     value={fmt(custo_api)}   sub={`${fmt(custo_api/Math.max(1,ativos.length))} por cliente`} color="#f44336" />
              <KPI label="Margem na IA"     value="98.6%"             sub="Custo R$0,07 · Cobra R$4,90" color="#00a884" />
            </div>
            {/* Breakdown */}
            <div style={{ background:"#0a0a14", border:"1px solid #0f0f1e", borderRadius:14, padding:20 }}>
              <div style={{ fontSize:13, fontWeight:700, marginBottom:16 }}>Receita vs Custos</div>
              {[
                { label:"Assinaturas",       value:mrr_base,     color:"#00a884" },
                { label:"Créditos IA extras", value:mrr_creditos, color:"#7c4dff" },
                { label:"Infra (Railway/Supabase/Vercel)", value:infra_fixa, color:"#f44336" },
                { label:"Claude API",        value:custo_api,    color:"#ff6d00" },
                { label:"Pagar.me 3.5%",     value:pagarme_fee,  color:"#ff9800" },
              ].map(item => (
                <div key={item.label} style={{ marginBottom:12 }}>
                  <div style={{ display:"flex", justifyContent:"space-between", marginBottom:4, fontSize:12 }}>
                    <span style={{ color:"#667781" }}>{item.label}</span>
                    <span style={{ fontWeight:700, color:item.color }}>{fmt(item.value)}</span>
                  </div>
                  <div style={{ height:4, background:"#12122a", borderRadius:2 }}>
                    <div style={{ height:"100%", width:`${Math.min(100,item.value/Math.max(1,mrr_total)*100)}%`, background:item.color, borderRadius:2 }} />
                  </div>
                </div>
              ))}
              <div style={{ marginTop:16, paddingTop:16, borderTop:"1px solid #0f0f1e", display:"flex", justifyContent:"space-between" }}>
                <span style={{ fontSize:13, color:"#667781" }}>Lucro líquido</span>
                <span style={{ fontSize:20, fontWeight:900, color:lucro>=0?"#00a884":"#f44336" }}>{fmt(lucro)}</span>
              </div>
            </div>
          </>
        )}

        {/* ── CLIENTES ── */}
        {activeTab === "clientes" && (
          <>
            <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center", marginBottom:16 }}>
              <div>
                <div style={{ fontSize:15, fontWeight:800 }}>Clientes</div>
                <div style={{ fontSize:12, color:"#54656f" }}>{ativos.length} ativos · {suspensos.length} suspensos</div>
              </div>
              <button onClick={() => setShowAdd(!showAdd)}
                style={{ padding:"8px 18px", borderRadius:9, border:"none", background:"linear-gradient(135deg,#00a884,#017561)", color:"#000", fontSize:13, fontWeight:700, cursor:"pointer", fontFamily:"inherit" }}>
                + Novo cliente
              </button>
            </div>

            {showAdd && (
              <div style={{ background:"#0a0a14", border:"1px solid #00a88433", borderRadius:14, padding:20, marginBottom:16 }}>
                <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr 1fr", gap:12, marginBottom:14 }}>
                  {[
                    { label:"NOME DA EMPRESA", key:"nome", type:"text", placeholder:"Ex: Academia FitLife" },
                    { label:"CRÉDITOS EXTRAS/MÊS", key:"creditos_extras", type:"number", placeholder:"0" },
                  ].map(f => (
                    <div key={f.key}>
                      <div style={{ fontSize:10, color:"#54656f", marginBottom:6, letterSpacing:1 }}>{f.label}</div>
                      <input type={f.type} value={novoCli[f.key]} placeholder={f.placeholder}
                        onChange={e => setNovoCli(p => ({ ...p, [f.key]: f.type==="number"?+e.target.value:e.target.value }))}
                        style={{ width:"100%", padding:"8px 12px", background:"#f0f2f5", border:"1px solid #e9edef", borderRadius:8, color:"#111b21", fontSize:13, outline:"none", fontFamily:"inherit", boxSizing:"border-box" }} />
                    </div>
                  ))}
                  <div>
                    <div style={{ fontSize:10, color:"#54656f", marginBottom:6, letterSpacing:1 }}>PLANO</div>
                    <select value={novoCli.plano} onChange={e => setNovoCli(p => ({ ...p, plano:e.target.value }))}
                      style={{ width:"100%", padding:"8px 12px", background:"#f0f2f5", border:"1px solid #e9edef", borderRadius:8, color:"#111b21", fontSize:13, outline:"none", fontFamily:"inherit", boxSizing:"border-box" }}>
                      {Object.entries(CONFIG_S.planos).map(([k,pl]) => (
                        <option key={k} value={k}>{pl.nome} — {fmt(pl.preco)}/mês</option>
                      ))}
                    </select>
                  </div>
                </div>
                <div style={{ display:"flex", gap:8 }}>
                  <button onClick={() => {
                    if (!novoCli.nome.trim()) return;
                    setClientes(p => [...p, { id:Date.now(), ...novoCli, status:"ativo" }]);
                    setNovoCli({ nome:"", plano:"pro", creditos_extras:0 });
                    setShowAdd(false);
                  }} style={{ padding:"8px 22px", borderRadius:8, border:"none", background:"#00a884", color:"#000", fontSize:13, fontWeight:700, cursor:"pointer", fontFamily:"inherit" }}>Salvar</button>
                  <button onClick={() => setShowAdd(false)} style={{ padding:"8px 14px", borderRadius:8, border:"1px solid #e9edef", background:"transparent", color:"#667781", fontSize:13, cursor:"pointer", fontFamily:"inherit" }}>Cancelar</button>
                </div>
              </div>
            )}

            <div style={{ display:"flex", flexDirection:"column", gap:8 }}>
              {clientes.length === 0 && <div style={{ textAlign:"center", padding:"50px 0", color:"#54656f", fontSize:13 }}>Nenhum cliente ainda.</div>}
              {clientes.map(c => {
                const pl = CONFIG_S.planos[c.plano] || CONFIG_S.planos.pro;
                const mrr_c = pl.preco + ((c.creditos_extras||0)/500)*29;
                const custo_c = (pl.creditos+200)*CONFIG_S.custo_credito;
                return (
                  <div key={c.id} style={{ background:"#0a0a14", border:"1px solid #0f0f1e", borderRadius:11, padding:"14px 18px", display:"flex", alignItems:"center", gap:14, opacity:c.status==="ativo"?1:0.5 }}>
                    <div style={{ width:38, height:38, borderRadius:9, background:`${pl.cor}20`, border:`1px solid ${pl.cor}44`, display:"flex", alignItems:"center", justifyContent:"center", fontSize:16, flexShrink:0 }}>
                      {c.plano==="business"?"🏢":c.plano==="pro"?"⭐":"🏪"}
                    </div>
                    <div style={{ flex:1 }}>
                      <div style={{ fontSize:14, fontWeight:700, color:"#e8e8ff" }}>{c.nome}</div>
                      <div style={{ fontSize:11, color:"#54656f" }}>
                        <span style={{ color:pl.cor, fontWeight:700 }}>{pl.nome}</span>
                        {c.creditos_extras>0 && <span> · +{fmtN(c.creditos_extras)} créditos/mês</span>}
                      </div>
                    </div>
                    <div style={{ textAlign:"right", marginRight:16 }}>
                      <div style={{ fontSize:15, fontWeight:800, color:"#00a884" }}>{fmt(mrr_c)}/mês</div>
                      <div style={{ fontSize:11, color:"#54656f" }}>Lucro: {fmt(mrr_c-custo_c)}</div>
                    </div>
                    <div style={{ display:"flex", gap:6 }}>
                      <button onClick={() => setClientes(p => p.map(x => x.id===c.id?{...x,status:x.status==="ativo"?"suspenso":"ativo"}:x))}
                        style={{ padding:"5px 12px", borderRadius:7, border:`1px solid ${c.status==="ativo"?"#f4433333":"#00a88433"}`, background:"transparent", color:c.status==="ativo"?"#f44336":"#00a884", fontSize:11, cursor:"pointer", fontFamily:"inherit" }}>
                        {c.status==="ativo"?"Suspender":"Reativar"}
                      </button>
                      <button onClick={() => setClientes(p => p.filter(x => x.id!==c.id))}
                        style={{ padding:"5px 10px", borderRadius:7, border:"1px solid #e9edef", background:"transparent", color:"#54656f", fontSize:11, cursor:"pointer", fontFamily:"inherit" }}>✕</button>
                    </div>
                  </div>
                );
              })}
            </div>
          </>
        )}

        {/* ── FINANCEIRO ── */}
        {activeTab === "financeiro" && (
          <>
            <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:14, marginBottom:14 }}>
              <div style={{ background:"#0a0a14", border:"1px solid #0f0f1e", borderRadius:14, padding:22 }}>
                <div style={{ fontSize:13, fontWeight:700, color:"#00a884", marginBottom:16 }}>📈 Receita</div>
                {[["Assinaturas mensais", mrr_base],["Créditos IA extras", mrr_creditos]].map(([l,v]) => (
                  <div key={l} style={{ display:"flex", justifyContent:"space-between", padding:"11px 0", borderBottom:"1px solid #0f0f1e" }}>
                    <span style={{ fontSize:13, color:"#667781" }}>{l}</span>
                    <span style={{ fontSize:14, fontWeight:700, color:"#00a884" }}>{fmt(v)}</span>
                  </div>
                ))}
                <div style={{ display:"flex", justifyContent:"space-between", paddingTop:12 }}>
                  <span style={{ fontSize:14, fontWeight:700 }}>Total</span>
                  <span style={{ fontSize:18, fontWeight:900, color:"#00a884" }}>{fmt(mrr_total)}</span>
                </div>
              </div>
              <div style={{ background:"#0a0a14", border:"1px solid #0f0f1e", borderRadius:14, padding:22 }}>
                <div style={{ fontSize:13, fontWeight:700, color:"#f44336", marginBottom:16 }}>📉 Custos</div>
                {[
                  ["Railway + Evolution API", infra_fixa*0.4],
                  ["Supabase", infra_fixa*0.25],
                  ["Claude API", custo_api],
                  ["Vercel", 0],
                  [`Pagar.me 3.5%`, pagarme_fee],
                  ["Domínios / Misc", 30],
                ].map(([l,v]) => (
                  <div key={l} style={{ display:"flex", justifyContent:"space-between", padding:"9px 0", borderBottom:"1px solid #0f0f1e" }}>
                    <span style={{ fontSize:12, color:"#667781" }}>{l}</span>
                    <span style={{ fontSize:13, fontWeight:700, color:"#f44336" }}>{fmt(v)}</span>
                  </div>
                ))}
                <div style={{ display:"flex", justifyContent:"space-between", paddingTop:12 }}>
                  <span style={{ fontSize:14, fontWeight:700 }}>Total</span>
                  <span style={{ fontSize:18, fontWeight:900, color:"#f44336" }}>{fmt(custo_total)}</span>
                </div>
              </div>
            </div>

            {/* Divisão entre sócios */}
            <div style={{ background:"#0a0a14", border:`1px solid ${lucro>=0?"#00a88433":"#f4433333"}`, borderRadius:14, padding:22, marginBottom:14 }}>
              <div style={{ fontSize:13, fontWeight:700, marginBottom:16 }}>💰 Divisão do Lucro — 3 Sócios</div>
              <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr 1fr 1fr 1fr", gap:12 }}>
                {[
                  { label:"Lucro Líquido", value:fmt(lucro), color:lucro>=0?"#00a884":"#f44336" },
                  { label:"Margem", value:`${margem.toFixed(0)}%`, color:margem>60?"#00a884":"#ff9800" },
                  { label:"Matheus", value:fmt(lucro_socio), color:"#7c4dff" },
                  { label:"Sócio 2", value:fmt(lucro_socio), color:"#00a884" },
                  { label:"Sócio 3", value:fmt(lucro_socio), color:"#ff9800" },
                ].map(s => (
                  <div key={s.label} style={{ background:"#f0f2f5", borderRadius:10, padding:"14px 16px", textAlign:"center" }}>
                    <div style={{ fontSize:10, color:"#54656f", marginBottom:6, letterSpacing:1 }}>{s.label.toUpperCase()}</div>
                    <div style={{ fontSize:20, fontWeight:900, color:s.color }}>{s.value}</div>
                    {["Matheus","Sócio 2","Sócio 3"].includes(s.label) && <div style={{ fontSize:10, color:"#54656f", marginTop:4 }}>1/3 do lucro</div>}
                  </div>
                ))}
              </div>
            </div>

            {/* Tabela de preços */}
            <div style={{ background:"#0a0a14", border:"1px solid #0f0f1e", borderRadius:14, padding:22 }}>
              <div style={{ fontSize:13, fontWeight:700, marginBottom:14 }}>💳 Modelo Híbrido — Preços e Margens</div>
              <table style={{ width:"100%", borderCollapse:"collapse", fontSize:12 }}>
                <thead>
                  <tr>{["Plano","Preço/mês","Créditos inclusos","Atendentes","Custo real","Margem"].map(h=>(
                    <th key={h} style={{ textAlign:"left", padding:"8px 10px", color:"#54656f", fontSize:10, letterSpacing:1, borderBottom:"1px solid #0f0f1e" }}>{h}</th>
                  ))}</tr>
                </thead>
                <tbody>
                  {Object.entries(CONFIG_S.planos).map(([k,pl]) => {
                    const c = (pl.creditos+200)*CONFIG_S.custo_credito+(infra_fixa/Math.max(1,ativos.length||5));
                    const mg = (pl.preco-c)/pl.preco*100;
                    return (
                      <tr key={k} style={{ borderBottom:"1px solid #0a0a14" }}>
                        <td style={{ padding:"11px 10px", fontWeight:700, color:pl.cor }}>{pl.nome}</td>
                        <td style={{ padding:"11px 10px", fontWeight:700, color:"#e8e8ff" }}>{fmt(pl.preco)}</td>
                        <td style={{ padding:"11px 10px", color:"#667781" }}>{pl.creditos===0?"Sem IA":fmtN(pl.creditos)}</td>
                        <td style={{ padding:"11px 10px", color:"#667781" }}>Até {pl.atendentes}</td>
                        <td style={{ padding:"11px 10px", color:"#f44336" }}>{fmt(c)}</td>
                        <td style={{ padding:"11px 10px", fontWeight:700, color:mg>70?"#00a884":"#ff9800" }}>{mg.toFixed(0)}%</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </>
        )}

        {/* ── PROJEÇÃO ── */}
        {activeTab === "projecao" && (
          <>
            <div style={{ background:"#0a0a14", border:"1px solid #0f0f1e", borderRadius:14, padding:22, marginBottom:14 }}>
              <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center", marginBottom:16 }}>
                <div>
                  <div style={{ fontSize:14, fontWeight:700 }}>🚀 Projeção — 12 meses</div>
                  <div style={{ fontSize:11, color:"#54656f" }}>Mix: 25% Starter · 55% Pro · 20% Business</div>
                </div>
                <div style={{ display:"flex", alignItems:"center", gap:10 }}>
                  <span style={{ fontSize:12, color:"#54656f" }}>Novos/mês:</span>
                  <input type="range" min={1} max={20} value={crescimento} onChange={e => setCrescimento(+e.target.value)} style={{ width:90, accentColor:"#00a884" }} />
                  <span style={{ fontSize:14, fontWeight:800, color:"#00a884", minWidth:20 }}>{crescimento}</span>
                </div>
              </div>
              <div style={{ overflowX:"auto" }}>
                <table style={{ width:"100%", borderCollapse:"collapse", fontSize:12 }}>
                  <thead>
                    <tr>{["Mês","Clientes","MRR","Custos","Lucro","Matheus","Sócio 2","Sócio 3"].map(h=>(
                      <th key={h} style={{ textAlign:"left", padding:"8px 10px", color:"#54656f", fontSize:10, letterSpacing:1, borderBottom:"1px solid #0f0f1e", whiteSpace:"nowrap" }}>{h}</th>
                    ))}</tr>
                  </thead>
                  <tbody>
                    {projection.map((r,i) => (
                      <tr key={i} style={{ borderBottom:"1px solid #0a0a14", background:i%2===0?"transparent":"#07070f" }}>
                        <td style={{ padding:"9px 10px", color:"#667781" }}>{r.mes}</td>
                        <td style={{ padding:"9px 10px", fontWeight:700, color:"#e8e8ff" }}>{r.clientes}</td>
                        <td style={{ padding:"9px 10px", fontWeight:700, color:"#00a884" }}>{fmt(r.MRR)}</td>
                        <td style={{ padding:"9px 10px", color:"#f44336" }}>{fmt(r.Custos)}</td>
                        <td style={{ padding:"9px 10px", fontWeight:700, color:r.Lucro>=0?"#00a884":"#f44336" }}>{fmt(r.Lucro)}</td>
                        <td style={{ padding:"9px 10px", fontWeight:700, color:"#7c4dff" }}>{fmt(r.Lucro/3)}</td>
                        <td style={{ padding:"9px 10px", fontWeight:700, color:"#00a884" }}>{fmt(r.Lucro/3)}</td>
                        <td style={{ padding:"9px 10px", fontWeight:700, color:"#ff9800" }}>{fmt(r.Lucro/3)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Milestones */}
            <div style={{ display:"grid", gridTemplateColumns:"repeat(4,1fr)", gap:10 }}>
              {[
                { n:10, label:"Break-even", icon:"🌱" },
                { n:25, label:"R$1k/sócio",  icon:"💪" },
                { n:50, label:"R$5k/sócio",  icon:"🚀" },
                { n:100,label:"R$10k/sócio", icon:"🏆" },
              ].map(m => {
                const mix = { starter:Math.round(m.n*0.25), pro:Math.round(m.n*0.55), business:Math.round(m.n*0.20) };
                let mrr=0;
                Object.entries(mix).forEach(([p,q]) => { mrr += CONFIG_S.planos[p].preco*q + q*0.2*49; });
                const lm = mrr - (200+m.n*8) - mrr*0.035 - m.n*200*CONFIG_S.custo_credito;
                const atingido = ativos.length >= m.n;
                return (
                  <div key={m.n} style={{ background:"#0a0a14", border:`1px solid ${atingido?"#00a88433":"#0f0f1e"}`, borderRadius:12, padding:18 }}>
                    <div style={{ fontSize:22, marginBottom:6 }}>{atingido?"✅":m.icon}</div>
                    <div style={{ fontSize:13, fontWeight:700, color:atingido?"#00a884":"#e8e8ff" }}>{m.label}</div>
                    <div style={{ fontSize:11, color:"#54656f", marginTop:2 }}>{m.n} clientes</div>
                    <div style={{ fontSize:16, fontWeight:800, color:"#00a884", marginTop:8 }}>{fmt(mrr)}</div>
                    <div style={{ fontSize:10, color:"#54656f" }}>MRR</div>
                    <div style={{ fontSize:13, fontWeight:700, color:"#7c4dff", marginTop:4 }}>{fmt(lm/3)}/sócio</div>
                  </div>
                );
              })}
            </div>
          </>
        )}

      </div>
    </div>
  );
}

export default function App() {
  const [auth, setAuth] = useState(getStoredAuth);
  const theme = "light";
  const toggleTheme = () => {};

  const handleLogin = (data) => { setStoredAuth(data); setAuth(data); };
  const handleLogout = () => { setStoredAuth(null); setAuth(null); };

  // Rota /admin → painel da Andressa (após hooks)
  if (window.location.pathname === "/admin" || window.location.pathname === "/admin/") {
    return <SuperAdminPanel />;
  }

  if (!auth) return <LoginScreen onLogin={handleLogin} theme={theme} toggleTheme={toggleTheme} />;

  return <AppInner auth={auth} onLogout={handleLogout} theme={theme} toggleTheme={toggleTheme} />;
}

// Permission helpers
const PERMS_ORDER = ["read", "read_write", "read_write_manage", "full"];
function hasPerm(user, required) {
  const userLevel = PERMS_ORDER.indexOf(user?.permissions || "read_write");
  const reqLevel  = PERMS_ORDER.indexOf(required);
  // admins always have full permissions
  if (user?.role === "admin") return true;
  return userLevel >= reqLevel;
}


// ── Upgrade Modal ─────────────────────────────────────────
function UpgradeModal({ feature, onClose, currentPlan }) {
  return (
    <div style={{ position: "fixed", inset: 0, background: "#000000cc", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 9999 }}
      onClick={onClose}>
      <div style={{ background: "#ffffff", border: "1px solid #7c4dff44", borderRadius: 18, padding: 32, maxWidth: 420, width: "90%", textAlign: "center" }}
        onMouseDown={e => e.stopPropagation()} onClick={e => e.stopPropagation()}>
        <div style={{ fontSize: 40, marginBottom: 12 }}>✨</div>
        <div style={{ fontSize: 18, fontWeight: 800, marginBottom: 8 }}>Recurso Premium</div>
        <div style={{ fontSize: 14, color: "#8696a0", marginBottom: 20 }}>{feature} está disponível a partir do plano <strong style={{ color: "#a78bfa" }}>Pro</strong>.</div>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginBottom: 20 }}>
          {[
            { plan: "Pro", price: "R$ 299/mês", color: "#00a884", features: ["8 atendentes", "2 números", "Co-pilot IA", "1.000 créditos/mês", "Checkout PIX"] },
            { plan: "Business", price: "R$ 599/mês", color: "#7c4dff", features: ["Ilimitado", "White-label", "3.000 créditos/mês", "API própria", "Suporte prioritário"] },
          ].map(p => (
            <div key={p.plan} style={{ background: "#f0f2f5", border: `1px solid ${p.color}44`, borderRadius: 12, padding: "14px 16px", textAlign: "left" }}>
              <div style={{ fontSize: 14, fontWeight: 700, color: p.color, marginBottom: 2 }}>{p.plan}</div>
              <div style={{ fontSize: 13, fontWeight: 800, marginBottom: 10 }}>{p.price}</div>
              {p.features.map(f => <div key={f} style={{ fontSize: 11, color: "#667781", marginBottom: 3 }}>✓ {f}</div>)}
            </div>
          ))}
        </div>
        <button onClick={() => window.open("https://wa.me/5511999999999?text=Quero+fazer+upgrade+do+7zap","_blank")}
          style={{ width: "100%", padding: "12px 0", borderRadius: 10, border: "none", background: "linear-gradient(135deg,#7c4dff,#5b21b6)", color: "#fff", fontSize: 14, fontWeight: 700, cursor: "pointer", fontFamily: "inherit", marginBottom: 8 }}>
          🚀 Fazer upgrade agora
        </button>
        <button onClick={onClose} style={{ width: "100%", padding: "9px 0", borderRadius: 10, border: "1px solid #e9edef", background: "transparent", color: "#667781", fontSize: 13, cursor: "pointer", fontFamily: "inherit" }}>
          Fechar
        </button>
      </div>
    </div>
  );
}

// ── Credits Widget ────────────────────────────────────────
function CreditsWidget({ credits, limit, plan, onBuyMore, onUpgrade }) {
  if (!limit || limit >= 99999) return null;
  const pct = Math.round((credits / limit) * 100);
  const isLow = pct <= 25;
  const isEmpty = credits <= 0;
  const color = isEmpty ? "#f44336" : isLow ? "#ff9800" : "#00a884";
  return (
    <div style={{ background: isEmpty ? "#f4433310" : isLow ? "#ff980010" : "#ffffff", border: `1px solid ${color}33`, borderRadius: 10, padding: "10px 14px", marginBottom: 12 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
        <span style={{ fontSize: 12, fontWeight: 700, color: "#8696a0" }}>⚡ Créditos IA</span>
        <span style={{ fontSize: 13, fontWeight: 800, color }}>{credits.toLocaleString("pt-BR")}<span style={{ color: "#667781", fontWeight: 400 }}>/{limit.toLocaleString("pt-BR")}</span></span>
      </div>
      <div style={{ height: 6, background: "#e9edef", borderRadius: 3, overflow: "hidden", marginBottom: 6 }}>
        <div style={{ height: "100%", width: `${Math.max(2, pct)}%`, background: color, borderRadius: 3, transition: "width 0.5s" }} />
      </div>
      {isEmpty && <div style={{ fontSize: 11, color: "#f44336", marginBottom: 6 }}>Créditos esgotados! IA pausada.</div>}
      {isLow && !isEmpty && <div style={{ fontSize: 11, color: "#ff9800", marginBottom: 6 }}>⚠️ Menos de 25% restante.</div>}
      <div style={{ display: "flex", gap: 6 }}>
        {plan === "starter" ? (
          <button onClick={onUpgrade} style={{ flex: 1, padding: "5px 0", borderRadius: 7, border: "none", background: "linear-gradient(135deg,#7c4dff,#5b21b6)", color: "#fff", fontSize: 11, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>
            ✨ Upgrade para mais créditos
          </button>
        ) : (isLow || isEmpty) && (
          <button onClick={onBuyMore} style={{ flex: 1, padding: "5px 0", borderRadius: 7, border: "none", background: "linear-gradient(135deg,#ff9800,#e65100)", color: "#fff", fontSize: 11, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>
            + Comprar créditos
          </button>
        )}
      </div>
    </div>
  );
}

// ── Buy Credits Modal ─────────────────────────────────────
function BuyCreditsModal({ tenantId, authHeaders, onClose, onSuccess, plan }) {
  const [buying, setBuying] = useState(false);
  const PACKS = [
    { amount: 500,  price: "R$ 29", label: "Pacote Básico",  perUnit: "R$0,058/cr" },
    { amount: 1000, price: "R$ 49", label: "Pacote Pro",     perUnit: "R$0,049/cr", highlight: true },
    { amount: 2000, price: "R$ 89", label: "Pacote Max",     perUnit: "R$0,044/cr" },
  ];
  const isStarter = plan === "starter";
  const buy = async (amount) => {
    setBuying(amount);
    try {
      const r = await fetch(`${API_URL}/credits/buy`, { method: "POST", headers: authHeaders,
        body: JSON.stringify({ tenant_id: tenantId, amount }) });
      const d = await r.json();
      if (d.ok) { onSuccess(amount); onClose(); }
    } catch(e) {}
    setBuying(false);
  };
  return (
    <div style={{ position: "fixed", inset: 0, background: "#000000cc", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 9999 }} onClick={onClose}>
      <div style={{ background: "#ffffff", border: "1px solid #ff980044", borderRadius: 18, padding: 28, maxWidth: 380, width: "90%" }}
        onMouseDown={e => e.stopPropagation()} onClick={e => e.stopPropagation()}>
        <div style={{ fontSize: 16, fontWeight: 700, marginBottom: 4 }}>⚡ Comprar Créditos de IA</div>
        <div style={{ fontSize: 12, color: "#667781", marginBottom: 16 }}>1 crédito = 1 sugestão do Co-pilot</div>

        {isStarter && (
          <div style={{ padding: "12px 14px", background: "#00a88410", border: "1px solid #00a88433", borderRadius: 10, marginBottom: 16 }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: "#00a884", marginBottom: 4 }}>💡 Dica — vale mais fazer upgrade</div>
            <div style={{ fontSize: 11, color: "#667781", lineHeight: 1.6 }}>
              No plano <strong style={{ color: "#8696a0" }}>Pro (R$149/mês)</strong> você já ganha <strong style={{ color: "#00a884" }}>300 créditos todo mês</strong> + todos os modos de IA desbloqueados.<br/>
              Comprar créditos avulsos no Starter sai mais caro a médio prazo.
            </div>
          </div>
        )}

        <div style={{ display: "flex", flexDirection: "column", gap: 8, marginBottom: 16 }}>
          {PACKS.map(p => (
            <button key={p.amount} onClick={() => buy(p.amount)} disabled={!!buying}
              style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "12px 16px", borderRadius: 10,
                border: `1px solid ${p.highlight ? "#ff980066" : "#d1d7db"}`,
                background: p.highlight ? "#ff980015" : "transparent",
                color: "#111b21", cursor: "pointer", fontFamily: "inherit" }}>
              <div style={{ textAlign: "left" }}>
                <div style={{ fontSize: 13, fontWeight: 700 }}>{p.label}</div>
                <div style={{ fontSize: 11, color: "#667781" }}>+{p.amount.toLocaleString("pt-BR")} créditos · {p.perUnit}</div>
              </div>
              <div style={{ fontSize: 15, fontWeight: 800, color: p.highlight ? "#ff9800" : "#111b21" }}>
                {buying === p.amount ? "..." : p.price}
              </div>
            </button>
          ))}
        </div>
        <button onClick={onClose} style={{ width: "100%", padding: "9px", borderRadius: 9, border: "1px solid #e9edef", background: "transparent", color: "#667781", fontSize: 13, cursor: "pointer", fontFamily: "inherit" }}>Cancelar</button>
      </div>
    </div>
  );
}

function AppInner({ auth, onLogout, theme, toggleTheme }) {
  // ── Theme tokens ──
  const T = theme === "dark" ? {
    app:      "#0a0a0f",
    topbar:   "#0d0d18",
    sidebar:  "#0d0d18",
    card:     "#13131f",
    border:   "#1a1a2e",
    border2:  "#252540",
    chatBg:   "#0a0a0f",
    msgIn:    "#1a1a2e",
    msgOut:   "#003d2e",
    input:    "#13131f",
    inputBdr: "#252540",
    text:     "#e8e8f0",
    text2:    "#888",
    text3:    "#555",
    hover:    "#1a1a2e",
    selected: "#1a1a2e",
    shadow:   "#00000080",
  } : {
    app:      "#f0f2f5",
    topbar:   "#f0f2f5",
    sidebar:  "#ffffff",
    card:     "#ffffff",
    border:   "#e9edef",
    border2:  "#d1d7db",
    chatBg:   "#efeae2",
    msgIn:    "#ffffff",
    msgOut:   "#d9fdd3",
    input:    "#f0f2f5",
    inputBdr: "#d1d7db",
    text:     "#111b21",
    text2:    "#667781",
    text3:    "#8696a0",
    hover:    "#f5f6f6",
    selected: "#e9edef",
    shadow:   "#0000001a",
  };

  const [isMobile, setIsMobile] = useState(() => window.innerWidth < 768);
  const [showMobileMenu, setShowMobileMenu] = useState(false);
  useEffect(() => {
    const handler = () => setIsMobile(window.innerWidth < 768);
    window.addEventListener("resize", handler);
    return () => window.removeEventListener("resize", handler);
  }, []);

  const [view, setView] = useState("inbox");
  const [trialInfo, setTrialInfo] = useState(null); // {status, days_left, is_blocked, plan}

  const fetchTrialStatus = async () => {
    try {
      const r = await fetch(`${API_URL}/tenant/trial-status?tenant_id=${TENANT_ID}`, { headers });
      const d = await r.json();
      setTrialInfo(d);
    } catch (e) {}
  };

  useEffect(() => { fetchTrialStatus(); }, []);
  const [conversations, setConversations] = useState([]);
  const [hasMoreConvs, setHasMoreConvs] = useState(false);
  const [loadingMoreConvs, setLoadingMoreConvs] = useState(false);
  const convListRef = useRef(null);
  const [selected, setSelected] = useState(null);
  const [messages, setMessages] = useState([]);
  const [messagesOffset, setMessagesOffset] = useState(0);
  const [hasMoreMessages, setHasMoreMessages] = useState(false);
  const [loadingMoreMsgs, setLoadingMoreMsgs] = useState(false);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(true);
  const [initialLoad, setInitialLoad] = useState(true); // true até primeira carga completar
  const [sending, setSending] = useState(false);
  const [filter, setFilter] = useState("all");
  const [search, setSearch] = useState("");
  const [unreadFilter, setUnreadFilter] = useState("all"); // all | unread
  // ── Toast notification system ──
  const [toast, setToast] = useState(null); // { msg, color, loading }
  const toastTimerRef = useRef(null);
  const showToast = (msg, color = "#00a884", loading = false) => {
    if (toastTimerRef.current) clearTimeout(toastTimerRef.current);
    setToast({ msg, color, loading });
    if (!loading) toastTimerRef.current = setTimeout(() => setToast(null), 3500);
  };
  const hideToast = () => { if (toastTimerRef.current) clearTimeout(toastTimerRef.current); setToast(null); };
  // ── Per-action loading states ──
  const [changingStatus, setChangingStatus] = useState(false); // status dropdown
  const [assigningConv, setAssigningConv] = useState(false);   // assign modal
  const [inactiveDays, setInactiveDays] = useState(null); // null | 3 | 7 | 15  (days without response)
  const [instanceFilter, setInstanceFilter] = useState(() => {
    // Restore last selected instance from sessionStorage
    try { return sessionStorage.getItem("7crm_instance") || null; } catch { return null; }
  }); // null = all | instance_name string
  const [resumingConv, setResumingConv] = useState(null); // conv id being resumed
  const [agents, setAgents] = useState([]);
  const [pendingTasksMap, setPendingTasksMap] = useState({}); // convId → count
  const [labels, setLabels] = useState([]);
  const [labelsError, setLabelsError] = useState("");
  const [aiCredits, setAiCredits] = useState(null); // { credits, limit, plan, pct, warning }
  const [showUpgrade, setShowUpgrade] = useState(null); // feature name string
  const [showBuyCredits, setShowBuyCredits] = useState(false);
  const [waInstances, setWaInstances] = useState([]); // for disconnect banner
  const selectInstance = (name) => {
    setInstanceFilter(name);
    try {
      if (name) sessionStorage.setItem("7crm_instance", name);
      else sessionStorage.removeItem("7crm_instance");
    } catch {}
    // Save to server so preference persists across devices
    if (auth?.user?.id) {
      fetch(`${API_URL}/users/me/preferences?user_id=${auth.user.id}`, {
        method: "PUT", headers,
        body: JSON.stringify({ instance_filter: name || null })
      }).catch(() => {});
    }
  };

  useEffect(() => {
    const fetchWaStatus = async () => {
      try {
        const r = await fetch(`${API_URL}/whatsapp/tenant-instances?tenant_id=${TENANT_ID}`, { headers });
        const d = await r.json();
        setWaInstances(d.instances || []);
      } catch (e) {}
    };
    fetchWaStatus();
    const t = setInterval(fetchWaStatus, 30000);
    return () => clearInterval(t);
  }, []);
  const fetchLabels = useCallback(async () => {
    setLabelsError("");
    try {
      const r = await fetch(`${API_URL}/labels?tenant_id=${TENANT_ID}`, { headers });
      if (!r.ok) {
        const fallback = loadLabels();
        setLabels(fallback);
        if (r.status === 404) setLabelsError("backend_not_deployed");
        return;
      }
      const d = await r.json();
      const fromApi = d.labels || [];

      // AUTO-MIGRATE: localStorage has labels with local IDs → create them in DB
      const local = loadLabels();
      const localUnsynced = local.filter(l => !l._synced && !fromApi.find(a => a.name === l.name));
      if (localUnsynced.length > 0) {
        const migrated = [...fromApi];
        for (const lbl of localUnsynced) {
          try {
            const cr = await fetch(`${API_URL}/labels`, { method: "POST", headers,
              body: JSON.stringify({ tenant_id: TENANT_ID, name: lbl.name, color: lbl.color }) });
            if (cr.ok) { const cd = await cr.json(); migrated.push(cd.label); }
          } catch (_) {}
        }
        // Clear localStorage now that labels are in DB
        saveLabels([]);
        setLabels(migrated);
        return;
      }

      setLabels(fromApi.length > 0 ? fromApi : local);
    } catch (e) {
      setLabels(loadLabels());
    }
  }, []);
  const [showAssign, setShowAssign] = useState(false);
  const [showLabelPicker, setShowLabelPicker] = useState(false);
  const [showLabelManager, setShowLabelManager] = useState(false);
  const [showColManager, setShowColManager] = useState(false);
  const [showTasks, setShowTasks] = useState(false);
  const [noteMode, setNoteMode] = useState(false);
  const [kanbanCols, setKanbanCols] = useState(loadColumns);
  const [suggestion, setSuggestion] = useState("");
  const [loadingSuggest, setLoadingSuggest] = useState(false);
  const [copilotPrompt, setCopilotPrompt] = useState("");
  const [copilotPromptSummary, setCopilotPromptSummary] = useState("");
  const [copilotAutoMode, setCopilotAutoMode] = useState("off"); // off | schedule | always | per_conv
  const [copilotScheduleStart, setCopilotScheduleStart] = useState("18:00");
  const [copilotScheduleEnd, setCopilotScheduleEnd] = useState("09:00");
  const [savingPrompt, setSavingPrompt] = useState(false);
  const [promptSaved, setPromptSaved] = useState(false);
  const bottomRef = useRef(null);
  const chatScrollRef = useRef(null);
  const pollRef = useRef(null);
  const labelOverrideRef = useRef({});
  const kanbanOverrideRef = useRef({}); // { convId: { labels, until } }
  const autoProcessedRef = useRef(new Set()); // msgIds already auto-replied
  const autoProcessingRef = useRef(new Set()); // per-conv processing lock
  const autoModeRef = useRef({}); // { convId: boolean } — persists across polls

  const mergeConvs = useCallback((list) => {
    const now = Date.now();
    return list.map(c => {
      const ov = labelOverrideRef.current[c.id];
      const labels = (ov && ov.until > now) ? ov.labels : c.labels;
      const auto_mode = autoModeRef.current[c.id] !== undefined ? autoModeRef.current[c.id] : (c.auto_mode || false);
      // Preserve kanban_stage from local optimistic state (overrides stale cache for 30s)
      const ko = kanbanOverrideRef.current[c.id];
      const kanban_stage = (ko && ko.until > now) ? ko.stage : c.kanban_stage;
      return { ...c, labels, auto_mode, kanban_stage };
    });
  }, []);

  const fetchConversations = useCallback(async () => {
    try {
      const r = await fetch(`${API_URL}/conversations?tenant_id=${TENANT_ID}&user_id=${auth.user.id}&limit=50`, { headers });
      if (r.status === 401) {
        const err = await r.json().catch(() => ({}));
        if ((err.detail || "").includes("Sessão encerrada")) {
          onLogout();
          alert("\u26a0\ufe0f Sua sessão foi encerrada pois outro dispositivo fez login com esta conta.");
        }
        return;
      }
      const d = await r.json();
      const fresh = mergeConvs(d.conversations || []);
      setConversations(prev => {
        // Se o usuário já carregou mais páginas (prev.length > fresh.length),
        // ATUALIZA as conversas existentes em vez de substituir tudo.
        // Isso evita que o poll de 4s apague as páginas extras carregadas.
        if (prev.length > fresh.length) {
          const freshMap = new Map(fresh.map(c => [c.id, c]));
          // Update existing entries that appear in fresh; keep the rest as-is
          return prev.map(c => freshMap.has(c.id) ? mergeConvs([{ ...c, ...freshMap.get(c.id) }])[0] : c);
        }
        return fresh;
      });
      setHasMoreConvs(d.has_more === true);
      setSelected(prev => {
        if (!prev) return prev;
        const updated = fresh.find(c => c.id === prev.id);
        return updated ? { ...prev, ...updated } : prev;
      });
    } catch (e) {}
    setLoading(false);
    setInitialLoad(false);
  }, [filter, mergeConvs]);

  const fetchMoreConversations = useCallback(async () => {
    if (loadingMoreConvs) return;
    setLoadingMoreConvs(true);
    try {
      const last = conversations[conversations.length - 1];
      if (!last?.last_message_at) return;
      const before = encodeURIComponent(last.last_message_at);
      const r = await fetch(`${API_URL}/conversations?tenant_id=${TENANT_ID}&user_id=${auth.user.id}&limit=50&before=${before}`, { headers });
      const d = await r.json();
      const more = mergeConvs(d.conversations || []);
      // Append avoiding duplicates
      setConversations(prev => {
        const ids = new Set(prev.map(c => c.id));
        return [...prev, ...more.filter(c => !ids.has(c.id))];
      });
      setHasMoreConvs(d.has_more === true);
    } catch (e) {}
    setLoadingMoreConvs(false);
  }, [conversations, loadingMoreConvs, mergeConvs]);
  const fetchAllConversations = useCallback(async () => {
    try {
      const r = await fetch(`${API_URL}/conversations?tenant_id=${TENANT_ID}&user_id=${auth.user.id}&limit=50`, { headers });
      const d = await r.json();
      const fresh = mergeConvs(d.conversations || []);
      setConversations(prev => {
        if (prev.length > fresh.length) {
          const freshMap = new Map(fresh.map(c => [c.id, c]));
          return prev.map(c => freshMap.has(c.id) ? mergeConvs([{ ...c, ...freshMap.get(c.id) }])[0] : c);
        }
        return fresh;
      });
      setHasMoreConvs(d.has_more === true);
      setSelected(prev => {
        if (!prev) return prev;
        const updated = fresh.find(c => c.id === prev.id);
        return updated ? { ...prev, ...updated } : prev;
      });
    } catch (e) {}
    setLoading(false);
    setInitialLoad(false);
  }, [mergeConvs]);
  const lazySyncChat = useCallback(async (conv) => {
    // Busca mensagens do WhatsApp para essa conversa específica
    // Chamado uma vez quando o atendente abre a conversa
    const phone = conv?.contacts?.phone;
    if (!phone || !conv?.id) return;
    try {
      await fetch(`${API_URL}/whatsapp/sync-chat`, {
        method: "POST", headers,
        body: JSON.stringify({
          tenant_id: TENANT_ID,
          conversation_id: conv.id,
          phone: phone,
          instance: "default"
        })
      });
    } catch (e) {}
  }, []);

  const PAGE_SIZE = 50;
  const [loadingMessages, setLoadingMessages] = useState(false);
  const [messagesError, setMessagesError] = useState(null);

  const fetchMessages = useCallback(async (convId) => {
    // Load full history from WAHA + DB (called once on conversation open)
    setMessagesError(null);
    try {
      const r = await fetch(`${API_URL}/conversations/${convId}/history?limit=40`, { headers });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = await r.json();
      setMessages(d.messages || []);
      setHasMoreMessages(false);
      setMessagesOffset(0);
    } catch (e) {
      try {
        const r2 = await fetch(`${API_URL}/conversations/${convId}/messages?limit=50`, { headers });
        const d2 = await r2.json();
        setMessages(d2.messages || []);
        setHasMoreMessages(d2.has_more === true);
      } catch {
        setMessagesError("Não foi possível carregar. Toque para tentar novamente.");
      }
    }
  }, []);
  const fetchMoreMessages = useCallback(async (convId, currentMessages) => {
    setLoadingMoreMsgs(true);
    try {
      const oldest = currentMessages[0]?.created_at;
      if (!oldest) return;
      const r = await fetch(`${API_URL}/conversations/${convId}/messages?limit=50&before=${encodeURIComponent(oldest)}`, { headers });
      const d = await r.json();
      const older = d.messages || [];
      setMessages(prev => [...older, ...prev]);
      setHasMoreMessages(d.has_more === true);
    } catch (e) {}
    setLoadingMoreMsgs(false);
  }, []);
  const fetchAgents = useCallback(async () => {
    try { const r = await fetch(`${API_URL}/users?tenant_id=${TENANT_ID}`, { headers }); const d = await r.json(); setAgents(d.users || []); } catch (e) {}
  }, []);
  const loadUserPreferences = useCallback(async () => {
    try {
      const r = await fetch(`${API_URL}/users/me/preferences?user_id=${auth.user.id}`, { headers });
      if (!r.ok) return;
      const d = await r.json();
      const prefs = d.preferences || {};
      if (prefs.instance_filter) {
        setInstanceFilter(prefs.instance_filter);
        try { sessionStorage.setItem("7crm_instance", prefs.instance_filter); } catch {}
      }
    } catch (e) {}
  }, []);

  const fetchTenant = useCallback(async () => {
    try { const r = await fetch(`${API_URL}/tenant?tenant_id=${TENANT_ID}`, { headers }); const d = await r.json();
      setCopilotPrompt(d.copilot_prompt || "");
      if (d.copilot_prompt_summary) setCopilotPromptSummary(d.copilot_prompt_summary);
      setCopilotAutoMode(d.copilot_auto_mode || "off");
      setCopilotScheduleStart(d.copilot_schedule_start || "18:00");
      setCopilotScheduleEnd(d.copilot_schedule_end || "09:00");
      if (d.ai_credits !== undefined) setAiCredits({ credits: d.ai_credits, limit: d.ai_credits_limit, plan: d.plan, pct: d.ai_credits_pct, warning: d.ai_credits_pct <= 25 });
    } catch (e) {}
  }, []);

  const fetchCredits = useCallback(async () => {
    try {
      const r = await fetch(`${API_URL}/credits?tenant_id=${TENANT_ID}`, { headers });
      const d = await r.json();
      setAiCredits({ credits: d.credits, limit: d.limit, plan: d.plan, pct: d.pct, warning: d.warning });
    } catch(e) {}
  }, []);
  const fetchPendingTasks = useCallback(async () => {
    try {
      const r = await fetch(`${API_URL}/tasks?tenant_id=${TENANT_ID}`, { headers });
      const d = await r.json();
      const map = {};
      (d.tasks || []).forEach(t => {
        if (!t.done) map[t.conversation_id] = (map[t.conversation_id] || 0) + 1;
      });
      setPendingTasksMap(map);
    } catch (e) {}
  }, []);
  const savePrompt = async () => {
    setSavingPrompt(true); setPromptSaved(false);
    try {
      await fetch(`${API_URL}/tenant/copilot-prompt`, { method: "PUT", headers, body: JSON.stringify({ tenant_id: TENANT_ID, copilot_prompt: copilotPrompt, copilot_auto_mode: copilotAutoMode, copilot_schedule_start: copilotScheduleStart, copilot_schedule_end: copilotScheduleEnd }) });
      setPromptSaved(true); setTimeout(() => setPromptSaved(false), 3000);
    } catch (e) {}
    setSavingPrompt(false);
  };

  const syncContactNames = async () => {
    if (!window.confirm("Buscar o nome real de todos os contatos direto do WhatsApp? Pode demorar alguns segundos.")) return;
    try {
      showToast("🔄 Sincronizando nomes...");
      const inst = instanceFilter || instances[0]?.name || "default";
      const r = await fetch(`${API_URL}/contacts/sync-names`, { method: "POST", headers, body: JSON.stringify({ tenant_id: TENANT_ID, instance: inst }) });
      const d = await r.json();
      showToast(`✅ ${d.updated || 0} nomes sincronizados!`);
      fetchConversations();
    } catch (e) {
      showToast("❌ Erro ao sincronizar nomes.");
    }
  };

  useEffect(() => {
    const isMulti = view === "kanban" || view === "leads";
    const fn = isMulti ? fetchAllConversations : fetchConversations;
    fn(); clearInterval(pollRef.current); pollRef.current = setInterval(fn, 4000);
    return () => clearInterval(pollRef.current);
  }, [fetchConversations, fetchAllConversations, view, filter]);
  const backgroundRefreshMessages = useCallback(async (convId) => {
    // Silent background refresh — no spinner, no skeleton, just appends new messages
    try {
      const r = await fetch(`${API_URL}/conversations/${convId}/messages?limit=50`, { headers });
      if (!r.ok) return;
      const d = await r.json();
      const fresh = d.messages || [];
      if (fresh.length === 0) return;
      setMessages(prev => {
        // Merge: keep existing, append truly new ones (by id)
        const existingIds = new Set(prev.map(m => m.id || m.waha_id));
        const newOnes = fresh.filter(m => !existingIds.has(m.id) && !existingIds.has(m.waha_id));
        if (newOnes.length === 0) return prev; // no change — avoid re-render
        return [...prev, ...newOnes];
      });
    } catch (e) {}
  }, []);

  useEffect(() => {
    if (!selected) return;
    setMessages([]);
    setMessagesOffset(0);
    setHasMoreMessages(false);
    setLoadingMessages(true);
    fetchMessages(selected.id, false).finally(() => setLoadingMessages(false));
    const t = setInterval(() => backgroundRefreshMessages(selected.id), 3000);
    return () => clearInterval(t);
  }, [selected?.id]);
  const prevMsgCountRef = useRef(0);
  useEffect(() => {
    const count = messages.length;
    // Scroll to bottom only when: first load OR new message appended (not when prepending older ones)
    if (count > 0 && count >= prevMsgCountRef.current) {
      const lastMsg = messages[messages.length - 1];
      const prevLast = prevMsgCountRef._lastId;
      if (prevMsgCountRef.current === 0 || lastMsg?.id !== prevLast) {
        bottomRef.current?.scrollIntoView({ behavior: prevMsgCountRef.current === 0 ? "instant" : "smooth" });
        prevMsgCountRef._lastId = lastMsg?.id;
      }
    }
    prevMsgCountRef.current = count;
  }, [messages]);
  useEffect(() => { fetchAgents(); fetchTenant(); fetchCredits(); fetchPendingTasks(); fetchLabels(); loadUserPreferences(); const t = setInterval(fetchPendingTasks, 30000); const t2 = setInterval(fetchCredits, 60000); return () => { clearInterval(t); clearInterval(t2); }; }, [fetchAgents, fetchTenant, fetchCredits, fetchPendingTasks, fetchLabels, loadUserPreferences]);

  const resumeConversation = async (conv) => {
    if (resumingConv) return;
    setResumingConv(conv.id);
    setSelected(conv);
    setSuggestion("");
    try {
      // Fetch messages to give AI context
      const mr = await fetch(`${API_URL}/conversations/${conv.id}/messages`, { headers });
      const md = await mr.json();
      const msgs = (md.messages || []).slice(-20); // last 20 msgs for context
      const lastMsgDate = conv.last_message_at ? new Date(conv.last_message_at).toLocaleDateString("pt-BR") : "desconhecida";
      const daysInactive = conv.last_message_at
        ? Math.floor((Date.now() - new Date(conv.last_message_at).getTime()) / (1000 * 60 * 60 * 24))
        : 0;
      const history = msgs.map(m => `${m.direction === "inbound" ? "Cliente" : "Atendente"}: ${m.content}`).join("\n");
      const contactName = conv.contacts?.name || conv.contacts?.phone || "o cliente";

      const response = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model: "claude-sonnet-4-20250514",
          max_tokens: 300,
          system: `Você é um assistente de atendimento ao cliente. Gere uma mensagem curta e natural para retomar contato com um cliente que não responde há ${daysInactive} dias. 
A mensagem deve:
- Ser informal e amigável, como uma pessoa real escreveria
- Fazer referência ao contexto da última conversa
- Ter no máximo 2-3 frases curtas
- NÃO mencionar que é IA
- NÃO usar emojis em excesso (máximo 1)
- Responda APENAS com a mensagem, sem explicações`,
          messages: [{
            role: "user",
            content: `Nome do cliente: ${contactName}\nÚltima mensagem: ${lastMsgDate}\nDias sem resposta: ${daysInactive}\n\nHistórico recente:\n${history}\n\nGere uma mensagem de retomada de contato.`
          }]
        })
      });
      const data = await response.json();
      const msg = data.content?.[0]?.text?.trim() || "";
      if (msg) {
        setSuggestion(msg);
        setInput(msg);
        setView("inbox");
      }
    } catch (e) { console.error("Resume error:", e); }
    setResumingConv(null);
  };

  const sendMessage = async () => {
    if (!input.trim() || !selected || sending) return;
    setSending(true);
    try { await fetch(`${API_URL}/conversations/${selected.id}/messages`, { method: "POST", headers, body: JSON.stringify({ conversation_id: selected.id, text: input.trim(), is_internal_note: noteMode }) }); setInput(""); setNoteMode(false); await backgroundRefreshMessages(selected.id); await fetchConversations(); } catch (e) {}
    setSending(false);
  };

  const changeStatus = async (convId, newStatus) => {
    if (changingStatus) return;
    setChangingStatus(true);
    const labels = { resolved: "Resolvendo...", pending: "Colocando em espera...", open: "Reabrindo..." };
    showToast(labels[newStatus] || "Atualizando...", "#00a884", true);
    const endpoint = newStatus === "resolved" ? "resolve" : newStatus === "pending" ? "pending" : "reopen";
    try {
      await fetch(`${API_URL}/conversations/${convId}/${endpoint}`, { method: "PUT", headers });
      setSelected(prev => prev ? { ...prev, status: newStatus } : null);
      setConversations(prev => prev.map(c => c.id === convId ? { ...c, status: newStatus } : c));
      const doneLabels = { resolved: "✓ Conversa resolvida", pending: "⏸ Em espera", open: "▶ Conversa reaberta" };
      showToast(doneLabels[newStatus] || "Status atualizado", "#00a884");
      if (newStatus !== filter && filter !== "all") { setTimeout(() => { setSelected(null); fetchConversations(); }, 300); }
    } catch (e) { showToast("Erro ao atualizar status", "#f44336"); }
    setChangingStatus(false);
  };

  const assignConv = async (agent) => {
    setAssigningConv(true);
    showToast(`Atribuindo para ${agent.name}...`, "#00a884", true);
    try {
      await fetch(`${API_URL}/conversations/${selected.id}/assign`, { method: "PUT", headers, body: JSON.stringify({ user_id: agent.id }) });
      const patch = { assigned_to: agent.id, assigned_agent: agent.name };
      setSelected(prev => ({ ...prev, ...patch }));
      setConversations(prev => prev.map(c => c.id === selected.id ? { ...c, ...patch } : c));
      showToast(`✓ Atribuído para ${agent.name}`);
    } catch (e) { showToast("Erro ao atribuir conversa", "#f44336"); }
    setAssigningConv(false);
    setShowAssign(false);
  };
  const toggleLabel = async (label) => {
    if (!selected) return;
    const previous = selected.labels || [];
    const exists = previous.some(l => l.id === label.id);
    const updated = exists ? previous.filter(l => l.id !== label.id) : [...previous, label];
    // Optimistic update — show immediately, protect from polls
    setSelected(prev => ({ ...prev, labels: updated }));
    setConversations(prev => prev.map(c => c.id === selected.id ? { ...c, labels: updated } : c));
    labelOverrideRef.current[selected.id] = { labels: updated, until: Date.now() + 120000 };
    try {
      const resp = await fetch(`${API_URL}/conversations/${selected.id}/labels`, { method: "PUT", headers, body: JSON.stringify({ label_ids: updated.map(l => l.id) }) });
      if (resp.ok) {
        // Success — keep override for 2 min so next polls don't overwrite
        labelOverrideRef.current[selected.id] = { labels: updated, until: Date.now() + 120000 };
      } else {
        const errBody = await resp.json().catch(() => ({}));
        console.error("Label PUT failed:", resp.status, errBody);
        showToast(`Erro ao salvar etiqueta: ${errBody.detail || resp.status}`, "#f44336");
        // Still keep optimistic state — don't revert, user already saw it
        labelOverrideRef.current[selected.id] = { labels: updated, until: Date.now() + 120000 };
      }
    } catch (e) {
      console.error("Label update exception:", e);
      // Network error — keep state, will retry on next interaction
      labelOverrideRef.current[selected.id] = { labels: updated, until: Date.now() + 120000 };
    }
  };
  const moveKanbanCard = async (conv, newStage) => {
    if (!instanceFilter) { showToast("⚠️ Selecione um número de telefone primeiro", "#ff9800"); return; }
    // Optimistic: store override for 30s so polling doesn't revert it
    kanbanOverrideRef.current[conv.id] = { stage: newStage, until: Date.now() + 30000 };
    setConversations(prev => prev.map(c => c.id === conv.id ? { ...c, kanban_stage: newStage } : c));
    try { await fetch(`${API_URL}/conversations/${conv.id}/kanban`, { method: "PUT", headers, body: JSON.stringify({ stage: newStage }) }); } catch (e) {}
  };

  const moveLabelCard = async (conv, fromLabelId, targetLabel) => {
    if (!instanceFilter) { showToast("⚠️ Selecione um número de telefone primeiro", "#ff9800"); return; }
    const current = conv.labels || [];
    let updated;
    if (!targetLabel) {
      // Soltar em "Sem etiqueta" → remove só a etiqueta de origem
      updated = current.filter(l => l.id !== fromLabelId);
    } else if (!fromLabelId) {
      // Veio de "Sem etiqueta" → apenas adiciona
      const alreadyHas = current.some(l => l.id === targetLabel.id);
      updated = alreadyHas ? current : [...current, targetLabel];
    } else {
      // SWAP: remove a de origem, adiciona a de destino
      const withoutFrom = current.filter(l => l.id !== fromLabelId);
      const alreadyHas = withoutFrom.some(l => l.id === targetLabel.id);
      updated = alreadyHas ? withoutFrom : [...withoutFrom, targetLabel];
    }
    // Optimistic: store override for 60s so polling doesn't revert it
    labelOverrideRef.current[conv.id] = { labels: updated, until: Date.now() + 60000 };
    setConversations(prev => prev.map(c => c.id === conv.id ? { ...c, labels: updated } : c));
    try { await fetch(`${API_URL}/conversations/${conv.id}/labels`, { method: "PUT", headers, body: JSON.stringify({ label_ids: updated.map(l => l.id) }) }); } catch (e) {}
  };
  const fetchSuggestion = async () => {
    if (!selected || loadingSuggest) return; setLoadingSuggest(true); setSuggestion("");
    try {
      if (aiCredits && aiCredits.credits <= 0) {
        if (aiCredits.plan === "starter") { setShowUpgrade("Co-pilot IA"); return; }
        setShowBuyCredits(true); return;
      }
      const r = await fetch(`${API_URL}/conversations/${selected.id}/suggest?tenant_id=${TENANT_ID}`, { headers });
      if (r.status === 402) { const e = await r.json(); showToast(e.detail, "#f44336"); fetchCredits(); return; }
      const d = await r.json();
      setSuggestion(d.suggestion || "");
      // Deduct 1 credit optimistically
      setAiCredits(prev => prev ? { ...prev, credits: Math.max(0, prev.credits - 1), pct: Math.round(Math.max(0, prev.credits - 1) / prev.limit * 100) } : prev);
    } catch (e) { setSuggestion("Erro ao buscar sugestão."); }
    setLoadingSuggest(false);
  };

  // ─── Auto-pilot helpers ────────────────────────────────────────────────
  const isAutoActive = useCallback((conv) => {
    if (copilotAutoMode === "always") return true;
    if (copilotAutoMode === "per_conv") return !!conv?.auto_mode;
    if (copilotAutoMode === "schedule") {
      const now = new Date();
      const [sh, sm] = (copilotScheduleStart || "18:00").split(":").map(Number);
      const [eh, em] = (copilotScheduleEnd || "09:00").split(":").map(Number);
      const nowMins = now.getHours() * 60 + now.getMinutes();
      const startMins = sh * 60 + sm;
      const endMins = eh * 60 + em;
      // Handles overnight ranges (e.g. 18:00 → 09:00)
      if (startMins > endMins) return nowMins >= startMins || nowMins < endMins;
      return nowMins >= startMins && nowMins < endMins;
    }
    return false;
  }, [copilotAutoMode, copilotScheduleStart, copilotScheduleEnd]);

  const autoReply = useCallback(async (conv) => {
    if (!isAutoActive(conv)) return;
    // Per-conversation lock — don't block other convs
    if (autoProcessingRef.current.has(conv.id)) return;
    autoProcessingRef.current.add(conv.id);
    try {
      const r = await fetch(`${API_URL}/conversations/${conv.id}/messages`, { headers });
      const d = await r.json();
      const msgs = d.messages || [];
      const lastInbound = [...msgs].reverse().find(m => m.direction === "inbound");
      if (!lastInbound) return;
      if (autoProcessedRef.current.has(lastInbound.id)) return;
      autoProcessedRef.current.add(lastInbound.id);
      const sr = await fetch(`${API_URL}/conversations/${conv.id}/suggest?tenant_id=${TENANT_ID}`, { headers });
      if (sr.status === 402) { console.warn("Auto-reply: sem créditos"); return; }
      if (!sr.ok) { console.error("Suggest failed:", sr.status); return; }
      const sd = await sr.json();
      const suggestion = (sd.suggestion || "").trim();
      if (!suggestion) return;
      await fetch(`${API_URL}/conversations/${conv.id}/messages`, {
        method: "POST", headers,
        body: JSON.stringify({ conversation_id: conv.id, text: suggestion, is_internal_note: false })
      });
      console.log(`🤖 Auto-reply sent to conv ${conv.id}`);
    } catch (e) {
      console.error("Auto-reply error:", e);
    } finally {
      autoProcessingRef.current.delete(conv.id);
    }
  }, [isAutoActive, headers]);

  // Keep a ref to conversations so the engine always has fresh data without recreating interval
  const conversationsRef = useRef(conversations);
  useEffect(() => { conversationsRef.current = conversations; }, [conversations]);

  // Engine: scan all conversations every 6s for new inbound messages when auto active
  useEffect(() => {
    if (copilotAutoMode === "off") return;
    console.log("🤖 Auto-pilot engine started, mode:", copilotAutoMode);
    const tick = async () => {
      const now = Date.now();
      const convs = conversationsRef.current.filter(c => {
        if (!isAutoActive(c)) return false;
        // Primary: has unread messages
        if ((c.unread_count || 0) > 0) return true;
        // Fallback: last message was recent (< 3 min) — unread_count may not be set yet
        const lastMsgAge = c.last_message_at ? (now - new Date(c.last_message_at).getTime()) : Infinity;
        return lastMsgAge < 3 * 60 * 1000;
      });
      if (convs.length > 0) console.log(`🤖 Checking ${convs.length} conv(s) for auto-reply`);
      await Promise.all(convs.map(conv => autoReply(conv)));
    };
    const t = setInterval(tick, 6000);
    tick(); // run immediately on activation
    return () => { clearInterval(t); console.log("🤖 Auto-pilot engine stopped"); };
  }, [copilotAutoMode, isAutoActive, autoReply]);

  // Permission flags
  const canWrite  = auth.user?.role === "admin" || PERMS_ORDER.indexOf(auth.user?.permissions || "read_write") >= PERMS_ORDER.indexOf("read_write");
  const canManage = auth.user?.role === "admin" || PERMS_ORDER.indexOf(auth.user?.permissions || "read_write") >= PERMS_ORDER.indexOf("read_write_manage");
  const canDelete = auth.user?.role === "admin" || PERMS_ORDER.indexOf(auth.user?.permissions || "read_write") >= PERMS_ORDER.indexOf("full");

  const unreadCount = conversations.filter(c => c.unread_count > 0).length;
  const filtered = conversations.filter(c => {
    const name = (c.contacts?.name || "").toLowerCase();
    const phone = (c.contacts?.phone || "").replace(/\D/g, "");
    const q = search.toLowerCase().trim();
    const qDigits = q.replace(/\D/g, "");
    const matchSearch = !q || name.includes(q) || (qDigits && phone.includes(qDigits)) || phone.includes(q);
    const matchUnread = unreadFilter === "all" || c.unread_count > 0;
    let matchInactive = true;
    if (inactiveDays) {
      const lastMsg = c.last_message_at ? new Date(c.last_message_at).getTime() : 0;
      const daysAgo = (Date.now() - lastMsg) / (1000 * 60 * 60 * 24);
      matchInactive = daysAgo >= inactiveDays;
    }
    // instance_name === null → legacy conv created before multi-instance tracking.
    // Show under ALL instance filters until backfill assigns the correct instance.
    const matchInstance = !instanceFilter
      || c.instance_name === instanceFilter
      || !c.instance_name;  // null = legacy, always visible
    return matchSearch && matchUnread && matchInactive && matchInstance;
  });

  const totalPendingTasks = Object.values(pendingTasksMap).reduce((a, b) => a + b, 0);
  const WORK_TABS = [
    { id: "inbox", label: "📥 Inbox" },
    { id: "leads", label: "🏷 Leads" },
    { id: "kanban", label: "🗂 Kanban" },
    { id: "tasks_global", label: "✅ Tarefas" },
    { id: "disparos", label: "📢 Disparos" },
    { id: "config", label: "⚙️ Config IA" },
    ...(auth.user.role === "admin" && trialInfo?.plan !== "trial" ? [{ id: "onboarding", label: "🧠 Onboarding IA" }] : []),
    ...(auth.user.role === "admin" ? [{ id: "relatorios", label: "📈 Relatórios" }] : []),
    ...(trialInfo?.status === "trial" ? [{ id: "upgrade", label: "⭐ Assinar" }] : []),
  ];

  const IS_SOCIO = SOCIOS_EMAILS.includes(auth.user?.email);
  const ADMIN_TABS = [
    ...(auth.user.role === "admin" ? [{ id: "whatsapp", label: "📱 WhatsApp" }] : []),
    ...(auth.user.role === "admin" ? [{ id: "admin", label: "🔐 Admin" }] : []),
    ...(IS_SOCIO ? [{ id: "socios", label: "📊 Sócios" }] : []),
  ];

  return (
    <div style={{ display: "flex", height: "100dvh", width: "100vw", flexDirection: "column", background: T.app, color: T.text, fontFamily: "'DM Sans', 'Segoe UI', sans-serif", overflow: "hidden" }}>
      {/* TopBar */}
      <div style={{ height: isMobile ? 52 : 48, flexShrink: 0, borderBottom: `1px solid ${T.border}`, background: T.topbar, display: "flex", alignItems: "center", padding: isMobile ? "0 12px" : "0 20px", gap: isMobile ? 8 : 24 }}>
        {/* Logo */}
        <div style={{ display: "flex", alignItems: "center", gap: 6, flexShrink: 0 }}>
          <div style={{ width: 26, height: 26, borderRadius: 7, background: "linear-gradient(135deg, #00a884, #017561)", display: "flex", alignItems: "center", justifyContent: "center" }}>
            <span style={{ fontSize: 15, fontWeight: 900, color: "#fff", lineHeight: 1 }}>7</span>
          </div>
          {!isMobile && <span style={{ fontWeight: 800, fontSize: 15, color: "#00a884", letterSpacing: "-0.3px" }}>CRM</span>}
        </div>

        {/* 📱 Número ativo — sempre visível no topo */}
        {instanceFilter && waInstances.length >= 1 && (() => {
          const activeInst = waInstances.find(i => i.instance_name === instanceFilter);
          if (!activeInst) return null;
          return (
            <div style={{ display: "flex", alignItems: "center", gap: 6, padding: isMobile ? "4px 8px" : "4px 12px", borderRadius: 20, background: "#00a88415", border: "1px solid #00a88430", flexShrink: 0, maxWidth: isMobile ? 130 : 200 }}>
              <span style={{ width: 7, height: 7, borderRadius: "50%", background: activeInst.connected ? "#00a884" : "#f44336", flexShrink: 0, display: "inline-block" }} />
              <span style={{ fontSize: isMobile ? 10 : 12, fontWeight: 700, color: "#00a884", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                📱 {activeInst.label || activeInst.instance_name}
              </span>
              <button onClick={() => selectInstance(null)} title="Trocar número" style={{ background: "none", border: "none", cursor: "pointer", color: "#00a884", fontSize: 12, padding: 0, lineHeight: 1, flexShrink: 0 }}>✕</button>
            </div>
          );
        })()}


        {/* Work tabs — desktop only; mobile uses bottom nav */}
        {!isMobile && <div style={{ display: "flex", gap: 2 }}>
          {WORK_TABS.map(tab => (
            <button key={tab.id} onClick={() => setView(tab.id)} style={{ display: "inline-flex", alignItems: "center", gap: 5, padding: "5px 12px", borderRadius: 6, border: "none", background: view === tab.id ? "#00a88420" : "transparent", color: view === tab.id ? "#00a884" : T.text2, fontSize: 13, fontWeight: 600, cursor: "pointer", fontFamily: "inherit", position: "relative" }}>
              <span>{tab.label.split(" ")[0]}</span>
              <span>{tab.label.split(" ").slice(1).join(" ")}</span>
              {tab.id === "tasks_global" && totalPendingTasks > 0 && <span style={{ background: "#ff6d00", color: "#000", fontSize: 9, fontWeight: 800, padding: "1px 5px", borderRadius: 10, lineHeight: 1.4 }}>{totalPendingTasks}</span>}
            </button>
          ))}
        </div>}

        {/* Spacer — only on desktop */}
        {!isMobile && <div style={{ flex: 1 }} />}

        {/* Admin tabs — hidden on mobile (moved to within admin section) */}
        {!isMobile && <div style={{ display: "flex", alignItems: "center", gap: 2, paddingLeft: 10, borderLeft: "1px solid #e9edef" }}>
          {ADMIN_TABS.map(tab => (
            <button key={tab.id} onClick={() => setView(tab.id)} style={{ display: "inline-flex", alignItems: "center", gap: 5, padding: "5px 12px", borderRadius: 6, border: tab.id === "upgrade" ? "1px solid #ff6d0044" : "none", background: view === tab.id ? "#00a88420" : tab.id === "upgrade" ? "#ff6d0012" : "transparent", color: view === tab.id ? "#00a884" : tab.id === "upgrade" ? "#ff6d00" : T.text2, fontSize: 13, fontWeight: 600, cursor: "pointer", fontFamily: "inherit" }}>
              {tab.label}
            </button>
          ))}
        </div>}
        <div style={{ display: "flex", alignItems: "center", gap: isMobile ? 6 : 10, paddingLeft: isMobile ? 0 : 12, borderLeft: isMobile ? "none" : "1px solid #e9edef" }}>
          {/* Trial days badge */}
          {trialInfo?.status === "trial" && trialInfo?.days_left !== null && (
            <div style={{ fontSize: 11, fontWeight: 700, color: trialInfo.days_left <= 2 ? "#f44336" : "#ff6d00" }}>
              ⏰ {trialInfo.days_left === 0 ? "último dia!" : `${trialInfo.days_left}d restantes`}
            </div>
          )}

          {/* ⚡ Credits pill */}
          {aiCredits && aiCredits.limit > 0 && (() => {
            const pct = aiCredits.limit > 0 ? Math.round(aiCredits.credits / aiCredits.limit * 100) : 100;
            const color = aiCredits.credits <= 0 ? "#f44336" : pct <= 25 ? "#ff9800" : "#00a884";
            return (
              <button onClick={() => setView("config")}
                title={`${aiCredits.credits.toLocaleString("pt-BR")} / ${aiCredits.limit.toLocaleString("pt-BR")} créditos IA restantes`}
                style={{ display: "flex", alignItems: "center", gap: 6, padding: "4px 10px", borderRadius: 20, border: `1px solid ${color}44`, background: `${color}12`, cursor: "pointer", fontFamily: "inherit" }}>
                <span style={{ fontSize: 12 }}>⚡</span>
                <div style={{ display: "flex", flexDirection: "column", gap: 2, minWidth: 60 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    <span style={{ fontSize: 11, fontWeight: 700, color }}>{aiCredits.credits.toLocaleString("pt-BR")}</span>
                    <span style={{ fontSize: 10, color: "#667781" }}>/{aiCredits.limit.toLocaleString("pt-BR")}</span>
                  </div>
                  <div style={{ height: 3, background: "#e9edef", borderRadius: 2, overflow: "hidden", width: "100%" }}>
                    <div style={{ height: "100%", width: `${Math.max(3, pct)}%`, background: color, borderRadius: 2, transition: "width 0.5s" }} />
                  </div>
                </div>
                {pct <= 25 && <span style={{ fontSize: 10, color }}>{aiCredits.credits <= 0 ? "Esgotado!" : "Baixo"}</span>}
              </button>
            );
          })()}

          <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: T.text2 }}>
            <div style={{ width: 6, height: 6, borderRadius: "50%", background: "#00a884" }} />
            <span style={{ color: T.text2 }}>{auth.user.name}</span>
          </div>
          <button onClick={onLogout} style={{ padding: "4px 10px", borderRadius: 6, border: `1px solid ${T.border}`, background: "transparent", color: T.text2, fontSize: 11, cursor: "pointer", fontFamily: "inherit" }}>Sair</button>
        </div>
      </div>

      <div style={{ flex: 1, display: "flex", overflow: "hidden", background: T.app }}>
        {/* Disparos */}
        {view === "disparos" && (
          <BroadcastsView
            conversations={conversations}
            labels={labels}
            agents={agents}
            kanbanCols={kanbanCols}
            instanceFilter={instanceFilter}
            instances={waInstances}
          />
        )}

        {/* Admin */}
        {view === "admin" && auth.user.role === "admin" && (
          <AdminPanel auth={auth} onLogout={onLogout} />
        )}

        {/* Dashboard Sócios */}
        {view === "socios" && IS_SOCIO && (
          <DashboardSocios auth={auth} clientes_reais={[]} />
        )}

        {/* Relatórios */}
        {view === "relatorios" && auth.user.role === "admin" && (
          <ReportsView auth={auth} T={T} />
        )}

        {/* Onboarding IA */}
        {view === "onboarding" && auth.user.role === "admin" && (
          <OnboardingView auth={auth} aiCredits={aiCredits} />
        )}

        {/* WhatsApp Connection */}
        {view === "whatsapp" && auth.user.role === "admin" && (
          <WhatsAppScreen auth={auth} T={T} theme={theme} />
        )}

        {/* Upgrade */}
        {view === "upgrade" && (
          <div style={{ flex: 1, overflowY: "auto", padding: 40 }}>
            <div style={{ maxWidth: 600, margin: "0 auto" }}>
              <div style={{ marginBottom: 32 }}>
                <div style={{ fontSize: 22, fontWeight: 700, marginBottom: 6 }}>🚀 Escolha seu plano</div>
                <div style={{ fontSize: 13, color: "#667781" }}>Todos os planos incluem 7 dias de trial grátis para novos clientes</div>
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                {[
                  { plan: "starter", label: "Starter", price: "R$ 149", desc: "Para recepções e pequenos negócios", color: "#00a884", features: ["1 número conectado", "Até 5 atendentes", "Inbox + Kanban + Etiquetas", "Disparos em massa", "Número extra: +R$49/mês"] },
                  { plan: "pro", label: "Pro", price: "R$ 299", desc: "Para academias, clínicas e empresas em crescimento", color: "#00a884", highlight: true, features: ["3 números conectados", "Até 15 atendentes", "Tudo do Starter", "Co-pilot IA (1.000 créditos/mês)", "Onboarding Inteligente IA (200 conversas)", "Número extra: +R$49/mês"] },
                  { plan: "business", label: "Business", price: "R$ 599", desc: "Para redes, franquias e operações maiores", color: "#7c4dff", features: ["8 números conectados", "Até 30 atendentes", "Tudo do Pro", "3.000 créditos IA/mês", "Onboarding Inteligente IA (500 conversas)", "White-label", "Suporte prioritário"] },
                ].map(p => (
                  <div key={p.plan} style={{ background: "#ffffff", border: `2px solid ${p.highlight ? p.color : "#e9edef"}`, borderRadius: 14, padding: 24, position: "relative" }}>
                    {p.highlight && <div style={{ position: "absolute", top: -10, right: 20, background: "#00a884", color: "#000", fontSize: 10, fontWeight: 800, padding: "2px 12px", borderRadius: 20 }}>MAIS POPULAR</div>}
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
                      <div>
                        <div style={{ fontSize: 18, fontWeight: 800, color: p.color }}>{p.label}</div>
                        <div style={{ fontSize: 12, color: "#667781", marginTop: 2 }}>{p.desc}</div>
                      </div>
                      <div style={{ textAlign: "right" }}>
                        <div style={{ fontSize: 24, fontWeight: 800, color: "#111b21" }}>{p.price}</div>
                        <div style={{ fontSize: 11, color: "#667781" }}>/mês</div>
                      </div>
                    </div>
                    <div style={{ display: "flex", flexDirection: "column", gap: 6, marginBottom: 16 }}>
                      {p.features.map(f => <div key={f} style={{ display: "flex", gap: 8, alignItems: "center", fontSize: 13, color: "#8696a0" }}><span style={{ color: p.color }}>✓</span>{f}</div>)}
                    </div>
                    <button onClick={async () => {
                      await fetch(`${API_URL}/tenant/activate-plan`, { method: "POST", headers, body: JSON.stringify({ tenant_id: TENANT_ID, plan: p.plan }) });
                      await fetchTrialStatus();
                      setView("inbox");
                    }} style={{ width: "100%", padding: "11px 0", borderRadius: 10, border: "none", background: p.highlight ? `linear-gradient(135deg, ${p.color}, #017561)` : `${p.color}22`, color: p.highlight ? "#000" : p.color, fontSize: 14, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>
                      Assinar {p.label} →
                    </button>
                  </div>
                ))}
              </div>
              <div style={{ marginTop: 24, padding: 16, background: "#ffffff", border: "1px solid #e9edef", borderRadius: 12, fontSize: 12, color: "#667781", textAlign: "center" }}>
                💬 Pagamento via PIX, boleto ou cartão · Fale com a gente no WhatsApp para dúvidas
              </div>
            </div>
          </div>
        )}

        {/* Config */}
        {view === "config" && (
          <div style={{ flex: 1, overflowY: "auto", padding: "32px 40px" }}>
            <div style={{ maxWidth: 1100, margin: "0 auto" }}>
            <div style={{ marginBottom: 28 }}><div style={{ fontSize: 22, fontWeight: 700, marginBottom: 6 }}>⚙️ Configurações</div><div style={{ fontSize: 13, color: "#667781" }}>Personalize o comportamento do 7zap para sua empresa</div></div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 340px", gap: 24, alignItems: "start" }}>
            <div style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 14, padding: 28, marginBottom: 0 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}><span style={{ fontSize: 18 }}>✨</span><span style={{ fontSize: 16, fontWeight: 700 }}>Co-pilot IA</span><span style={{ background: "#7c4dff22", color: "#a78bfa", fontSize: 11, fontWeight: 700, padding: "2px 10px", borderRadius: 20 }}>Co-pilot IA</span></div>
              <div style={{ fontSize: 13, color: "#667781", marginBottom: 20 }}>Prompt + modo automático do Co-pilot para sua empresa.</div>

              {/* Auto mode */}
              <div style={{ background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 12, padding: 20, marginBottom: 20 }}>
                <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 4 }}>🤖 Modo Automático</div>
                <div style={{ fontSize: 12, color: "#667781", marginBottom: 14 }}>Quando ativo, o Co-pilot responde sozinho sem precisar de aprovação humana.</div>
                {/* Starter locked banner */}
                {aiCredits?.plan === "starter" && (
                  <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "10px 14px", background: "#7c4dff12", border: "1px solid #7c4dff33", borderRadius: 10, marginBottom: 14 }}>
                    <span style={{ fontSize: 16 }}>🔒</span>
                    <div style={{ flex: 1 }}>
                      <div style={{ fontSize: 12, fontWeight: 700, color: "#a78bfa", marginBottom: 2 }}>Modos de IA disponíveis no plano Pro</div>
                      <div style={{ fontSize: 11, color: "#667781" }}>No Starter só o modo Desativado está disponível. Faça upgrade para liberar o Co-pilot completo.</div>
                    </div>
                    <button onClick={() => setView("upgrade")}
                      style={{ padding: "6px 14px", borderRadius: 7, border: "none", background: "linear-gradient(135deg, #7c4dff, #5b21b6)", color: "#fff", fontSize: 11, fontWeight: 700, cursor: "pointer", fontFamily: "inherit", whiteSpace: "nowrap" }}>
                      Ver planos →
                    </button>
                  </div>
                )}
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 16 }}>
                  {[
                    { id: "off",      label: "⛔ Desativado",   desc: "Só sugere resposta",              locked: false },
                    { id: "schedule", label: "🕐 Por horário",  desc: "Fora do horário comercial",        locked: true  },
                    { id: "always",   label: "🔄 Sempre ativo", desc: "Responde tudo automaticamente",    locked: true  },
                    { id: "per_conv", label: "🎛 Por conversa", desc: "Atendente ativa manualmente",      locked: true  },
                  ].map(m => {
                    const isStarter = aiCredits?.plan === "starter";
                    const isLocked = isStarter && m.locked;
                    const isActive = copilotAutoMode === m.id;
                    return (
                      <div key={m.id}
                        onClick={() => { if (isLocked) { setView("upgrade"); return; } setCopilotAutoMode(m.id); }}
                        style={{ flex: "1 1 180px", padding: "12px 14px", borderRadius: 10, position: "relative", transition: "all 0.15s",
                          border: `2px solid ${isActive ? "#7c4dff" : isLocked ? "#e9edef" : "#d1d7db"}`,
                          background: isActive ? "#7c4dff18" : isLocked ? "#f0f2f5" : "#ffffff",
                          cursor: isLocked ? "not-allowed" : "pointer",
                          opacity: isLocked ? 0.5 : 1 }}>
                        {isLocked && (
                          <span style={{ position: "absolute", top: 7, right: 8, fontSize: 9, fontWeight: 800, color: "#7c4dff", background: "#7c4dff18", border: "1px solid #7c4dff33", padding: "2px 7px", borderRadius: 20 }}>🔒 PRO</span>
                        )}
                        <div style={{ fontSize: 13, fontWeight: 700, color: isActive ? "#a78bfa" : isLocked ? "#54656f" : "#8696a0", marginBottom: 3 }}>{m.label}</div>
                        <div style={{ fontSize: 11, color: isLocked ? "#2a2a3a" : "#667781" }}>{m.desc}</div>
                      </div>
                    );
                  })}
                </div>
                {copilotAutoMode === "schedule" && (
                  <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
                    <div>
                      <div style={{ fontSize: 11, color: "#667781", marginBottom: 4, fontWeight: 700 }}>INÍCIO DO PERÍODO AUTOMÁTICO</div>
                      <input type="time" value={copilotScheduleStart} onChange={e => setCopilotScheduleStart(e.target.value)}
                        style={{ padding: "7px 12px", background: "#ffffff", border: "1px solid #e9edef", borderRadius: 8, color: "#111b21", fontSize: 13, outline: "none", colorScheme: "dark" }} />
                    </div>
                    <div style={{ color: "#667781", paddingTop: 18 }}>até</div>
                    <div>
                      <div style={{ fontSize: 11, color: "#667781", marginBottom: 4, fontWeight: 700 }}>FIM DO PERÍODO AUTOMÁTICO</div>
                      <input type="time" value={copilotScheduleEnd} onChange={e => setCopilotScheduleEnd(e.target.value)}
                        style={{ padding: "7px 12px", background: "#ffffff", border: "1px solid #e9edef", borderRadius: 8, color: "#111b21", fontSize: 13, outline: "none", colorScheme: "dark" }} />
                    </div>
                    <div style={{ fontSize: 11, color: "#7c4dff", paddingTop: 18 }}>Automático das {copilotScheduleStart} às {copilotScheduleEnd}</div>
                  </div>
                )}
                {copilotAutoMode === "always" && (
                  <div style={{ background: "#f4433315", border: "1px solid #f4433333", borderRadius: 8, padding: "10px 14px", fontSize: 12, color: "#f44336" }}>
                    ⚠️ Modo <strong>Sempre ativo</strong> responde automaticamente a TODAS as mensagens sem revisão humana. Use com atenção.
                  </div>
                )}
                {copilotAutoMode === "per_conv" && (
                  <div style={{ background: "#00a88415", border: "1px solid #00a88433", borderRadius: 8, padding: "10px 14px", fontSize: 12, color: "#00a884" }}>
                    ✓ Um botão <strong>🤖 Auto</strong> vai aparecer em cada conversa para o atendente ativar o modo automático individualmente.
                  </div>
                )}
              </div>

              {/* Prompt — read-only display */}
              <div style={{ fontSize: 12, fontWeight: 700, color: "#667781", marginBottom: 8 }}>🧠 O QUE A IA SABE SOBRE SEU NEGÓCIO</div>
              {copilotPrompt ? (
                <div style={{ background: "linear-gradient(135deg, #f5f0ff, #faf5ff)", border: "1px solid #a78bfa55", borderRadius: 12, padding: "18px 20px", marginBottom: 16, maxHeight: 340, overflowY: "auto" }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14, paddingBottom: 12, borderBottom: "1px solid #a78bfa22" }}>
                    <span style={{ fontSize: 18 }}>✨</span>
                    <div>
                      <div style={{ fontSize: 13, fontWeight: 700, color: "#5e35b1" }}>Personalidade da IA configurada</div>
                      <div style={{ fontSize: 11, color: "#9e7cc9" }}>Gerado pelo Onboarding Inteligente com base no seu histórico real</div>
                    </div>
                  </div>
                  {copilotPrompt.split("\n").filter(l => l.trim()).map((line, i) => {
                    const isHeader = line.startsWith("#");
                    const isBullet = line.startsWith("•") || line.startsWith("-") || line.startsWith("*");
                    const isNumbered = /^\d+\./.test(line.trim());
                    const clean = line.replace(/^#+\s*/, "").replace(/\*\*/g, "");
                    if (isHeader) return (
                      <div key={i} style={{ fontSize: 13, fontWeight: 800, color: "#5e35b1", margin: "14px 0 8px", textTransform: "uppercase", letterSpacing: "0.5px" }}>{clean}</div>
                    );
                    if (isBullet || isNumbered) return (
                      <div key={i} style={{ display: "flex", gap: 8, marginBottom: 6, alignItems: "flex-start" }}>
                        <span style={{ color: "#a78bfa", fontWeight: 700, flexShrink: 0, marginTop: 1 }}>▸</span>
                        <span style={{ fontSize: 13, color: "#3d1a78", lineHeight: 1.55 }}>{clean.replace(/^[•\-*]\s*/, "").replace(/^\d+\.\s*/, "")}</span>
                      </div>
                    );
                    return (
                      <div key={i} style={{ fontSize: 13, color: "#4a2080", lineHeight: 1.6, marginBottom: 5 }}>{clean}</div>
                    );
                  })}
                  <div style={{ marginTop: 14, paddingTop: 12, borderTop: "1px solid #a78bfa22", fontSize: 11, color: "#9e7cc9", display: "flex", alignItems: "center", gap: 6 }}>
                    <span>🔄</span> Para atualizar: use <strong style={{ color: "#a78bfa", cursor: "pointer" }} onClick={() => setView("onboarding")}>Onboarding Inteligente</strong> e refaça a análise.
                  </div>
                </div>
              ) : (
                <div style={{ background: "#f5f0ff", border: "1px dashed #a78bfa55", borderRadius: 12, padding: "28px 20px", textAlign: "center", marginBottom: 16 }}>
                  <div style={{ fontSize: 32, marginBottom: 10 }}>🧠</div>
                  <div style={{ fontSize: 14, fontWeight: 700, color: "#5e35b1", marginBottom: 6 }}>A IA ainda não conhece seu negócio</div>
                  <div style={{ fontSize: 12, color: "#9e7cc9", marginBottom: 14 }}>Use o Onboarding Inteligente para a IA aprender como sua empresa funciona, seu tom de voz, produtos e regras.</div>
                  <button onClick={() => setView("onboarding")} style={{ padding: "9px 22px", borderRadius: 8, border: "none", background: "linear-gradient(135deg, #a78bfa, #7c4dff)", color: "#fff", fontSize: 13, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>
                    🧠 Fazer Onboarding agora →
                  </button>
                </div>
              )}
              <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
                <button onClick={savePrompt} disabled={savingPrompt} style={{ padding: "10px 28px", borderRadius: 9, border: "none", background: savingPrompt ? "#e9edef" : "linear-gradient(135deg, #7c4dff, #5b21b6)", color: savingPrompt ? "#667781" : "#fff", fontSize: 14, fontWeight: 700, cursor: savingPrompt ? "not-allowed" : "pointer", fontFamily: "inherit" }}>{savingPrompt ? "Salvando..." : "💾 Salvar configurações"}</button>
                {promptSaved && <span style={{ fontSize: 13, color: "#00a884", fontWeight: 600 }}>✓ Salvo!</span>}
              </div>
              {/* Credits card — direto abaixo do salvar */}
              <div style={{ padding: "14px 16px", background: "linear-gradient(135deg, #00a88410, #7c4dff10)", border: "1px solid #00a88433", borderRadius: 10, display: "flex", alignItems: "center", gap: 12 }}>
                <span style={{ fontSize: 22 }}>⚡</span>
                <div style={{ flex: 1 }}>
                  <div style={{ fontSize: 12, fontWeight: 700, color: "#00a884", marginBottom: 2 }}>Créditos de IA</div>
                  <div style={{ fontSize: 11, color: "#667781" }}>
                    {aiCredits ? `${aiCredits.credits.toLocaleString("pt-BR")} créditos restantes de ${aiCredits.limit.toLocaleString("pt-BR")}` : "Carregando..."}
                  </div>
                </div>
                <button onClick={() => setShowBuyCredits(true)}
                  style={{ padding: "8px 18px", borderRadius: 8, border: "none", background: "linear-gradient(135deg, #00a884, #017561)", color: "#000", fontSize: 12, fontWeight: 800, cursor: "pointer", fontFamily: "inherit", whiteSpace: "nowrap" }}>
                  + Comprar créditos
                </button>
              </div>
            </div>{/* end Co-pilot IA card */}

            {/* Right column — company info sidebar */}
            <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
              <div style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 14, padding: 24 }}>
                <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 4 }}>🏢 Sua Empresa</div>
                <div style={{ fontSize: 12, color: "#667781", marginBottom: 20 }}>Informações do plano e uso atual</div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 20 }}>
                  {[
                    { label: "Plano atual", value: (aiCredits?.plan || "pro").charAt(0).toUpperCase() + (aiCredits?.plan || "pro").slice(1), color: "#7c4dff" },
                    { label: "Versão", value: "CRM v1.0", color: "#00a884" },
                    { label: "Atendentes", value: `${agents.length} ativo${agents.length !== 1 ? "s" : ""}`, color: "#00a884" },
                    { label: "Status", value: "🟢 Online", color: "#00a884" },
                  ].map(({ label, value, color }) => (
                    <div key={label} style={{ background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 10, padding: "12px 16px" }}>
                      <div style={{ fontSize: 11, color: "#667781", marginBottom: 4 }}>{label}</div>
                      <div style={{ fontSize: 14, fontWeight: 700, color }}>{value}</div>
                    </div>
                  ))}
                </div>
                <div style={{ padding: "12px 16px", background: "#f0f2f5", border: "1px solid #e9edef", borderRadius: 10, display: "flex", alignItems: "center", gap: 10 }}>
                  <span style={{ fontSize: 18 }}>🔐</span>
                  <div style={{ flex: 1 }}>
                    <div style={{ fontSize: 12, fontWeight: 600, color: "#8696a0", marginBottom: 2 }}>Gestão de usuários</div>
                    <div style={{ fontSize: 11, color: "#667781" }}>Convites, permissões e acesso</div>
                  </div>
                  <button onClick={() => setView("admin")}
                    style={{ padding: "6px 14px", borderRadius: 7, border: "1px solid #7c4dff44", background: "#7c4dff15", color: "#a78bfa", fontSize: 11, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>
                    Gerenciar →
                  </button>
                </div>
              </div>

              <div style={{ background: "#ffffff", border: "1px solid #e9edef", borderRadius: 14, padding: 20 }}>
                <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 4 }}>👥 Contatos</div>
                <div style={{ fontSize: 12, color: "#667781", marginBottom: 14 }}>Manutenção dos dados de contato</div>
                <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                  <div style={{ flex: 1 }}>
                    <div style={{ fontSize: 12, fontWeight: 700, marginBottom: 2 }}>Sincronizar nomes</div>
                    <div style={{ fontSize: 11, color: "#667781" }}>Busca o nome real de cada contato no WhatsApp</div>
                  </div>
                  <button onClick={syncContactNames}
                    style={{ padding: "7px 14px", borderRadius: 8, border: "1px solid #00a88444", background: "#00a88415", color: "#00a884", fontSize: 11, fontWeight: 700, cursor: "pointer", fontFamily: "inherit", whiteSpace: "nowrap" }}>
                    Sincronizar →
                  </button>
                </div>
              </div>
            </div>{/* end right column */}

            </div>{/* end grid */}
            </div>{/* end maxWidth wrapper */}
          </div>
        )}

        {/* Global Tasks */}
        {view === "tasks_global" && (
          <GlobalTasksView
            pendingTasksMap={pendingTasksMap}
            conversations={filtered}
            agents={agents}
            onSelectConv={(conv) => { setSelected(conv); setView("inbox"); }}
            onRefresh={fetchPendingTasks}
          />
        )}

        {/* Leads */}
        {view === "leads" && (
          <LeadsBoard
            instanceFilter={instanceFilter}
            instances={waInstances}
            conversations={filtered}
            kanbanCols={kanbanCols}
            labels={labels}
            onSelectConv={(conv) => { setSelected(conv); setView("inbox"); }}
            onManageLabels={() => setShowLabelManager(true)}
            onMoveLabel={moveLabelCard}
          />
        )}

        {/* Kanban */}
        {view === "kanban" && <KanbanBoard conversations={filtered} columns={kanbanCols} onMoveCard={moveKanbanCard} onSelectConv={(conv) => { setSelected(conv); setView("inbox"); }} onManageCols={() => setShowColManager(true)} instanceFilter={instanceFilter} />}

        {/* Inbox */}
        {view === "inbox" && (
          <div style={{ display: "flex", flexDirection: "column", flex: 1, overflow: "hidden" }}>
            {/* Disconnect banner — flow element, pushes content down */}
            {waInstances.some(i => !i.connected && i.phone) && (
              <div style={{ background: "linear-gradient(90deg,#b71c1c,#c62828)", padding: "9px 20px", display: "flex", alignItems: "center", gap: 10, boxShadow: "0 1px 4px #f4433620", flexShrink: 0, zIndex: 10 }}>
                <span style={{ fontSize: 16 }}>📵</span>
                <span style={{ flex: 1, fontSize: 13, fontWeight: 600, color: "#fff" }}>
                  {waInstances.filter(i => !i.connected && i.phone).map(i => `"${i.label || i.phone}"`).join(", ")} {waInstances.filter(i => !i.connected && i.phone).length === 1 ? "está desconectado" : "estão desconectados"} — nenhuma mensagem nova está sendo recebida.
                </span>
                <button onClick={() => setView("whatsapp")}
                  style={{ padding: "5px 14px", borderRadius: 7, border: "1px solid #ffffff44", background: "#ffffff18", color: "#fff", fontSize: 12, fontWeight: 700, cursor: "pointer", fontFamily: "inherit", whiteSpace: "nowrap" }}>
                  Reconectar →
                </button>
              </div>
            )}

            {/* Inbox row: sidebar + chat — responsive */}
            <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>

            {/* Sidebar — full width on mobile when no conversation selected */}
            <div style={{ width: isMobile ? (selected ? 0 : "100%") : 300, flexShrink: 0, display: isMobile && selected ? "none" : "flex", flexDirection: "column", borderRight: isMobile ? "none" : `1px solid ${T.border}`, background: T.sidebar, transition: "width 0.2s" }}>
              <div style={{ padding: "12px 14px", borderBottom: `1px solid ${T.border}` }}>
                <div style={{ position: "relative" }}>
                  <span style={{ position: "absolute", left: 10, top: "50%", transform: "translateY(-50%)", fontSize: 13, opacity: 0.4 }}>🔍</span>
                  <input value={search} onChange={e => setSearch(e.target.value)} placeholder="Buscar conversa..." style={{ width: "100%", padding: "7px 12px 7px 30px", background: T.hover, border: `1px solid ${T.border}`, borderRadius: 8, color: T.text, fontSize: 13, outline: "none", boxSizing: "border-box" }} />
                </div>
              </div>

              {/* Tudo / Não lidas */}
              <div style={{ display: "flex", borderBottom: `1px solid ${T.border}` }}>
                {[["all", "Tudo"], ["unread", "Não lidas"]].map(([id, label]) => (
                  <button key={id} onClick={() => setUnreadFilter(id)} style={{ flex: 1, padding: "8px 0", border: "none", background: "transparent", color: unreadFilter === id ? "#00a884" : "#667781", fontSize: 12, fontWeight: unreadFilter === id ? 700 : 500, cursor: "pointer", fontFamily: "inherit", borderBottom: `2px solid ${unreadFilter === id ? "#00a884" : "transparent"}`, transition: "all 0.15s" }}>
                    {label}{id === "unread" && unreadCount > 0 ? ` (${unreadCount})` : ""}
                  </button>
                ))}
              </div>

              {/* ─── SELETOR DE NÚMERO PRINCIPAL ─── */}
              {waInstances.length >= 2 && !instanceFilter && (
                <div style={{ padding: "10px 12px", background: "linear-gradient(135deg, #00a88412, #00695c0a)", borderBottom: "2px solid #00a88430" }}>
                  <div style={{ fontSize: 11, fontWeight: 700, color: "#00a884", marginBottom: 7, letterSpacing: 0.3 }}>📱 SELECIONE O NÚMERO</div>
                  <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                    {waInstances.map(inst => (
                      <button key={inst.id} onClick={() => selectInstance(inst.instance_name)}
                        style={{ display: "flex", alignItems: "center", gap: 6, padding: "7px 14px", borderRadius: 10, border: "1px solid #00a88433", background: "#fff", color: "#111b21", fontSize: 12, fontWeight: 700, cursor: "pointer", fontFamily: "inherit", boxShadow: "0 1px 4px #0001", transition: "all 0.15s" }}
                        onMouseEnter={e => e.currentTarget.style.background = "#00a88412"}
                        onMouseLeave={e => e.currentTarget.style.background = "#fff"}>
                        <span style={{ width: 8, height: 8, borderRadius: "50%", background: inst.connected ? "#00a884" : "#f44336", display: "inline-block", flexShrink: 0 }} />
                        <div style={{ textAlign: "left" }}>
                          <div>{inst.label || inst.instance_name}</div>
                          {inst.phone && <div style={{ fontSize: 10, color: "#667781", fontWeight: 500 }}>{inst.phone}</div>}
                        </div>
                      </button>
                    ))}
                  </div>
                </div>
              )}

              {/* Number selector pills — only shown when 2+ instances exist */}
              {waInstances.length >= 2 && (
                <div style={{ padding: "7px 10px", borderBottom: "1px solid #e9edef", display: "flex", gap: 5, alignItems: "center", overflowX: "auto" }}>

                  {waInstances.map(inst => (
                    <button key={inst.id} onClick={() => selectInstance(instanceFilter === inst.instance_name ? null : inst.instance_name)}
                      style={{ padding: "3px 10px", borderRadius: 20, border: `1px solid ${instanceFilter === inst.instance_name ? "#00a88466" : T.border}`, background: instanceFilter === inst.instance_name ? "#00a88418" : "transparent", color: instanceFilter === inst.instance_name ? "#00a884" : "#667781", fontSize: 11, fontWeight: instanceFilter === inst.instance_name ? 700 : 500, cursor: "pointer", fontFamily: "inherit", whiteSpace: "nowrap", flexShrink: 0, display: "flex", alignItems: "center", gap: 4 }}>
                      <span style={{ width: 6, height: 6, borderRadius: "50%", background: inst.connected ? "#00a884" : "#f44336", display: "inline-block", flexShrink: 0 }} />
                      {inst.label || inst.instance_name}
                    </button>
                  ))}
                </div>
              )}

              {/* Inactive days filter */}
              <div style={{ padding: "8px 10px", borderBottom: "1px solid #e9edef", display: "flex", flexWrap: "wrap", gap: 5, alignItems: "center" }}>
                <span style={{ fontSize: 10, color: T.text2, fontWeight: 700, letterSpacing: 0.5, marginRight: 2 }}>SEM RESPOSTA:</span>
                {[null, 3, 7, 15].map(d => (
                  <button key={d ?? "all"} onClick={() => setInactiveDays(d === inactiveDays ? null : d)}
                    style={{ padding: "3px 9px", borderRadius: 20, border: `1px solid ${inactiveDays === d && d !== null ? "#ff6d0066" : T.border}`, background: inactiveDays === d && d !== null ? "#ff6d0018" : "transparent", color: inactiveDays === d && d !== null ? "#ff6d00" : d === null ? "#54656f" : "#667781", fontSize: 11, fontWeight: inactiveDays === d ? 700 : 500, cursor: "pointer", fontFamily: "inherit", whiteSpace: "nowrap" }}>
                    {d === null ? "Todos" : d === 15 ? "15d+" : `${d}d`}
                  </button>
                ))}
                {inactiveDays && <span style={{ fontSize: 10, color: "#ff6d00" }}>({filtered.length})</span>}
              </div>

              <div ref={convListRef} style={{ flex: 1, overflowY: "auto" }} onScroll={e => {
                  const el = e.currentTarget;
                  if (hasMoreConvs && !loadingMoreConvs && el.scrollHeight - el.scrollTop - el.clientHeight < 120) {
                    fetchMoreConversations();
                  }
                }}>
                {(loading || initialLoad) ? (
                  <div style={{ padding: "8px 14px" }}>
                    {[...Array(8)].map((_,i) => (
                      <div key={i} style={{ display: "flex", gap: 10, padding: "11px 0", borderBottom: "1px solid #f0f2f5", opacity: 1 - i*0.1 }}>
                        <div style={{ width: 40, height: 40, borderRadius: "50%", background: "linear-gradient(90deg,#f0f2f5 25%,#e9edef 50%,#f0f2f5 75%)", backgroundSize: "200% 100%", animation: "shimmer 1.2s infinite", flexShrink: 0 }} />
                        <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 6, justifyContent: "center" }}>
                          <div style={{ height: 12, borderRadius: 6, background: "linear-gradient(90deg,#f0f2f5 25%,#e9edef 50%,#f0f2f5 75%)", backgroundSize: "200% 100%", animation: "shimmer 1.2s infinite", width: "60%" }} />
                          <div style={{ height: 10, borderRadius: 6, background: "linear-gradient(90deg,#f0f2f5 25%,#e9edef 50%,#f0f2f5 75%)", backgroundSize: "200% 100%", animation: "shimmer 1.2s infinite", width: "40%" }} />
                        </div>
                      </div>
                    ))}
                  </div>
                ) : filtered.length === 0 ? <div style={{ padding: 24, textAlign: "center", color: "#667781", fontSize: 13 }}>Nenhuma conversa</div>
                  : filtered.map(conv => (
                    <div key={conv.id} onClick={() => { setSelected(conv); setSuggestion(""); setShowTasks(false); setNoteMode(false); }} style={{ display: "flex", alignItems: "flex-start", gap: 10, padding: "11px 14px", cursor: "pointer", background: selected?.id === conv.id ? T.selected : "transparent", borderLeft: selected?.id === conv.id ? "3px solid #00a884" : "3px solid transparent" }}>
                      <Avatar name={displayName(conv.contacts?.name, conv.contacts?.phone)} phone={conv.contacts?.phone} instanceFilter={instanceFilter} />
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 4, marginBottom: 2 }}>
                          <span style={{ fontWeight: conv.unread_count > 0 ? 800 : 600, fontSize: 13, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", color: conv.unread_count > 0 ? T.text : T.text2 }}>{displayName(conv.contacts?.name, conv.contacts?.phone)}</span>
                          <span style={{ fontSize: 11, color: inactiveDays ? "#ff6d00" : (conv.unread_count > 0 ? "#00a884" : "#667781"), flexShrink: 0, fontWeight: conv.unread_count > 0 ? 700 : 400 }}>{timeAgo(conv.last_message_at)}</span>
                        </div>
                        <div style={{ display: "flex", gap: 4, flexWrap: "wrap", marginBottom: 4 }}>
                          {conv.labels?.map(l => <LabelChip key={l.id} label={l} />)}
                          <KanbanBadge stage={conv.kanban_stage} columns={kanbanCols} />
                        </div>
                        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                          <span style={{ fontSize: 11, color: "#667781", flex: 1 }}>{conv.assigned_agent ? `👤 ${conv.assigned_agent}` : formatPhone(conv.contacts?.phone)}</span>
                          {conv.unread_count > 0 && (
                            <span title={`${conv.unread_count} mensagem${conv.unread_count > 1 ? "s" : ""} não lida${conv.unread_count > 1 ? "s" : ""}`}
                              style={{ background: "#00a884", color: "#fff", fontSize: 10, fontWeight: 800, minWidth: 18, height: 18, display: "inline-flex", alignItems: "center", justifyContent: "center", borderRadius: "50%", padding: "0 5px", flexShrink: 0, letterSpacing: 0 }}>
                              {conv.unread_count > 99 ? "99+" : conv.unread_count}
                            </span>
                          )}
                          {pendingTasksMap[conv.id] > 0 && <span title={`${pendingTasksMap[conv.id]} tarefa(s) pendente(s)`} style={{ background: "#ff6d0022", border: "1px solid #ff6d0066", color: "#ff6d00", fontSize: 10, fontWeight: 700, padding: "1px 5px", borderRadius: 10, flexShrink: 0 }}>✅ {pendingTasksMap[conv.id]}</span>}
                          {isAutoActive(conv) && <span title="Co-pilot automático ativo" style={{ fontSize: 12 }}>🤖</span>}
                        </div>
                        {/* Retomar button — shown when inactive filter is active */}
                        {inactiveDays && (
                          <button
                            onClick={e => { e.stopPropagation(); resumeConversation(conv); }}
                            disabled={resumingConv === conv.id}
                            style={{ marginTop: 6, width: "100%", padding: "5px 0", borderRadius: 6, border: "1px solid #7c4dff44", background: resumingConv === conv.id ? "#e9edef" : "#7c4dff18", color: resumingConv === conv.id ? "#667781" : "#a78bfa", fontSize: 11, fontWeight: 700, cursor: "pointer", fontFamily: "inherit", display: "flex", alignItems: "center", justifyContent: "center", gap: 5 }}>
                            {resumingConv === conv.id ? <><span style={{ animation: "spin 1s linear infinite", display: "inline-block" }}>⏳</span> Gerando...</> : "✨ Retomar conversa"}
                          </button>
                        )}
                      </div>
                    </div>
                  ))}
              </div>
              {/* Infinite scroll loader */}
              {loadingMoreConvs && (
                <div style={{ padding: "12px 0", textAlign: "center", color: "#667781", fontSize: 12 }}>
                  ⏳ Carregando mais...
                </div>
              )}
              {hasMoreConvs && !loadingMoreConvs && (
                <button onClick={fetchMoreConversations} style={{ width: "100%", padding: "10px 0", background: "none", border: "none", cursor: "pointer", fontSize: 12, color: "#00a884", fontFamily: "inherit", fontWeight: 600 }}>
                  ↓ Carregar mais conversas
                </button>
              )}
              {!hasMoreConvs && conversations.length > 50 && (
                <div style={{ padding: "10px 0", textAlign: "center", color: "#d1d7db", fontSize: 11 }}>
                  — fim das conversas —
                </div>
              )}
              <div style={{ padding: "10px 14px", borderTop: `1px solid ${T.border}`, fontSize: 12, color: T.text2, display: "flex", alignItems: "center", gap: 6 }}>
                <div style={{ width: 6, height: 6, borderRadius: "50%", background: "#00a884" }} />{conversations.length} conversa{conversations.length !== 1 ? "s" : ""}{hasMoreConvs ? "+" : ""}
              </div>
            </div>

            {/* Chat area */}
            {selected ? (
              <div style={{ flex: 1, display: "flex", minWidth: 0 }}>
                <div style={{ flex: 1, display: "flex", flexDirection: "column", minWidth: 0 }}>
                  {/* Chat header */}
                  <div style={{ padding: "10px 14px", borderBottom: "1px solid #e9edef", display: "flex", alignItems: "center", gap: 10, background: T.topbar, flexWrap: "wrap" }}>
                    {/* Mobile back button */}
                    {isMobile && (
                      <button onClick={() => setSelected(null)}
                        style={{ padding: "4px 8px", borderRadius: 7, border: "none", background: "transparent", color: "#00a884", fontSize: 20, cursor: "pointer", lineHeight: 1, flexShrink: 0 }}>
                        ←
                      </button>
                    )}
                    <Avatar name={displayName(selected.contacts?.name, selected.contacts?.phone)} size={34} phone={selected.contacts?.phone} instanceFilter={instanceFilter} />
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontWeight: 700, fontSize: 13 }}>{displayName(selected.contacts?.name, selected.contacts?.phone)}</div>
                      <div style={{ display: "flex", alignItems: "center", gap: 6, flexWrap: "wrap", marginTop: 2 }}>
                        <span style={{ fontSize: 11, color: "#667781" }}>{formatPhone(selected.contacts?.phone)}</span>
                        {selected.assigned_agent && <span style={{ fontSize: 11, color: "#00a884" }}>· 👤 {selected.assigned_agent}</span>}
                        {selected.labels?.map(l => <LabelChip key={l.id} label={l} />)}
                        <KanbanBadge stage={selected.kanban_stage} columns={kanbanCols} />
                      </div>
                    </div>
                    {isAutoActive(selected) && (
                      <div style={{ width: "100%", padding: "5px 14px", background: "#7c4dff18", borderTop: "1px solid #7c4dff33", display: "flex", alignItems: "center", gap: 8, fontSize: 11 }}>
                        <span style={{ animation: "pulse 2s infinite", display: "inline-block" }}>🤖</span>
                        <span style={{ color: "#a78bfa", fontWeight: 700 }}>Co-pilot automático ativo</span>
                        <span style={{ color: "#667781" }}>— respondendo automaticamente mensagens recebidas</span>
                        {copilotAutoMode === "per_conv" && <button onClick={async () => {
                          const newVal = false;
                          autoModeRef.current[selected.id] = newVal;
                          await fetch(`${API_URL}/conversations/${selected.id}/auto-mode`, { method: "PUT", headers, body: JSON.stringify({ enabled: newVal }) }).catch(() => {});
                          setSelected(prev => ({ ...prev, auto_mode: newVal }));
                          setConversations(prev => prev.map(c => c.id === selected.id ? { ...c, auto_mode: newVal } : c));
                        }} style={{ marginLeft: "auto", padding: "2px 10px", borderRadius: 5, border: "1px solid #f4433333", background: "transparent", color: "#f44336", fontSize: 11, cursor: "pointer", fontFamily: "inherit" }}>Pausar</button>}
                      </div>
                    )}
                    <div style={{ display: "flex", gap: 5, flexWrap: "wrap", alignItems: "center" }}>
                      <StatusDropdown status={selected.status} onChange={(s) => changeStatus(selected.id, s)} isChanging={changingStatus} />
                      <button onClick={() => setShowLabelPicker(true)} style={{ padding: "5px 10px", borderRadius: 6, border: "1px solid #e9edef", background: "transparent", color: "#8696a0", fontSize: 11, cursor: "pointer", fontFamily: "inherit", fontWeight: 600 }}>🏷 Etiqueta</button>
                      <button onClick={() => setShowAssign(true)} disabled={assigningConv} style={{ padding: "5px 10px", borderRadius: 6, border: "1px solid #e9edef", background: "transparent", color: assigningConv ? "#d1d7db" : "#8696a0", fontSize: 11, cursor: assigningConv ? "wait" : "pointer", fontFamily: "inherit", fontWeight: 600 }}>{assigningConv ? "⏳..." : "👤 Atribuir"}</button>
                      <button onClick={fetchSuggestion} disabled={loadingSuggest} style={{ padding: "5px 10px", borderRadius: 6, border: "1px solid #7c4dff44", background: loadingSuggest ? "#e9edef" : "#7c4dff15", color: loadingSuggest ? "#667781" : "#a78bfa", fontSize: 11, cursor: loadingSuggest ? "not-allowed" : "pointer", fontFamily: "inherit", fontWeight: 600 }}>{loadingSuggest ? "⏳..." : "✨ Co-pilot"}</button>
                      {copilotAutoMode === "per_conv" && (
                        <button onClick={async () => {
                          const newVal = !selected.auto_mode;
                          autoModeRef.current[selected.id] = newVal;
                          await fetch(`${API_URL}/conversations/${selected.id}/auto-mode`, { method: "PUT", headers, body: JSON.stringify({ enabled: newVal }) }).catch(() => {});
                          setSelected(prev => ({ ...prev, auto_mode: newVal }));
                          setConversations(prev => prev.map(c => c.id === selected.id ? { ...c, auto_mode: newVal } : c));
                        }} style={{ padding: "5px 10px", borderRadius: 6, border: `1px solid ${selected.auto_mode ? "#00a88444" : "#d1d7db"}`, background: selected.auto_mode ? "#00a88418" : "transparent", color: selected.auto_mode ? "#00a884" : "#667781", fontSize: 11, cursor: "pointer", fontFamily: "inherit", fontWeight: 700 }}>🤖 Auto {selected.auto_mode ? "ON" : "OFF"}</button>
                      )}
                      <button onClick={() => setShowTasks(t => !t)} style={{ position: "relative", display: "inline-flex", alignItems: "center", gap: 5, padding: "5px 10px", borderRadius: 6, border: `1px solid ${showTasks ? "#00a88444" : pendingTasksMap[selected?.id] > 0 ? "#ff6d0044" : "#d1d7db"}`, background: showTasks ? "#00a88415" : pendingTasksMap[selected?.id] > 0 ? "#ff6d0010" : "transparent", color: showTasks ? "#00a884" : pendingTasksMap[selected?.id] > 0 ? "#ff6d00" : "#8696a0", fontSize: 11, cursor: "pointer", fontFamily: "inherit", fontWeight: 600 }}>✅ Tarefas{!showTasks && pendingTasksMap[selected?.id] > 0 && <span style={{ background: "#ff6d00", color: "#000", fontSize: 9, fontWeight: 700, padding: "1px 5px", borderRadius: 10 }}>{pendingTasksMap[selected.id]}</span>}</button>
                    </div>
                  </div>

                  {/* Messages */}
                  <div ref={chatScrollRef} onMouseEnter={() => { if (selected?.unread_count > 0) { setConversations(prev => prev.map(c => c.id === selected.id ? { ...c, unread_count: 0 } : c)); setSelected(prev => prev ? { ...prev, unread_count: 0 } : prev); } }} style={{ flex: 1, overflowY: "auto", padding: "16px 20px", display: "flex", flexDirection: "column", gap: 2, background: T.chatBg }}>
                    {/* Load More button */}
                    {hasMoreMessages && (
                      <div style={{ textAlign: "center", marginBottom: 12 }}>
                        <button
                          onClick={async () => {
                            const scrollEl = chatScrollRef.current;
                            const prevScrollHeight = scrollEl?.scrollHeight || 0;
                            await fetchMoreMessages(selected.id, messages);
                            // Preserve scroll position after prepending older messages
                            if (scrollEl) {
                              requestAnimationFrame(() => {
                                scrollEl.scrollTop = scrollEl.scrollHeight - prevScrollHeight;
                              });
                            }
                          }}
                          disabled={loadingMoreMsgs}
                          style={{ padding: "6px 18px", borderRadius: 20, border: "1px solid #d1d7db", background: loadingMoreMsgs ? "#f0f2f5" : "#fff", color: "#54656f", fontSize: 12, fontWeight: 600, cursor: loadingMoreMsgs ? "not-allowed" : "pointer", fontFamily: "inherit", boxShadow: "0 1px 2px #0000001a" }}
                        >
                          {loadingMoreMsgs ? "Carregando..." : "⬆ Carregar mais"}
                        </button>
                      </div>
                    )}
                    {loadingMessages && messages.length === 0 ? (
                      <div style={{ display: "flex", flexDirection: "column", gap: 10, padding: "20px 16px" }}>
                        {[...Array(6)].map((_,i) => (
                          <div key={i} style={{ display: "flex", justifyContent: i%2===0 ? "flex-start" : "flex-end" }}>
                            <div style={{ width: `${180 + (i*37)%120}px`, height: 38, borderRadius: 10, background: "linear-gradient(90deg,#f0f2f5 25%,#e9edef 50%,#f0f2f5 75%)", backgroundSize: "200% 100%", animation: "shimmer 1.2s infinite" }} />
                          </div>
                        ))}
                      </div>
                    ) : messagesError ? (
                      <div style={{ textAlign: "center", marginTop: 60, padding: "0 24px" }}>
                        <div style={{ fontSize: 32, marginBottom: 10 }}>⚠️</div>
                        <div style={{ color: "#667781", fontSize: 13, marginBottom: 16 }}>{messagesError}</div>
                        <button onClick={() => fetchMessages(selected.id)} style={{ padding: "8px 20px", borderRadius: 8, background: "#00a884", color: "#fff", border: "none", fontWeight: 700, fontSize: 13, cursor: "pointer", fontFamily: "inherit" }}>
                          Tentar novamente
                        </button>
                      </div>
                    ) : messages.length === 0 ? <div style={{ textAlign: "center", color: "#667781", fontSize: 13, marginTop: 40 }}>Nenhuma mensagem ainda</div>
                      : messages.map((msg, i) => {
                        const isOut = msg.direction === "outbound";
                        const isInternal = msg.is_internal_note;
                        return (
                          <div key={msg.id || i} style={{ display: "flex", justifyContent: isOut ? "flex-end" : "flex-start", marginBottom: 2 }}>
                            <div style={{ maxWidth: "65%", padding: "7px 12px 8px 12px", borderRadius: isOut ? "8px 0px 8px 8px" : "0px 8px 8px 8px", background: isInternal ? "#fff8dc" : isOut ? T.msgOut : T.msgIn, boxShadow: `0 1px 2px ${T.shadow}`, fontSize: 14, lineHeight: 1.5, color: T.text }}>
                              {isInternal && <div style={{ fontSize: 10, fontWeight: 700, color: "#8a6914", marginBottom: 4 }}>📝 NOTA INTERNA</div>}
                              <div style={{ wordBreak: "break-word" }}>{msg.content}</div>
                              <div style={{ fontSize: 10, color: "#667781", marginTop: 2, textAlign: isOut ? "right" : "left" }}>{new Date(msg.created_at).toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" })}{isOut && <span style={{ marginLeft: 4, color: "#53bdeb" }}>✓✓</span>}</div>
                            </div>
                          </div>
                        );
                      })}
                    <div ref={bottomRef} />
                  </div>

                  {/* Input */}
                  <div style={{ padding: "10px 14px", borderTop: `1px solid ${T.border}`, background: T.input }}>
                    {suggestion && (
                      <div style={{ marginBottom: 10, padding: "12px 14px", background: "#f5f0ff", border: "1px solid #7c4dff33", borderRadius: 10 }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8 }}>
                          <span>✨</span><span style={{ fontSize: 11, fontWeight: 700, color: "#7c3aed" }}>SUGESTÃO DO CO-PILOT</span>
                          <span onClick={() => setSuggestion("")} style={{ marginLeft: "auto", fontSize: 16, cursor: "pointer", color: "#667781" }}>×</span>
                        </div>
                        <div style={{ fontSize: 13, color: "#4c1d95", lineHeight: 1.5, marginBottom: 10 }}>{suggestion}</div>
                        <div style={{ display: "flex", gap: 8 }}>
                          <button onClick={() => { setInput(suggestion); setSuggestion(""); }} style={{ flex: 1, padding: "6px 0", borderRadius: 7, border: "none", background: "linear-gradient(135deg, #7c4dff, #5b21b6)", color: "#fff", fontSize: 12, fontWeight: 700, cursor: "pointer", fontFamily: "inherit" }}>✓ Usar resposta</button>
                          <button onClick={fetchSuggestion} style={{ padding: "6px 14px", borderRadius: 7, border: "1px solid #7c4dff44", background: "transparent", color: "#a78bfa", fontSize: 12, cursor: "pointer", fontFamily: "inherit" }}>↻ Nova</button>
                          <button onClick={() => setSuggestion("")} style={{ padding: "6px 14px", borderRadius: 7, border: "1px solid #e9edef", background: "transparent", color: "#667781", fontSize: 12, cursor: "pointer", fontFamily: "inherit" }}>Ignorar</button>
                        </div>
                      </div>
                    )}
                    {noteMode && (
                      <div style={{ marginBottom: 8, padding: "6px 12px", background: "#fffbeb", border: "1px solid #fcd34d", borderRadius: 7, display: "flex", alignItems: "center", gap: 8 }}>
                        <span style={{ fontSize: 12 }}>📝</span>
                        <span style={{ fontSize: 12, color: "#92400e", fontWeight: 600 }}>Modo nota interna — visível só para a equipe</span>
                        <span onClick={() => setNoteMode(false)} style={{ marginLeft: "auto", cursor: "pointer", color: "#92400e", fontSize: 14, opacity: 0.7 }}>×</span>
                      </div>
                    )}
                    <div style={{ display: "flex", gap: 8, alignItems: "flex-end" }}>
                      <button onClick={() => setNoteMode(n => !n)} style={{ padding: "9px 10px", borderRadius: 9, border: `1px solid ${noteMode ? "#ffd60044" : "#d1d7db"}`, background: noteMode ? "#ffd60015" : "transparent", color: noteMode ? "#ffd600" : "#667781", fontSize: 14, cursor: "pointer", flexShrink: 0 }}>📝</button>
                      <textarea value={input} onChange={e => setInput(e.target.value)} onKeyDown={e => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); sendMessage(); } }} placeholder={noteMode ? "Escreva uma nota interna..." : "Digite uma mensagem... (Enter para enviar)"} rows={1} style={{ flex: 1, padding: "9px 13px", background: noteMode ? "#fffbeb" : T.input, border: `1px solid ${noteMode ? "#ffd60033" : T.inputBdr}`, borderRadius: 9, color: noteMode ? "#92400e" : "#111b21", fontSize: 14, outline: "none", resize: "none", fontFamily: "inherit", lineHeight: 1.5, maxHeight: 120, overflowY: "auto" }} />
                      <button onClick={sendMessage} disabled={sending || !input.trim()} style={{ padding: "9px 16px", borderRadius: 9, border: "none", background: sending || !input.trim() ? "#e9edef" : noteMode ? "linear-gradient(135deg, #ffd600, #f57f17)" : "linear-gradient(135deg, #00a884, #017561)", color: sending || !input.trim() ? "#667781" : "#000", fontSize: 14, fontWeight: 700, cursor: sending || !input.trim() ? "not-allowed" : "pointer", fontFamily: "inherit", flexShrink: 0 }}>{sending ? "..." : noteMode ? "Nota" : "Enviar"}</button>
                    </div>
                  </div>
                </div>
                {showTasks && <TasksPanel convId={selected.id} agents={agents} onClose={() => { setShowTasks(false); fetchPendingTasks(); }} onTaskDone={fetchPendingTasks} />}
              </div>
            ) : (
              <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", flexDirection: "column", gap: 14, color: "#54656f" }}>
                <div style={{ fontSize: 44 }}>💬</div>
                <div style={{ fontSize: 17, fontWeight: 600, color: "#667781" }}>Selecione uma conversa</div>
                <div style={{ fontSize: 13, color: T.text2 }}>Escolha uma conversa na lista ao lado</div>
              </div>
            )}
            </div>
          </div>
        )}
      </div>

      {/* Modals */}
      {showAssign && selected && <AssignModal conversation={selected} agents={agents} onAssign={assignConv} onClose={() => setShowAssign(false)} />}
      {showLabelPicker && selected && (
        <LabelPickerModal
          conversation={selected}
          labels={labels}
          onToggle={toggleLabel}
          onClose={() => setShowLabelPicker(false)}
          onManage={() => { setShowLabelPicker(false); setShowLabelManager(true); }}
        />
      )}
      {showUpgrade && <UpgradeModal feature={showUpgrade} currentPlan={aiCredits?.plan} onClose={() => setShowUpgrade(null)} />}
      {showBuyCredits && <BuyCreditsModal tenantId={TENANT_ID} authHeaders={headers} plan={aiCredits?.plan} onClose={() => setShowBuyCredits(false)} onSuccess={(n) => { setAiCredits(p => p ? { ...p, credits: p.credits + n } : p); showToast(`✅ +${n} créditos adicionados!`); }} />}
      {showLabelManager && (
        <LabelManagerModal
          labels={labels}
          onChange={(newLabels) => { setLabels(newLabels); saveLabels(newLabels); }}
          onClose={() => setShowLabelManager(false)}
          tenantId={TENANT_ID}
          authHeaders={headers}
          labelsApiError={labelsError}
        />
      )}
      {showColManager && <ColumnManagerModal columns={kanbanCols} onChange={setKanbanCols} onClose={() => setShowColManager(false)} />}

      {/* ── Mobile Bottom Navigation ──────────────────────── */}
      {isMobile && (
        <div style={{ height: 58, flexShrink: 0, borderTop: `1px solid ${T.border}`, background: T.topbar, display: "flex", alignItems: "center", justifyContent: "space-around", paddingBottom: "env(safe-area-inset-bottom, 0px)", zIndex: 100 }}>
          {[
            { id: "inbox",        icon: "💬", label: "Inbox" },
            { id: "kanban",       icon: "🗂",  label: "Kanban" },
            { id: "tasks_global", icon: "✅",  label: "Tarefas", badge: totalPendingTasks },
            { id: "disparos",     icon: "📢",  label: "Disparos" },
            { id: "__more__",     icon: "⋯",   label: "Mais" },
          ].map(tab => {
            if (tab.id === "__more__") {
              return (
                <button key="more" onClick={() => setShowMobileMenu(v => !v)}
                  style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 2, padding: "6px 12px", border: "none", background: "transparent", color: showMobileMenu ? "#00a884" : T.text2, fontSize: 20, cursor: "pointer", fontFamily: "inherit", minWidth: 52 }}>
                  <span style={{ lineHeight: 1, fontSize: 22, fontWeight: 700 }}>⋯</span>
                  <span style={{ fontSize: 10, fontWeight: 600 }}>Mais</span>
                </button>
              );
            }
            const isActive = view === tab.id;
            return (
              <button key={tab.id} onClick={() => { setView(tab.id); setShowMobileMenu(false); }}
                style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 2, padding: "6px 12px", border: "none", background: "transparent", color: isActive ? "#00a884" : T.text2, cursor: "pointer", fontFamily: "inherit", minWidth: 52, position: "relative" }}>
                <span style={{ fontSize: 20, lineHeight: 1 }}>{tab.icon}</span>
                <span style={{ fontSize: 10, fontWeight: isActive ? 700 : 500 }}>{tab.label}</span>
                {isActive && <span style={{ position: "absolute", bottom: 0, left: "50%", transform: "translateX(-50%)", width: 24, height: 2, background: "#00a884", borderRadius: 2 }} />}
                {(tab.badge || 0) > 0 && <span style={{ position: "absolute", top: 4, right: 6, background: "#ff6d00", color: "#fff", fontSize: 9, fontWeight: 800, padding: "1px 5px", borderRadius: 10, lineHeight: 1.4 }}>{tab.badge}</span>}
              </button>
            );
          })}
        </div>
      )}

      {/* ── Mobile "Mais" slide-up menu ───────────────────── */}
      {isMobile && showMobileMenu && (
        <div style={{ position: "fixed", inset: 0, zIndex: 8000, background: "#00000044" }} onClick={() => setShowMobileMenu(false)}>
          <div style={{ position: "absolute", bottom: 0, left: 0, right: 0, background: T.card, borderRadius: "20px 20px 0 0", padding: "20px 16px 32px", boxShadow: "0 -8px 32px #00000033" }}
            onClick={e => e.stopPropagation()}>
            <div style={{ width: 40, height: 4, background: T.border, borderRadius: 2, margin: "0 auto 20px" }} />
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 8 }}>
              {[
                { id: "leads",      icon: "🏷",  label: "Leads" },
                { id: "config",     icon: "⚙️",   label: "Config IA" },
                { id: "relatorios", icon: "📈",   label: "Relatórios" },
                { id: "whatsapp",   icon: "📱",   label: "WhatsApp" },
                { id: "admin",      icon: "🔐",   label: "Admin" },
                ...(trialInfo?.status === "trial" ? [{ id: "upgrade", icon: "⭐", label: "Assinar" }] : []),
              ].filter(t => {
                if (["whatsapp","admin","relatorios"].includes(t.id) && auth.user.role !== "admin") return false;
                return true;
              }).map(tab => (
                <button key={tab.id} onClick={() => { setView(tab.id); setShowMobileMenu(false); }}
                  style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 6, padding: "14px 8px", borderRadius: 14, border: "none", background: view === tab.id ? "#00a88418" : T.hover, color: view === tab.id ? "#00a884" : T.text, fontSize: 11, fontWeight: 600, cursor: "pointer", fontFamily: "inherit" }}>
                  <span style={{ fontSize: 24 }}>{tab.icon}</span>
                  <span style={{ textAlign: "center", lineHeight: 1.2, fontSize: 10 }}>{tab.label}</span>
                </button>
              ))}
            </div>
            <div style={{ marginTop: 16, paddingTop: 16, borderTop: `1px solid ${T.border}`, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
              <div style={{ fontSize: 13, color: T.text2 }}>👤 {auth.user.name}</div>
              <div style={{ display: "flex", gap: 8 }}>
                <button onClick={onLogout} style={{ padding: "6px 14px", borderRadius: 8, border: "1px solid #f4433333", background: "transparent", color: "#f44336", fontSize: 12, fontWeight: 600, cursor: "pointer", fontFamily: "inherit" }}>Sair</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ── Global Toast ─────────────────────────────────── */}
      {toast && (
        <div style={{ position: "fixed", bottom: 80, left: "50%", transform: "translateX(-50%)", zIndex: 9999, pointerEvents: "none" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "12px 20px", borderRadius: 12, background: "#1f2937", boxShadow: "0 4px 20px #00000040", color: "#fff", fontSize: 13, fontWeight: 600, whiteSpace: "nowrap", minWidth: 180, maxWidth: 380 }}>
            {toast.loading && (
              <span style={{ width: 14, height: 14, border: "2px solid #ffffff44", borderTop: `2px solid ${toast.color || "#00a884"}`, borderRadius: "50%", display: "inline-block", animation: "spin 0.7s linear infinite", flexShrink: 0 }} />
            )}
            {!toast.loading && (
              <span style={{ width: 8, height: 8, borderRadius: "50%", background: toast.color || "#00a884", flexShrink: 0 }} />
            )}
            <span style={{ flex: 1 }}>{toast.msg}</span>
          </div>
        </div>
      )}
    </div>
  );
}
