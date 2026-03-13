import asyncio
import random
from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks, Body, UploadFile, File, Form, Request
import concurrent.futures
_thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)

async def run_sync(fn):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_thread_pool, fn)

# ── CACHE: Redis (preferido) + in-memory fallback ─────────
# redis imported lazily inside _get_redis()

_cache: dict = {}  # fallback in-memory
_redis = None

def _get_redis():
    global _redis
    if _redis is not None:
        return _redis
    url = os.getenv("REDIS_URL") or os.getenv("REDIS_PRIVATE_URL") or os.getenv("CACHE_REDIS_URI")
    if url:
        try:
            import redis as _redis_lib, pickle as _pickle
            globals()["_pickle"] = _pickle
            _redis = _redis_lib.from_url(url, decode_responses=False, socket_connect_timeout=2, socket_timeout=2)
            _redis.ping()
            print(f"Redis connected: {url[:30]}...")
        except Exception as e:
            print(f"Redis unavailable, using memory cache: {e}")
            _redis = False
    else:
        _redis = False
    return _redis

def cache_get(key, stale_ok=False):
    r = _get_redis()
    if r:
        try:
            import pickle
            raw = r.get(f"7crm:{key}")
            if raw:
                return pickle.loads(raw)
        except: pass
    v = _cache.get(key)
    if not v: return None
    age = datetime.utcnow().timestamp() - v["ts"]
    if age < v["ttl"]: return v["data"]
    if stale_ok and age < v["ttl"] * 10: return v["data"]
    return None

def cache_set(key, data, ttl=30):
    r = _get_redis()
    if r:
        try:
            import pickle
            r.setex(f"7crm:{key}", ttl, pickle.dumps(data))
        except: pass
    _cache[key] = {"data": data, "ts": datetime.utcnow().timestamp(), "ttl": ttl}

def cache_del(key):
    r = _get_redis()
    if r:
        try: r.delete(f"7crm:{key}")
        except: pass
    _cache.pop(key, None)

# ── Auto-pilot pause helpers ───────────────────────────────────────────────
# Quando atendente responde manualmente, pausa o auto-pilot da conversa por
# AUTOPILOT_PAUSE_SECS (default 30min). Usa Redis com TTL para auto-expirar.
AUTOPILOT_PAUSE_SECS = 1800  # 30 minutos

def autopilot_pause(conv_id: str, secs: int = AUTOPILOT_PAUSE_SECS):
    """Pausa o auto-pilot para uma conversa por `secs` segundos."""
    r = _get_redis()
    if r:
        try: r.setex(f"7crm:ap_pause:{conv_id}", secs, "1")
        except: pass

def autopilot_is_paused(conv_id: str) -> bool:
    """Retorna True se o auto-pilot está pausado para esta conversa."""
    r = _get_redis()
    if r:
        try: return bool(r.exists(f"7crm:ap_pause:{conv_id}"))
        except: pass
    return False

def autopilot_resume(conv_id: str):
    """Remove a pausa manualmente (ex: quando atendente quiser reativar)."""
    r = _get_redis()
    if r:
        try: r.delete(f"7crm:ap_pause:{conv_id}")
        except: pass
# ──────────────────────────────────────────────────────────────────────────

def cache_is_stale(key):
    r = _get_redis()
    if r:
        try:
            ttl_remaining = r.ttl(f"7crm:{key}")
            return ttl_remaining <= 0
        except: pass
    v = _cache.get(key)
    if not v: return True
    return (datetime.utcnow().timestamp() - v["ts"]) >= v["ttl"]
# ── PLAN CREDIT CONSTANTS ────────────────────────────────
PLAN_CREDITS = {
    "trial":      1000,  # onboarding consome ~1000 créditos — suficiente pra ver resultado
    "starter":    0,     # sem IA
    "pro":        300,   # ~1 semana de uso intenso → incentiva compra de pacote
    "business":   1000,  # generoso mas não infinito
    "enterprise": 99999,
}
PLAN_RESETS = {
    "pro":       True,
    "business":  True,
    "enterprise": True,
}

# ── CREDIT HELPERS ───────────────────────────────────────
def get_tenant_credits(tenant_id: str):
    """Returns (credits_remaining, plan, credits_limit). Also resets if monthly."""
    tenant = supabase.table("tenants").select("plan,ai_credits,ai_credits_reset_at,ai_credits_purchased").eq("id", tenant_id).single().execute().data
    plan = tenant.get("plan", "starter")
    limit = PLAN_CREDITS.get(plan, 10)
    credits = tenant.get("ai_credits")
    reset_at = tenant.get("ai_credits_reset_at")
    now = datetime.utcnow()

    # Initialize credits if never set
    if credits is None:
        supabase.table("tenants").update({"ai_credits": limit, "ai_credits_reset_at": now.isoformat()}).eq("id", tenant_id).execute()
        return limit, plan, limit

    # Monthly reset for Pro/Business
    if PLAN_RESETS.get(plan) and reset_at:
        reset_dt = datetime.fromisoformat(reset_at.replace("Z","").replace("+00:00",""))
        if (now - reset_dt).days >= 30:
            new_credits = limit + (tenant.get("ai_credits_purchased") or 0)
            supabase.table("tenants").update({"ai_credits": new_credits, "ai_credits_reset_at": now.isoformat()}).eq("id", tenant_id).execute()
            return new_credits, plan, limit

    return credits, plan, limit

def consume_credit(tenant_id: str, amount: int = 1):
    """Deduct credits. Returns (ok, credits_remaining, error_detail)."""
    credits, plan, limit = get_tenant_credits(tenant_id)
    if credits < amount:
        return False, credits, f"Créditos insuficientes. Restam {credits} crédito(s). Faça upgrade ou compre mais créditos."
    new_val = credits - amount
    supabase.table("tenants").update({"ai_credits": new_val}).eq("id", tenant_id).execute()
    return True, new_val, None

from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional, List
from supabase import create_client, Client
import httpx, os, jwt, bcrypt, asyncio, random, base64, json, secrets as _secrets_mod
from datetime import datetime, timedelta

app = FastAPI(title="7CRM API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(_startup_sync_names())
    # Recovery: broadcasts presos em "sending" são cancelados automaticamente
    # (acontece quando Railway redeployar com background tasks rodando)
    try:
        stuck = supabase.table("broadcasts").select("id,name").eq("status", "sending").execute().data or []
        for b in stuck:
            supabase.table("broadcasts").update({"status": "cancelled", "finished_at": datetime.utcnow().isoformat()}).eq("id", b["id"]).execute()
            print(f"[STARTUP] Broadcast preso cancelado: {b['name']} ({b['id']})")
    except Exception as e:
        print(f"[STARTUP] Erro ao cancelar broadcasts presos: {e}")

SUPABASE_URL      = os.getenv("SUPABASE_URL")
SUPABASE_KEY      = os.getenv("SUPABASE_SERVICE_KEY")
WAHA_URL          = os.getenv("WAHA_URL", "http://localhost:3000")
WAHA_KEY          = os.getenv("WAHA_KEY", "pulsekey")
BACKEND_URL       = os.getenv("BACKEND_URL", "https://7zap-inbox-production.up.railway.app")
WEBHOOK_SECRET    = os.getenv("WEBHOOK_SECRET") or os.getenv("INBOX_API_KEY") or "7zap_inbox_secret"
INBOX_API_KEY     = os.getenv("INBOX_API_KEY") or "7zap_inbox_secret"
if INBOX_API_KEY.endswith("_CHANGE_ME"):
    import warnings; warnings.warn("⚠️  INBOX_API_KEY não configurada — usando valor padrão inseguro!")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
OPENAI_API_KEY    = os.getenv("OPENAI_API_KEY", "")
MONITOR_PHONE     = os.getenv("MONITOR_PHONE", "")  # ex: 5511940774447 — recebe alertas do sistema

# ── AI HELPER — usa OpenAI GPT-4o Mini (mais barato) com fallback pro Claude ──
async def call_ai(system: str, user: str, max_tokens: int = 300, prefer_openai: bool = True, timeout: int = 30) -> str:
    """Chama GPT-4o Mini se disponível, senão fallback pro Claude Haiku."""
    if prefer_openai and OPENAI_API_KEY:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.post("https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini", "max_tokens": max_tokens,
                      "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]})
            data = r.json()
            return data["choices"][0]["message"]["content"]
    # Fallback: Claude Haiku
    if ANTHROPIC_API_KEY:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={"model": "claude-haiku-4-5-20251001", "max_tokens": max_tokens,
                      "system": system, "messages": [{"role": "user", "content": user}]})
            return r.json()["content"][0]["text"]
    raise Exception("Nenhuma API de IA configurada (OPENAI_API_KEY ou ANTHROPIC_API_KEY)")

def trim_context(msgs: list, max_msgs: int = 15) -> tuple[str, list, list]:
    """
    Divide mensagens em recentes (últimas N) e antigas (restante).
    Retorna: (history_str_recentes, msgs_recentes, msgs_antigas)
    """
    clean = [m for m in msgs if not m.get("is_internal_note")]
    old   = clean[:-max_msgs] if len(clean) > max_msgs else []
    recent = clean[-max_msgs:] if len(clean) > max_msgs else clean
    history = "\n".join([
        f"{'Cliente' if m['direction']=='inbound' else 'Atendente'}: {(m.get('content') or '')[:300]}"
        for m in recent
    ])
    return history, recent, old

async def get_or_generate_summary(conv_id: str, old_msgs: list, existing_summary: str | None) -> str | None:
    """
    Retorna resumo das mensagens antigas.
    - Se já existe resumo salvo e não há novas mensagens antigas além do já resumido: reutiliza.
    - Se há mensagens antigas não resumidas: gera (ou atualiza) resumo e salva no banco.
    - Se não há mensagens antigas: retorna None.
    """
    if not old_msgs:
        return None

    # Verifica se o resumo existente já cobre todas as mensagens antigas
    # Estratégia simples: sempre regenera se tiver mais de 15 msgs antigas novas
    # (na prática, regenera raramente pois conversas crescem devagar)
    if existing_summary and len(old_msgs) <= 20:
        # Provavelmente já foi resumido antes — reutiliza sem custo
        return existing_summary

    # Gera novo resumo das mensagens antigas
    old_text = "\n".join([
        f"{'Cliente' if m['direction']=='inbound' else 'Atendente'}: {(m.get('content') or '')[:200]}"
        for m in old_msgs
    ])
    summary = await call_ai(
        "Você resume conversas de atendimento WhatsApp de forma compacta. "
        "Foque em: problema/interesse do cliente, o que já foi discutido, decisões tomadas. "
        "Máximo 5 linhas. Seja direto e objetivo.",
        f"Resuma esta parte da conversa (mensagens antigas):\n\n{old_text}",
        max_tokens=200
    )
    # Salva no banco para reutilizar nas próximas chamadas
    try:
        supabase.table("conversations").update({"ai_summary": summary}).eq("id", conv_id).execute()
    except Exception:
        pass  # Não quebra se a coluna ainda não existir
    return summary
PAGARME_API_KEY   = os.getenv("PAGARME_API_KEY", "")
SUPER_ADMIN_TENANT = os.getenv("SUPER_ADMIN_TENANT", "98c38c97-2796-471f-bfc9-f093ff3ae6e9")
JWT_SECRET        = os.getenv("JWT_SECRET") or "7crm_super_secret_CHANGE_ME_IN_PROD"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
security = HTTPBearer(auto_error=False)

@app.on_event("startup")
async def start_keepalive():
    asyncio.create_task(keepalive_loop())
    asyncio.create_task(fix_existing_sessions_webhook())
    asyncio.create_task(watchguard_loop())

async def ensure_webhooks():
    """Auto-configure webhooks for all active WAHA sessions that are missing it."""
    return  # DISABLED: PUT to WAHA causes session restart loop
    if not WAHA_URL:
        return
    webhook_url = f"{BACKEND_URL}/webhook/message"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(f"{WAHA_URL}/api/sessions", headers=waha_headers())
            if r.status_code != 200:
                return
            sessions = r.json()
            if not isinstance(sessions, list):
                sessions = sessions.get("data", sessions.get("sessions", []))
            for session in sessions:
                name = session.get("name", "")
                status = session.get("status", "")
                if status not in ("WORKING", "CONNECTED", "AUTHENTICATED"):
                    continue
                # Check if webhook already configured correctly
                webhooks = session.get("config", {}).get("webhooks", [])
                already_set = any(w.get("url") == webhook_url for w in webhooks)
                if already_set:
                    continue
                # WAHA Plus: PUT /api/sessions/{name} to update config
                r2 = await client.put(
                    f"{WAHA_URL}/api/sessions/{name}",
                    headers=waha_headers(),
                    json={"config": {"webhooks": [{
                        "url": webhook_url,
                        "events": ["message", "message.any", "session.status"],
                        "customHeaders": [{"name": "x-api-key", "value": WEBHOOK_SECRET}]
                    }]}}
                )
                print(f"✅ Auto-webhook configured for session '{name}': {r2.status_code}")
    except Exception as e:
        print(f"ensure_webhooks error: {e}")

async def fix_existing_sessions_webhook():
    """Corrige sessoes existentes que ainda usam /webhook/message → /webhook/inbox"""
    return  # DISABLED: PUT to WAHA causes session restart loop
    if not WAHA_URL:
        return
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(f"{WAHA_URL}/api/sessions", headers=waha_headers())
            if r.status_code != 200:
                return
            sessions = r.json()
            fixed = 0
            for s in sessions:
                name = s.get("name", "")
                webhooks = s.get("config", {}).get("webhooks", [])
                for wh in webhooks:
                    if "/webhook/message" in wh.get("url", ""):
                        # Corrigir para /webhook/inbox
                        new_url = wh["url"].replace("/webhook/message", "/webhook/inbox")
                        await client.put(f"{WAHA_URL}/api/sessions/{name}", headers=waha_headers(),
                            json={"config": {"webhooks": [{
                                "url": new_url,
                                "events": ["message", "message.any", "session.status"],
                                "customHeaders": wh.get("customHeaders", [])
                            }]}})
                        print(f"[STARTUP] Corrigido webhook de {name}: /webhook/message → /webhook/inbox")
                        fixed += 1
            if fixed > 0:
                print(f"[STARTUP] {fixed} sessoes corrigidas")
    except Exception as e:
        print(f"[STARTUP] fix_existing_sessions_webhook error: {e}")

# ── MONITORAMENTO DE SAÚDE DO WAHA ───────────────────────────────────────────
# Estado interno: rastreia o que já foi alertado pra não spammar
_monitor_state: dict = {
    "waha_down": False,       # True se WAHA não responde
    "sessions_down": set(),   # sessões que estão offline
    "last_alert_at": {},      # ts do último alerta por chave (anti-spam)
}

async def _send_monitor_alert(text: str, key: str, cooldown_min: int = 30):
    """Envia alerta via WhatsApp pro MONITOR_PHONE. Respeita cooldown pra não spammar."""
    if not MONITOR_PHONE or not WAHA_URL:
        return
    now = datetime.utcnow().timestamp()
    last = _monitor_state["last_alert_at"].get(key, 0)
    if now - last < cooldown_min * 60:
        return  # dentro do cooldown, não manda de novo
    _monitor_state["last_alert_at"][key] = now
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            # Usa a primeira sessão disponível que esteja WORKING
            sessions_resp = await client.get(f"{WAHA_URL}/api/sessions", headers=waha_headers())
            sessions = sessions_resp.json() if sessions_resp.status_code == 200 else []
            working = next((s["name"] for s in sessions if s.get("status") == "WORKING"), None)
            if not working:
                print(f"[monitor] Não tem sessão WORKING pra enviar alerta: {text}")
                return
            await client.post(
                f"{WAHA_URL}/api/sendText",
                headers=waha_headers(),
                json={"session": working, "chatId": f"{MONITOR_PHONE}@c.us", "text": text}
            )
            print(f"[monitor] ✅ Alerta enviado para {MONITOR_PHONE}: {text[:60]}")
    except Exception as e:
        print(f"[monitor] ❌ Falha ao enviar alerta: {e}")

async def _check_waha_health():
    """Verifica saúde do WAHA e sessões. Envia alertas se necessário."""
    if not WAHA_URL:
        return
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.get(f"{WAHA_URL}/api/sessions", headers=waha_headers())
        if resp.status_code != 200:
            raise Exception(f"HTTP {resp.status_code}")

        # WAHA respondeu — se estava down, avisa que voltou
        if _monitor_state["waha_down"]:
            _monitor_state["waha_down"] = False
            await _send_monitor_alert(
                "✅ *7CRM — WAHA voltou!*\n\nServidor WhatsApp respondendo normalmente.",
                key="waha_up", cooldown_min=5
            )

        sessions = resp.json() or []
        current_down = set()

        for s in sessions:
            name = s.get("name", "")
            status = s.get("status", "")
            if status not in ("WORKING", "STARTING"):
                current_down.add(name)

        # Sessões que ficaram offline agora (não estavam antes)
        newly_down = current_down - _monitor_state["sessions_down"]
        # Sessões que voltaram
        newly_up = _monitor_state["sessions_down"] - current_down

        for name in newly_down:
            # Tenta pegar o número da sessão pra deixar o alerta mais útil
            session_info = next((s for s in sessions if s.get("name") == name), {})
            status = session_info.get("status", "UNKNOWN")
            me = session_info.get("me", {})
            phone_display = me.get("pushname") or me.get("id", name) if me else name
            await _send_monitor_alert(
                f"🔴 *7CRM — Sessão desconectada!*\n\n"
                f"Sessão: `{name}`\n"
                f"Número: {phone_display}\n"
                f"Status: {status}\n\n"
                f"Acesse: http://89.167.96.250:3000\n"
                f"Reconecte escaneando o QR Code.",
                key=f"session_down_{name}", cooldown_min=60
            )

        for name in newly_up:
            await _send_monitor_alert(
                f"✅ *7CRM — Sessão reconectada!*\n\nSessão `{name}` está WORKING novamente.",
                key=f"session_up_{name}", cooldown_min=5
            )

        _monitor_state["sessions_down"] = current_down

    except Exception as e:
        print(f"[monitor] WAHA não respondeu: {e}")
        if not _monitor_state["waha_down"]:
            _monitor_state["waha_down"] = True
            await _send_monitor_alert(
                f"🔴 *7CRM — WAHA offline!*\n\n"
                f"Servidor WhatsApp não está respondendo.\n"
                f"VPS: 89.167.96.250\n\n"
                f"Verifique: http://89.167.96.250:3000\n"
                f"Erro: {str(e)[:100]}",
                key="waha_down", cooldown_min=30
            )

# ─────────────────────────────────────────────────────────────────────────────

async def keepalive_loop():
    await asyncio.sleep(15)
    ping_count = 0
    while True:
        # Ping Supabase a cada 60s para evitar cold start (free tier adormece conexão)
        try:
            supabase.table("tenants").select("id").limit(1).execute()
        except:
            pass
        ping_count += 1
        # A cada 5 pings (5 min), aquece também as tabelas mais consultadas
        if ping_count % 5 == 0:
            try:
                supabase.table("conversations").select("id").limit(1).execute()
                supabase.table("messages").select("id").limit(1).execute()
                supabase.table("contacts").select("id").limit(1).execute()
            except:
                pass
        # A cada 2 pings (2 min), verifica saúde do WAHA e sessões
        if ping_count % 2 == 0:
            asyncio.create_task(_check_waha_health())
        # A cada 10 pings (10 min), re-verifica webhooks WAHA
        if ping_count % 10 == 0:
            asyncio.create_task(ensure_webhooks())
        await asyncio.sleep(60)

async def _startup_sync_names():
    """Roda uma vez no startup: sincroniza nomes de contatos sem nome via WAHA."""
    await asyncio.sleep(15)  # espera backend estabilizar
    try:
        tenants = supabase.table("tenants").select("id").execute().data
        for tenant in tenants:
            tid = tenant["id"]
            contacts = supabase.table("contacts").select("id,phone,name").eq("tenant_id", tid).execute().data or []
            nameless = [c for c in contacts if not c.get("name") or c["name"] == c["phone"] or c["name"] == (c["phone"] or "").split("@")[0]]
            if not nameless:
                continue
            instances = supabase.table("gateway_instances").select("instance_name").eq("tenant_id", tid).execute().data or []
            for inst in instances:
                iname = inst.get("instance_name")
                if not iname: continue
                try:
                    async with httpx.AsyncClient(timeout=12) as client:
                        r = await client.get(f"{WAHA_URL}/api/{iname}/chats?limit=1", headers=waha_headers())
                        chats = r.json() if r.status_code == 200 else []
                    name_map = {}
                    for chat in (chats if isinstance(chats, list) else []):
                        jid = chat.get("id", {}).get("_serialized", "")
                        name = chat.get("name", "")
                        if jid and name and not name.replace("+","").replace(" ","").replace("-","").isdigit():
                            name_map[jid] = name
                    updated = 0
                    for contact in nameless:
                        phone = contact["phone"]
                        if phone in name_map:
                            supabase.table("contacts").update({"name": name_map[phone]}).eq("id", contact["id"]).execute()
                            updated += 1
                    print(f"[startup_sync] tenant={tid[:8]} inst={iname}: {updated}/{len(nameless)} contatos atualizados")
                except Exception as e:
                    print(f"[startup_sync] erro inst={iname}: {e}")
    except Exception as e:
        print(f"[startup_sync] erro geral: {e}")


@app.get("/ping")
async def ping():
    return {"ok": True, "ts": datetime.utcnow().isoformat()}

def create_jwt(user, session_id: str):
    payload = {"sub": user["id"], "email": user["email"], "role": user["role"], "tenant_id": user["tenant_id"], "name": user["name"], "session_id": session_id, "exp": datetime.utcnow() + timedelta(hours=168)}
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def decode_jwt(token):
    try: return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except: raise HTTPException(status_code=401, detail="Token inválido ou expirado")

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not credentials: raise HTTPException(status_code=401, detail="Token necessário")
    payload = decode_jwt(credentials.credentials)
    # Single-session check: verify session_id still matches DB
    session_id = payload.get("session_id")
    if session_id:
        db_user = supabase.table("users").select("session_id,is_active").eq("id", payload["sub"]).single().execute().data
        if not db_user or not db_user.get("is_active"):
            raise HTTPException(status_code=401, detail="Usuário inativo")
        if db_user.get("session_id") != session_id:
            raise HTTPException(status_code=401, detail="Sessão encerrada. Outro dispositivo fez login com esta conta.")
    return payload

async def require_admin(user=Depends(get_current_user)):
    if user.get("role") != "admin": raise HTTPException(status_code=403, detail="Apenas admins")
    return user

async def require_super_admin(user=Depends(get_current_user)):
    if user.get("role") != "admin": raise HTTPException(status_code=403, detail="Apenas admins")
    if user.get("tenant_id") != SUPER_ADMIN_TENANT: raise HTTPException(status_code=403, detail="Acesso negado")
    return user

def verify_key(x_api_key: str = Header(...)):
    if x_api_key != INBOX_API_KEY: raise HTTPException(status_code=401, detail="Unauthorized")

# ── TENANT ENFORCEMENT ────────────────────────────────────
# Endpoints que recebem tenant_id como query param DEVEM usar esta função
# para garantir que o tenant_id do token bate com o parâmetro da requisição.
# Isso impede que um usuário passe o tenant_id de outro e acesse dados alheios.
async def enforce_tenant(
    tenant_id: str = "",
    user: dict = Depends(get_current_user)
) -> str:
    """Retorna o tenant_id do JWT — ignora qualquer tenant_id passado na URL."""
    return user["tenant_id"]

# ── MEDIA PROXY ───────────────────────────────────────────
@app.get("/media/proxy", dependencies=[Depends(get_current_user)])
async def media_proxy(url: str):
    """Proxy WAHA media through backend (adds auth header, avoids CORS)"""
    from fastapi.responses import Response
    if not url:
        raise HTTPException(400, "url required")
    if WAHA_URL and not url.startswith(WAHA_URL):
        if not url.startswith("http"):
            url = f"{WAHA_URL}{url}"
        else:
            raise HTTPException(403, "URL not allowed")
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(url, headers=waha_headers())
            content_type = r.headers.get("content-type", "application/octet-stream")
            return Response(content=r.content, media_type=content_type,
                headers={"Cache-Control": "max-age=3600",
                         "Content-Disposition": r.headers.get("content-disposition", "")})
    except Exception as e:
        raise HTTPException(502, f"Failed to fetch media: {e}")

# ── WAHA HEADERS (X-Api-Key — formato correto WAHA) ──────
def waha_headers():
    return {"X-Api-Key": WAHA_KEY, "Content-Type": "application/json"}

# ── Schemas ──────────────────────────────────────────────
class LoginRequest(BaseModel):
    email: str
    password: str

class CreateUser(BaseModel):
    tenant_id: str; name: str; email: str; password: str; role: str = "agent"; avatar_color: Optional[str] = "#00c853"; permissions: str = "read_write"; allowed_instances: Optional[List[str]] = []; phone: Optional[str] = None

class UpdateUser(BaseModel):
    name: Optional[str] = None; email: Optional[str] = None; password: Optional[str] = None
    role: Optional[str] = None; is_active: Optional[bool] = None; avatar_color: Optional[str] = None; permissions: Optional[str] = None; allowed_instances: Optional[List[str]] = None; phone: Optional[str] = None

class SendMessage(BaseModel):
    conversation_id: str; text: str; sent_by: Optional[str] = None; is_internal_note: Optional[bool] = False

class AssignConversation(BaseModel):
    user_id: Optional[str] = None

class UpdateKanban(BaseModel):
    stage: str

class UpdateLabels(BaseModel):
    label_ids: List[str]

class CreateTask(BaseModel):
    conversation_id: str; title: str; description: Optional[str] = None; assigned_to: Optional[str] = None; due_at: Optional[str] = None

class CreateTaskUpdate(BaseModel):
    content: str; author: Optional[str] = None

class CreateQuickReply(BaseModel):
    title: str; content: str

class UpdateCopilotPrompt(BaseModel):
    tenant_id: str
    instance_name: str  # obrigatório — config sempre por instância
    copilot_prompt: Optional[str] = None
    copilot_auto_mode: str = "off"
    copilot_schedule_start: str = "18:00"
    copilot_schedule_end: str = "09:00"
    copilot_watchguard: bool = False

class CreateBroadcast(BaseModel):
    tenant_id: str; name: str; message: str; interval_min: int = 60; interval_max: int = 120
    scheduled_at: Optional[str] = None; recipients: List[dict]; ai_personalize: bool = False
    resume_conversation: bool = False  # 🔄 Retomar conversa — lê histórico completo, 3 créditos/contato
    instance_name: Optional[str] = None  # instância para envio

class ScheduledMessageCreate(BaseModel):
    tenant_id: str; conversation_id: Optional[str] = None; contact_name: Optional[str] = None
    contact_phone: str; message: str; scheduled_at: str; recurrence: Optional[str] = None

# ── AUTH ─────────────────────────────────────────────────
@app.post("/auth/login")
async def login(body: LoginRequest):
    users = supabase.table("users").select("*").eq("email", body.email.lower().strip()).eq("is_active", True).execute().data
    if not users: raise HTTPException(status_code=401, detail="Email ou senha incorretos")
    user = users[0]
    if not user.get("password_hash"): raise HTTPException(status_code=401, detail="Usuário sem senha definida")
    loop = asyncio.get_event_loop()
    pw_ok = await loop.run_in_executor(_thread_pool, lambda: bcrypt.checkpw(body.password.encode(), user["password_hash"].encode()))
    if not pw_ok: raise HTTPException(status_code=401, detail="Email ou senha incorretos")
    import secrets as _secrets
    session_id = _secrets.token_hex(16)
    supabase.table("users").update({"last_login": datetime.utcnow().isoformat(), "session_id": session_id}).eq("id", user["id"]).execute()
    return {"token": create_jwt(user, session_id), "user": {"id": user["id"], "name": user["name"], "email": user["email"], "role": user["role"], "tenant_id": user["tenant_id"], "avatar_color": user.get("avatar_color", "#00c853"), "allowed_instances": user.get("allowed_instances") or []}}

@app.get("/auth/me")
async def me(user=Depends(get_current_user)):
    return supabase.table("users").select("id,name,email,role,tenant_id,avatar_color,last_login,allowed_instances").eq("id", user["sub"]).single().execute().data

@app.post("/auth/change-password")
async def change_password(body: dict, user=Depends(get_current_user)):
    new_pw = body.get("new_password", "")
    if len(new_pw) < 6: raise HTTPException(status_code=400, detail="Mínimo 6 caracteres")
    db = supabase.table("users").select("password_hash").eq("id", user["sub"]).single().execute().data
    loop = asyncio.get_event_loop()
    pw_ok = await loop.run_in_executor(_thread_pool, lambda: bcrypt.checkpw(body.get("current_password", "").encode(), db["password_hash"].encode()))
    if not pw_ok: raise HTTPException(status_code=401, detail="Senha atual incorreta")
    new_hash = await loop.run_in_executor(_thread_pool, lambda: bcrypt.hashpw(new_pw.encode(), bcrypt.gensalt()).decode())
    supabase.table("users").update({"password_hash": new_hash}).eq("id", user["sub"]).execute()
    return {"ok": True}

# ── ADMIN ────────────────────────────────────────────────
@app.get("/admin/users")
async def list_users_admin(admin=Depends(require_admin)):
    return {"users": supabase.table("users").select("id,name,email,role,is_active,avatar_color,last_login,tenant_id,permissions,allowed_instances").eq("tenant_id", admin["tenant_id"]).order("created_at").execute().data}

@app.post("/admin/users")
async def create_user_admin(body: CreateUser, admin=Depends(require_admin)):
    if supabase.table("users").select("id").eq("email", body.email.lower()).execute().data:
        raise HTTPException(status_code=400, detail="Email já cadastrado")
    pw_hash = bcrypt.hashpw(body.password.encode(), bcrypt.gensalt()).decode()
    user = supabase.table("users").insert({"tenant_id": body.tenant_id, "name": body.name, "email": body.email.lower().strip(), "role": body.role, "password_hash": pw_hash, "is_active": True, "avatar_color": body.avatar_color, "permissions": body.permissions, "allowed_instances": body.allowed_instances or [], "phone": body.phone}).execute().data[0]
    user.pop("password_hash", None)
    return {"user": user}

@app.put("/admin/users/{user_id}")
async def update_user_admin(user_id: str, body: UpdateUser, admin=Depends(require_admin)):
    updates = {k: v for k, v in {"name": body.name, "email": body.email, "role": body.role, "is_active": body.is_active, "avatar_color": body.avatar_color, "permissions": body.permissions, "allowed_instances": body.allowed_instances, "phone": body.phone}.items() if v is not None}
    if body.password:
        if len(body.password) < 6: raise HTTPException(status_code=400, detail="Mínimo 6 caracteres")
        updates["password_hash"] = bcrypt.hashpw(body.password.encode(), bcrypt.gensalt()).decode()
    supabase.table("users").update(updates).eq("id", user_id).execute()
    return {"ok": True}

@app.delete("/admin/users/{user_id}")
async def delete_user_admin(user_id: str, admin=Depends(require_admin)):
    if user_id == admin["sub"]: raise HTTPException(status_code=400, detail="Não pode excluir a própria conta")
    supabase.table("users").update({"is_active": False}).eq("id", user_id).execute()
    return {"ok": True}

# ── WEBHOOK — recebe mensagens do WAHA ───────────────────
@app.post("/webhook/message")
async def receive_message_legacy(payload: dict = None, x_api_key: str = Header(default="")):
    """Alias para /webhook/inbox — WAHA aponta para cá."""
    return await receive_message(payload or {}, x_api_key)

@app.post("/webhook/inbox")
async def receive_message(payload: dict, x_api_key: str = Header(default="")):
    """Aceita formato WAHA: {event, session, payload: {from, fromMe, body, type}}"""
    # Verifica autenticidade do webhook (cabeçalho enviado pelo WAHA)
    if WEBHOOK_SECRET and x_api_key != WEBHOOK_SECRET:
        masked = (x_api_key[:4] + "..." + x_api_key[-4:]) if len(x_api_key) > 8 else f"len={len(x_api_key)}"
        print(f"[WEBHOOK] AUTH REJECTED: received key '{masked}', expected starts with '{WEBHOOK_SECRET[:4]}...{WEBHOOK_SECRET[-4:]}'")
        return {"ok": True}  # Return ok to prevent WAHA retries
    # Suporte a ambos formatos: WAHA e Evolution API legado
    event = payload.get("event", "")

    # ── Evento message.reaction — alguém reagiu a uma mensagem ──
    if event == "message.reaction":
        try:
            data = payload.get("payload", {})
            reaction_val = data.get("reaction", {})
            reacted_msg_id = reaction_val.get("msgId") if isinstance(reaction_val, dict) else data.get("msgId", "")
            emoji = (reaction_val.get("reaction") if isinstance(reaction_val, dict) else reaction_val) or ""
            sender = data.get("from", "") or data.get("participant", "")
            instance_name = payload.get("session", "")
            if reacted_msg_id and instance_name:
                # Atualiza a mensagem no banco pela waha_id
                row = supabase.table("messages").select("id").eq("waha_id", reacted_msg_id).limit(1).execute().data
                if row:
                    supabase.table("messages").update({"reaction": emoji, "reaction_by": sender or "them"}).eq("id", row[0]["id"]).execute()
                    print(f"[REACTION] {emoji} em {reacted_msg_id} por {sender}")
        except Exception as e:
            print(f"[REACTION] erro: {e}")
        return {"ok": True}

    # ── Evento session.status — auto-sync + auto-reconnect ──
    if event == "session.status":
        sess_payload = payload.get("payload", {})
        sess_status = sess_payload.get("status", "") if isinstance(sess_payload, dict) else ""
        sess_name = payload.get("session", "")

        # Auto-reconnect: DISABLED — causa loop STOPPED→RESTART→STOPPED
        if sess_status in ("FAILED", "STOPPED") and sess_name:
            print(f"[WEBHOOK] session.status {sess_status} para {sess_name} — atualizando banco (sem auto-restart)")
            supabase.table("gateway_instances").update({"status": "disconnected"}).eq("instance_name", sess_name).execute()

        if sess_status == "WORKING" and sess_name:
            print(f"[WEBHOOK] session.status WORKING para {sess_name} — disparando auto-sync")
            async def _auto_sync_on_connect(instance_name=sess_name):
                try:
                    # Busca o tenant dono desta instância
                    inst_row = supabase.table("gateway_instances").select("tenant_id").eq("instance_name", instance_name).limit(1).execute().data
                    if not inst_row:
                        print(f"[AUTO-SYNC] Instância {instance_name} não encontrada no banco")
                        return
                    tenant_id = inst_row[0]["tenant_id"]
                    # Atualiza status da instância para connected
                    supabase.table("gateway_instances").update({"status": "connected"}).eq("instance_name", instance_name).execute()
                    # ESTÁGIO 1: imediato — busca contatos e conversas (sem mensagens)
                    print(f"[AUTO-SYNC] Estágio 1 imediato para {instance_name}")
                    asyncio.create_task(_deep_sync_progressive(tenant_id, instance_name))
                    print(f"[AUTO-SYNC] Deep-sync enfileirado para {instance_name}")
                except Exception as e:
                    print(f"[AUTO-SYNC] Erro: {e}")
            asyncio.create_task(_auto_sync_on_connect())
        return {"ok": True}

    # Formato WAHA
    if "payload" in payload and isinstance(payload["payload"], dict):
        data = payload["payload"]
        is_from_me = data.get("fromMe", False)
        # Para mensagens enviadas (fromMe), o contato é o destinatário ("to"), não o remetente ("from")
        if is_from_me:
            raw_from = data.get("to") or data.get("chatId") or data.get("from", "")
        else:
            raw_from = data.get("from", "")
        if "broadcast" in raw_from.lower(): return {"ok": True}  # ignora broadcast

        # Detecta se é grupo
        is_group = "@g.us" in raw_from or "@g." in raw_from
        participant_phone = ""
        participant_name = ""

        if is_group:
            # Grupo: phone = ID do grupo (ex: 5511999@g.us → store as "5511999@g.us")
            phone = raw_from  # mantém @g.us para identificar grupo
            group_id = raw_from  # chatId para envio
            # Participante que enviou (campo "author" ou "participant" no WAHA)
            participant_raw = data.get("author") or data.get("participant") or ""
            participant_phone = participant_raw.replace("@c.us", "").replace("@s.whatsapp.net", "").strip()
            participant_name = data.get("notifyName") or data.get("pushName") or participant_phone
        else:
            group_id = ""
            contact_name_override = None  # may be set by LID resolver
            # NOWEB engine sends LID (@lid) instead of real phone number
            # Try to extract real phone from other fields first
            if "@lid" in raw_from:
                # Check if there's a real phone in other payload fields
                _source = data.get("_data", {}) if isinstance(data.get("_data"), dict) else {}
                _real_phone = (
                    data.get("from_phone") or
                    data.get("phoneNumber") or
                    _source.get("from", "") or
                    ""
                )
                # If we can extract digits from a non-LID field, use it
                if _real_phone and "@lid" not in _real_phone:
                    phone = _real_phone.replace("@c.us", "").replace("@s.whatsapp.net", "")
                else:
                    # Keep LID for now — will try to resolve via WAHA /contacts below
                    phone = raw_from  # e.g. "188377416622286@lid"
            else:
                phone = raw_from.replace("@c.us", "").replace("@s.whatsapp.net", "")
        if not phone: return {"ok": True}
        content = data.get("body") or data.get("text") or ""
        msg_type = data.get("type", "chat")
        media_url = data.get("mediaUrl") or data.get("media_url") or ""
        media_mimetype = data.get("mimetype") or data.get("mimeType") or ""
        media_filename = data.get("filename") or data.get("fileName") or ""
        if not content:
            if "image" in msg_type:    content = "[Imagem]"
            elif "audio" in msg_type or msg_type == "ptt":  content = "[Áudio]"
            elif "video" in msg_type:  content = "[Vídeo]"
            elif "document" in msg_type:
                content = f"[Documento: {media_filename}]" if media_filename else "[Documento]"
            elif "sticker" in msg_type: content = "[Sticker]"
            else: return {"ok": True}
        # WAHA pode mandar id como string OU como objeto {fromMe, remote, id}
        _raw_id = data.get("id", "")
        if isinstance(_raw_id, dict):
            waha_id = _raw_id.get("id") or _raw_id.get("_serialized") or ""
        else:
            waha_id = str(_raw_id) if _raw_id else ""
        # Extract push name from WAHA payload (contact's WhatsApp display name)
        push_name = data.get("notifyName") or data.get("pushName") or data.get("_data", {}).get("notifyName", "") if isinstance(data.get("_data"), dict) else data.get("notifyName") or data.get("pushName") or ""
    # Formato Evolution API legado
    else:
        push_name = ""  # Evolution API doesn't provide push name easily
        is_group = False
        participant_name = ""
        group_id = ""
        data = payload.get("data", {})
        key = data.get("key", {})
        is_from_me = key.get("fromMe", False)
        phone = key.get("remoteJid", "").replace("@s.whatsapp.net", "").replace("@g.us", "")
        if not phone: return {"ok": True}
        if "broadcast" in phone.lower(): return {"ok": True}  # ignora broadcast
        msg = data.get("message", {})
        content = msg.get("conversation") or (msg.get("extendedTextMessage") or {}).get("text") or (
            "[Imagem]" if msg.get("imageMessage") else "[Áudio]" if msg.get("audioMessage") else
            "[Documento]" if msg.get("documentMessage") else "")
        if not content: return {"ok": True}
        waha_id = ""

    # Captura session/instance_name do webhook
    instance_name = payload.get("session") or payload.get("instance") or payload.get("instanceName") or ""

    # Resolve LID to real phone number via WAHA if needed
    if "@lid" in phone and instance_name:
        lid_raw = phone
        try:
            async with httpx.AsyncClient(timeout=6) as _c:
                # Try 1: WAHA v2 path style GET /api/{session}/contacts?contactId=LID
                _r = await _c.get(f"{WAHA_URL}/api/{instance_name}/contacts",
                                  headers=waha_headers(), params={"contactId": phone})
                print(f"[LID_RESOLVE] try1 status={_r.status_code} body={_r.text[:200]}")
                _resolved = None
                if _r.status_code == 200:
                    _d = _r.json()
                    if isinstance(_d, list): _d = _d[0] if _d else {}
                    _rid = _d.get("id", {})
                    _candidate = (_d.get("number") or _d.get("phone") or
                                  (_rid.get("user","") if isinstance(_rid,dict) else str(_rid)))
                    _candidate = str(_candidate).replace("@c.us","").replace("@s.whatsapp.net","").replace("@lid","")
                    if _candidate and _candidate.replace("+","").isdigit(): _resolved = _candidate

                if not _resolved:
                    # Try 2: original style GET /api/contacts?session=X&contactId=Y
                    _r2 = await _c.get(f"{WAHA_URL}/api/contacts", headers=waha_headers(),
                                       params={"session": instance_name, "contactId": phone})
                    print(f"[LID_RESOLVE] try2 status={_r2.status_code} body={_r2.text[:200]}")
                    if _r2.status_code == 200:
                        _d2 = _r2.json()
                        if isinstance(_d2, list): _d2 = _d2[0] if _d2 else {}
                        _rid2 = _d2.get("id", {})
                        _candidate2 = (_d2.get("number") or _d2.get("phone") or
                                       (_rid2.get("user","") if isinstance(_rid2,dict) else str(_rid2)))
                        _candidate2 = str(_candidate2).replace("@c.us","").replace("@s.whatsapp.net","").replace("@lid","")
                        if _candidate2 and _candidate2.replace("+","").isdigit(): _resolved = _candidate2

                # Also capture verifiedName/pushname for contact naming
                _lid_name = None
                for _dd in [locals().get("_d2"), locals().get("_d")]:
                    if isinstance(_dd, dict):
                        _lid_name = _dd.get("verifiedName") or _dd.get("pushname") or _dd.get("name")
                        if _lid_name: break

                if _resolved:
                    print(f"[LID_RESOLVE] {lid_raw} -> {_resolved} name={_lid_name}")
                    phone = _resolved
                    if _lid_name and not contact_name_override:
                        contact_name_override = _lid_name
                else:
                    # Could not resolve LID — store ONLY digits, never @lid in DB
                    _lid_digits = lid_raw.replace("@lid","").replace("@c.us","").strip()
                    if _lid_digits.isdigit():
                        phone = _lid_digits  # Clean digits only, no @lid suffix
                        print(f"[LID_RESOLVE] unresolved, using digits: {phone} name={_lid_name}")
                        if _lid_name and not contact_name_override:
                            contact_name_override = _lid_name
                    else:
                        print(f"[LID_RESOLVE] could not resolve {lid_raw}, skipping")
        except Exception as _e:
            print(f"[LID_RESOLVE] failed: {_e}")

    try:
        # Tenant isolation: find tenant via gateway_instance, not loop-all
        tid = None
        if instance_name:
            _rows = supabase.table("gateway_instances").select("tenant_id").eq("instance_name", instance_name).limit(1).execute().data
            inst_row = _rows[0] if _rows else None
            if inst_row:
                tid = inst_row["tenant_id"]
        if not tid:
            if instance_name:
                # Instance name present but not found in gateway_instances — unknown instance
                print(f"[WEBHOOK] UNKNOWN INSTANCE: '{instance_name}' not found in gateway_instances, dropping message from {phone}")
                return {"ok": True}
            # No instance_name at all — legacy fallback (iterate all tenants)
            print(f"[WEBHOOK] WARNING: no instance_name in payload, using legacy tenant fallback for {phone}")
            tenants = supabase.table("tenants").select("id").execute().data
        else:
            tenants = [{"id": tid}]
        for tenant in tenants:
            tid = tenant["id"]
            contacts = supabase.table("contacts").select("id").eq("tenant_id", tid).eq("phone", phone).execute().data
            if contacts:
                contact_id = contacts[0]["id"]
                # Update name if still set to the raw phone (never got a real name)
                existing_name = contacts[0].get("name", "")
                if existing_name == phone or existing_name == "" or (existing_name and "@" in existing_name):
                    if contact_name_override:
                        supabase.table("contacts").update({"name": contact_name_override}).eq("id", contact_id).execute()
                    async def _update_name(contact_id=contact_id, phone=phone, instance_name=instance_name):
                        try:
                            if not instance_name: return
                            clean = "".join(c for c in phone if c.isdigit())
                            chat_id = f"{clean}@c.us"
                            async with httpx.AsyncClient(timeout=5) as _c:
                                _r = await _c.get(f"{WAHA_URL}/api/contacts", headers=waha_headers(),
                                                  params={"session": instance_name, "contactId": chat_id})
                                if _r.status_code == 200:
                                    d = _r.json()
                                    if isinstance(d, list): d = d[0] if d else {}
                                    name = d.get("pushname") or d.get("name") or ""
                                    if name and not name.replace("+","").replace(" ","").replace("-","").isdigit():
                                        supabase.table("contacts").update({"name": name}).eq("id", contact_id).execute()
                        except: pass
                    asyncio.create_task(_update_name())
            else:
                # Create contact with phone as placeholder name
                # Use pushName from payload if available, otherwise fall back to phone
                initial_name = (contact_name_override or push_name) if ((contact_name_override or push_name) and not (contact_name_override or push_name).replace("+","").replace(" ","").replace("-","").isdigit()) else phone
                try:
                    new_contact = supabase.table("contacts").insert({"tenant_id": tid, "phone": phone, "name": initial_name}).execute().data[0]
                except Exception as _dup:
                    if "duplicate" in str(_dup).lower() or "23505" in str(_dup):
                        new_contact = supabase.table("contacts").select("*").eq("tenant_id", tid).eq("phone", phone).single().execute().data
                    else:
                        raise
                contact_id = new_contact["id"]
                # Also update name via WAHA /contacts if pushName is still just the phone
                if initial_name == phone:  # no push name from payload, try WAHA
                    pass  # _fetch_name will handle it below
                # Try to get real name from WAHA Plus /contacts endpoint (fast, accurate)
                async def _fetch_name(tid=tid, phone=phone, contact_id=contact_id, instance_name=instance_name):
                    try:
                        if not instance_name: return
                        # Normalize phone to chatId format
                        clean = "".join(c for c in phone if c.isdigit())
                        chat_id = f"{clean}@c.us"
                        async with httpx.AsyncClient(timeout=6) as _c:
                            # WAHA Plus: GET /api/contacts?session=X&contactId=Y
                            _r = await _c.get(
                                f"{WAHA_URL}/api/contacts",
                                headers=waha_headers(),
                                params={"session": instance_name, "contactId": chat_id}
                            )
                            if _r.status_code == 200:
                                data = _r.json()
                                # Response can be a single object or list
                                if isinstance(data, list): data = data[0] if data else {}
                                name = data.get("pushname") or data.get("name") or data.get("verifiedName") or ""
                                if name and not name.replace("+","").replace(" ","").replace("-","").isdigit():
                                    supabase.table("contacts").update({"name": name}).eq("id", contact_id).execute()
                                    return
                        # Fallback: try push_name from the webhook payload itself (passed via closure)
                    except: pass
                asyncio.create_task(_fetch_name())
            # Get most recent open conversation for this contact+instance (avoid cross-instance contamination)
            conv_query = supabase.table("conversations").select("id,unread_count").eq("contact_id", contact_id).neq("status", "resolved").order("created_at", desc=True)
            if instance_name:
                conv_query = conv_query.eq("instance_name", instance_name)
            convs = conv_query.limit(1).execute().data
            if convs:
                conv_id = convs[0]["id"]; uc = (convs[0].get("unread_count") or 0) + (0 if is_from_me else 1)
            else:
                insert_data = {"contact_id": contact_id, "tenant_id": tid, "status": "open", "kanban_stage": "new", "unread_count": 0}
                if instance_name:
                    insert_data["instance_name"] = instance_name
                if is_group:
                    insert_data["is_group"] = True
                try:
                    conv = supabase.table("conversations").insert(insert_data).execute().data[0]
                except Exception:
                    conv = supabase.table("conversations").insert({"contact_id": contact_id, "tenant_id": tid, "status": "open"}).execute().data[0]
                conv_id = conv["id"]; uc = 1
            # Also fix existing conversations that have wrong/null instance_name
            if instance_name and convs:
                existing_inst = supabase.table("conversations").select("instance_name").eq("id", conv_id).single().execute().data
                if existing_inst and not existing_inst.get("instance_name"):
                    supabase.table("conversations").update({"instance_name": instance_name}).eq("id", conv_id).execute()

            # Evita duplicata por waha_id — verificacao rapida no banco
            if waha_id:
                try:
                    dup = supabase.table("messages").select("id").eq("waha_id", waha_id).limit(1).execute().data
                    if dup:
                        print(f"[WEBHOOK] DUPLICATE skipped waha_id={waha_id}")
                        continue
                except Exception:
                    pass

            # Salva mensagem
            try:
                # Determine stored type
                stored_type = "text"
                if "image" in msg_type:    stored_type = "image"
                elif "audio" in msg_type or msg_type == "ptt":  stored_type = "audio"
                elif "video" in msg_type:  stored_type = "video"
                elif "document" in msg_type: stored_type = "document"
                msg_insert = {
                    "conversation_id": conv_id,
                    "tenant_id": tid,
                    "direction": "outbound" if is_from_me else "inbound",
                    "content": content,
                    "type": stored_type,
                    "waha_id": waha_id or None
                }
                if media_url:
                    msg_insert["media_url"] = media_url
                if media_mimetype:
                    msg_insert["media_mimetype"] = media_mimetype
                if media_filename:
                    msg_insert["media_filename"] = media_filename
                if is_group and participant_name:
                    msg_insert["participant_name"] = participant_name
                supabase.table("messages").insert(msg_insert).execute()
                # ── Auto-transcrição de áudio (background) ──────────────
                if stored_type == "audio" and media_url and (OPENAI_API_KEY or ANTHROPIC_API_KEY):
                    _saved_id = supabase.table("messages").select("id").eq("conversation_id", conv_id).eq("waha_id", waha_id).maybe_single().execute()
                    _msg_id = (_saved_id.data or {}).get("id") if _saved_id and _saved_id.data else None
                    if _msg_id:
                        async def _transcribe_audio(msg_id=_msg_id, url=media_url):
                            try:
                                async with httpx.AsyncClient(timeout=30) as _c:
                                    ar = await _c.get(url, headers=waha_headers())
                                    if ar.status_code != 200: return
                                    import io
                                    audio_bytes = io.BytesIO(ar.content)
                                    transcript_text = ""
                                    if OPENAI_API_KEY:
                                        import openai as _oai
                                        _oai_client = _oai.AsyncOpenAI(api_key=OPENAI_API_KEY)
                                        t = await _oai_client.audio.transcriptions.create(
                                            model="whisper-1",
                                            file=("audio.ogg", audio_bytes, "audio/ogg"),
                                            language="pt"
                                        )
                                        transcript_text = t.text.strip()
                                    elif ANTHROPIC_API_KEY:
                                        # Fallback: use Claude to transcribe via base64
                                        import base64 as _b64
                                        audio_b64 = _b64.b64encode(ar.content).decode()
                                        headers_a = {"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"}
                                        payload_a = {"model": "claude-haiku-4-5-20251001", "max_tokens": 500,
                                            "messages": [{"role": "user", "content": [
                                                {"type": "text", "text": "Transcreva exatamente o que está sendo dito neste áudio em português. Responda APENAS com a transcrição, sem explicações."},
                                                {"type": "document", "source": {"type": "base64", "media_type": "audio/ogg", "data": audio_b64}}
                                            ]}]}
                                        async with httpx.AsyncClient(timeout=30) as _ac:
                                            _ar = await _ac.post("https://api.anthropic.com/v1/messages", json=payload_a, headers=headers_a)
                                            _ad = _ar.json()
                                            transcript_text = (_ad.get("content") or [{}])[0].get("text", "").strip()
                                    if transcript_text:
                                        supabase.table("messages").update({"ai_suggestion": f"🎤 {transcript_text}"}).eq("id", msg_id).execute()
                                        print(f"[TRANSCRIPT] msg={msg_id[:8]}: {transcript_text[:60]}")
                            except Exception as _te:
                                print(f"[TRANSCRIPT] erro: {_te}")
                        loop = asyncio.get_event_loop()
                        loop.create_task(_transcribe_audio())
            except Exception as insert_err:
                err_str = str(insert_err)
                if "duplicate" in err_str.lower() or "unique" in err_str.lower():
                    print(f"[WEBHOOK] DUPLICATE blocked by DB constraint waha_id={waha_id}")
                    continue  # unique constraint pegou — ignora silenciosamente
                # Fallback: insert sem waha_id caso coluna nao exista
                supabase.table("messages").insert({
                    "conversation_id": conv_id,
                    "tenant_id": tid,
                    "direction": "outbound" if is_from_me else "inbound",
                    "content": content,
                    "type": "text"
                }).execute()

            _preview = (content[:80] if content else "")
            supabase.table("conversations").update({"last_message_at": datetime.utcnow().isoformat(), "unread_count": uc, "last_message_preview": _preview}).eq("id", conv_id).execute()
            # Lazy-sync profile picture if not yet fetched
            try:
                _contact_pic = supabase.table("contacts").select("profile_picture_url,last_sync_at").eq("id", contact_id).single().execute().data
                _needs_pic = not _contact_pic or not _contact_pic.get("profile_picture_url")
                _stale = _contact_pic and _contact_pic.get("last_sync_at") and (datetime.utcnow() - datetime.fromisoformat(_contact_pic["last_sync_at"].replace("Z",""))).days > 7
                if (_needs_pic or _stale) and instance_name:
                    async def _sync_pic(ph=phone, inst=instance_name, cid=contact_id):
                        try:
                            clean = "".join(c for c in ph if c.isdigit())
                            chat_id = f"{clean}@c.us"
                            async with httpx.AsyncClient(timeout=6) as _c:
                                _r = await _c.get(f"{WAHA_URL}/api/contacts/profile-picture",
                                    headers=waha_headers(), params={"session": inst, "contactId": chat_id})
                                if _r.status_code == 200:
                                    d = _r.json()
                                    url = d.get("eurl") or d.get("url") or d.get("profilePictureUrl") or d.get("profilePictureURL")
                                    if url:
                                        supabase.table("contacts").update({"profile_picture_url": url, "last_sync_at": datetime.utcnow().isoformat()}).eq("id", cid).execute()
                        except: pass
                    asyncio.create_task(_sync_pic())
            except: pass
            # Invalida cache para forçar reload no frontend
            cache_del(f"msgs:{conv_id}:latest")  # Força reload imediato no próximo poll (3s)
            # Bust ALL conversation cache keys for this tenant (including per-user keys)
            # Pattern: convs:{tid}:*
            try:
                r = _get_redis()
                if r:
                    for k in r.scan_iter(f"7crm:convs:{tid}:*"):
                        r.delete(k)
            except: pass
            # Also bust in-memory cache for all known variants
            keys_to_del = [k for k in list(_cache.keys()) if k.startswith(f"convs:{tid}:")]
            for k in keys_to_del:
                _cache.pop(k, None)

            # ── AUTO-PILOT: debounce — acumula mensagens na janela antes de responder ──
            if not is_from_me and conv_id and tid:
                try:
                    loop = asyncio.get_event_loop()
                    loop.create_task(_autopilot_debounce_trigger_async(conv_id, tid, instance_name))
                except Exception as ae:
                    print(f"[AUTOPILOT] Erro ao criar task: {ae}")

        return {"ok": True}
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"WEBHOOK ERROR: {e}\n{tb}")
        return {"ok": False, "error": str(e), "traceback": tb}

@app.get("/health/waha", dependencies=[Depends(verify_key)])
async def waha_health_check(tenant_id: str):
    """Verifica status das sessões WAHA e se os webhooks estão apontando para este backend."""
    results = []
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{WAHA_URL}/api/sessions", headers=waha_headers())
            sessions = r.json() if r.status_code == 200 else []
        prefix = tenant_id[:6].replace("-", "")
        for s in sessions:
            name = s.get("name", "")
            if not name.startswith(f"t{prefix}"): continue
            status = s.get("status", "UNKNOWN")
            webhooks = s.get("config", {}).get("webhooks", [])
            webhook_ok = False; webhook_url = None; headers_ok = False
            for wh in webhooks:
                url = wh.get("url", ""); webhook_url = url
                backend_host = BACKEND_URL.replace("https://", "").replace("http://", "").split("/")[0]
                if backend_host in url and ("/webhook/inbox" in url or "/webhook/message" in url):
                    webhook_ok = True
                    headers_ok = any(
                        h.get("name", "").lower() == "x-api-key" and h.get("value") == WEBHOOK_SECRET
                        for h in wh.get("customHeaders", []))
                    break
            results.append({
                "session": name, "whatsapp_status": status, "status_ok": status == "WORKING",
                "webhook_url": webhook_url, "webhook_points_to_backend": webhook_ok,
                "webhook_auth_ok": headers_ok, "all_ok": status == "WORKING" and webhook_ok and headers_ok,
                "me": (s.get("me") or {}).get("id", ""), "engine": (s.get("engine") or {}).get("engine", "")
            })
    except Exception as e:
        return {"ok": False, "error": str(e), "sessions": []}
    all_healthy = all(s["all_ok"] for s in results) if results else False
    return {"ok": all_healthy, "sessions": results, "backend_url": BACKEND_URL}


@app.get("/health/diagnostics", dependencies=[Depends(verify_key)])
async def full_diagnostics(tenant_id: str):
    """Diagnóstico completo: WAHA → Webhook → Backend → Banco de dados."""
    now = datetime.utcnow()
    diag = {
        "timestamp": now.isoformat(),
        "backend": {"ok": True, "url": BACKEND_URL},
        "waha": {"ok": False, "url": WAHA_URL, "version": None, "sessions": []},
        "webhook": {"ok": False, "endpoint": f"{BACKEND_URL}/webhook/inbox", "auth_configured": bool(WEBHOOK_SECRET)},
        "database": {"ok": False, "recent_messages": 0, "last_message_at": None, "total_conversations": 0},
        "flow": {"steps": [], "all_ok": False}
    }

    # ── 1. BACKEND ──
    diag["flow"]["steps"].append({"step": "Backend online", "ok": True, "detail": BACKEND_URL})

    # ── 2. WAHA ──
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            vr = await client.get(f"{WAHA_URL}/api/version", headers=waha_headers())
            sr = await client.get(f"{WAHA_URL}/api/sessions", headers=waha_headers())
        version_data = vr.json() if vr.status_code == 200 else {}
        sessions_raw = sr.json() if sr.status_code == 200 else []
        diag["waha"]["version"] = version_data.get("version")
        diag["waha"]["engine_default"] = version_data.get("engine")
        diag["waha"]["ok"] = sr.status_code == 200

        prefix = tenant_id[:6].replace("-", "")
        backend_host = BACKEND_URL.replace("https://", "").replace("http://", "").split("/")[0]
        waha_ok_count = 0

        for s in sessions_raw:
            name = s.get("name", "")
            if not name.startswith(f"t{prefix}"): continue
            status = s.get("status", "UNKNOWN")
            webhooks = s.get("config", {}).get("webhooks", [])
            wh_url = None; wh_ok = False; auth_ok = False; correct_endpoint = False
            for wh in webhooks:
                wh_url = wh.get("url", "")
                if backend_host in wh_url:
                    wh_ok = True
                    correct_endpoint = "/webhook/inbox" in wh_url or "/webhook/message" in wh_url
                    auth_ok = any(
                        h.get("name","").lower()=="x-api-key" and h.get("value")==WEBHOOK_SECRET
                        for h in wh.get("customHeaders",[]))
                    break
            session_ok = status == "WORKING" and wh_ok and auth_ok
            if session_ok: waha_ok_count += 1
            # Last message from DB for this session
            last_msg = None
            try:
                rows = supabase.table("conversations").select("last_message_at").eq("tenant_id", tenant_id).eq("instance_name", name).order("last_message_at", desc=True).limit(1).execute().data
                last_msg = rows[0]["last_message_at"] if rows else None
            except: pass
            diag["waha"]["sessions"].append({
                "name": name,
                "phone": (s.get("me") or {}).get("id","").replace("@c.us",""),
                "push_name": (s.get("me") or {}).get("pushName",""),
                "status": status,
                "status_ok": status == "WORKING",
                "engine": (s.get("engine") or {}).get("engine",""),
                "webhook_url": wh_url,
                "webhook_points_to_backend": wh_ok,
                "webhook_correct_endpoint": correct_endpoint,
                "webhook_auth_ok": auth_ok,
                "session_ok": session_ok,
                "last_db_message_at": last_msg
            })
        diag["flow"]["steps"].append({
            "step": "WAHA acessível",
            "ok": diag["waha"]["ok"],
            "detail": f"v{diag['waha']['version']} | {len(diag['waha']['sessions'])} sessão(ões) do tenant"
        })
        waha_sessions_ok = waha_ok_count == len(diag["waha"]["sessions"]) and len(diag["waha"]["sessions"]) > 0
        diag["flow"]["steps"].append({
            "step": "Sessões WhatsApp conectadas",
            "ok": waha_sessions_ok,
            "detail": f"{waha_ok_count}/{len(diag['waha']['sessions'])} sessões OK (WORKING + webhook configurado)"
        })
    except Exception as e:
        diag["waha"]["error"] = str(e)
        diag["flow"]["steps"].append({"step": "WAHA acessível", "ok": False, "detail": str(e)})

    # ── 3. WEBHOOK CONFIG ──
    webhook_sessions_ok = all(
        s["webhook_points_to_backend"] and s["webhook_auth_ok"]
        for s in diag["waha"]["sessions"]
    ) if diag["waha"]["sessions"] else False
    diag["webhook"]["ok"] = webhook_sessions_ok
    diag["flow"]["steps"].append({
        "step": "Webhooks apontando para o backend",
        "ok": webhook_sessions_ok,
        "detail": f"Endpoint esperado: {BACKEND_URL}/webhook/inbox"
    })

    # ── 4. DATABASE ──
    try:
        cutoff_5min = (now - timedelta(minutes=5)).isoformat()
        cutoff_1h   = (now - timedelta(hours=1)).isoformat()
        cutoff_24h  = (now - timedelta(hours=24)).isoformat()

        recent = supabase.table("messages").select("id,created_at,direction").eq("tenant_id", tenant_id).gte("created_at", cutoff_1h).order("created_at", desc=True).limit(50).execute().data
        total_convs = supabase.table("conversations").select("id", count="exact").eq("tenant_id", tenant_id).execute()
        last_msg_row = supabase.table("messages").select("created_at,direction,content").eq("tenant_id", tenant_id).order("created_at", desc=True).limit(1).execute().data

        msgs_5m  = len([m for m in recent if m["created_at"] >= cutoff_5min])
        msgs_1h  = len(recent)
        msgs_24h_r = supabase.table("messages").select("id", count="exact").eq("tenant_id", tenant_id).gte("created_at", cutoff_24h).execute()
        msgs_24h = msgs_24h_r.count if hasattr(msgs_24h_r, "count") else 0

        last_msg_at = last_msg_row[0]["created_at"] if last_msg_row else None
        last_msg_ago_s = None
        if last_msg_at:
            try:
                # Handle both 'Z' and '+00:00' formats from Supabase
                ts_str = last_msg_at.replace("Z", "+00:00")
                from datetime import timezone
                dt = datetime.fromisoformat(ts_str)
                now_utc = datetime.now(timezone.utc)
                last_msg_ago_s = int((now_utc - dt).total_seconds())
            except Exception:
                last_msg_ago_s = None

        diag["database"] = {
            "ok": True,
            "recent_messages_5min": msgs_5m,
            "recent_messages_1h": msgs_1h,
            "recent_messages_24h": msgs_24h,
            "total_conversations": getattr(total_convs, "count", 0),
            "last_message_at": last_msg_at,
            "last_message_ago_seconds": last_msg_ago_s,
            "last_message_direction": last_msg_row[0]["direction"] if last_msg_row else None,
        }
        db_receiving = msgs_5m > 0 or (last_msg_ago_s is not None and last_msg_ago_s < 300)
        diag["flow"]["steps"].append({
            "step": "Mensagens gravadas no banco",
            "ok": db_receiving,
            "detail": f"{msgs_5m} msg nos últimos 5min | {msgs_1h} na última hora | última há {last_msg_ago_s}s" if last_msg_ago_s else "Nenhuma mensagem recente encontrada"
        })
    except Exception as e:
        diag["database"]["error"] = str(e)
        diag["flow"]["steps"].append({"step": "Banco de dados", "ok": False, "detail": str(e)})

    # ── 5. OVERALL ──
    diag["flow"]["all_ok"] = all(s["ok"] for s in diag["flow"]["steps"])
    return diag


# ── CONVERSATIONS ────────────────────────────────────────

@app.post("/debug/webhook-test")
async def debug_webhook_test(payload: dict, x_api_key: str = Header(default="")):
    """Debug: same as webhook but returns detailed error info"""
    import traceback as tb_module
    try:
        event = payload.get("event", "")
        if "payload" in payload and isinstance(payload["payload"], dict):
            data = payload["payload"]
            is_from_me = data.get("fromMe", False)
            if is_from_me:
                raw_from = data.get("to") or data.get("chatId") or data.get("from", "")
            else:
                raw_from = data.get("from", "")
            if "@g" in raw_from: return {"ok": True, "skipped": "group"}
            if "@lid" in raw_from:
                phone = raw_from.replace("@lid","").replace("@c.us","").strip()
                phone = "".join(c for c in phone if c.isdigit())  # digits only
            else:
                phone = raw_from.replace("@c.us", "").replace("@s.whatsapp.net", "")
            if not phone: return {"ok": False, "reason": "no phone"}
            content = data.get("body") or data.get("text") or ""
            if not content: return {"ok": False, "reason": "no content"}
        else:
            return {"ok": False, "reason": "invalid payload format"}
        
        instance_name = payload.get("session") or ""
        
        # Test tenant lookup
        tid = None
        if instance_name:
            _rows = supabase.table("gateway_instances").select("tenant_id").eq("instance_name", instance_name).limit(1).execute().data
            inst_row = _rows[0] if _rows else None
            if inst_row:
                tid = inst_row["tenant_id"]
        
        if not tid:
            return {"ok": False, "reason": "tenant not found for instance", "instance_name": instance_name}
        
        # Test contact lookup
        contacts = supabase.table("contacts").select("id").eq("tenant_id", tid).eq("phone", phone).execute().data
        contact_id = contacts[0]["id"] if contacts else None
        
        if not contact_id:
            # Try insert
            try:
                new_contact = supabase.table("contacts").insert({"tenant_id": tid, "phone": phone, "name": phone}).execute().data
            except Exception as _dup:
                if "duplicate" in str(_dup).lower() or "23505" in str(_dup):
                    new_contact = supabase.table("contacts").select("*").eq("tenant_id", tid).eq("phone", phone).execute().data
                else:
                    raise
            if new_contact:
                contact_id = new_contact[0]["id"]
            else:
                return {"ok": False, "reason": "failed to create contact"}
        
        # Test conv lookup
        convs = supabase.table("conversations").select("id,unread_count").eq("contact_id", contact_id).neq("status", "resolved").order("created_at", desc=True).limit(1).execute().data
        
        # Test message insert
        if convs:
            conv_id = convs[0]["id"]
            msg = supabase.table("messages").insert({"conversation_id": conv_id, "tenant_id": tid, "direction": "inbound", "content": content, "type": "text"}).execute().data
            supabase.table("conversations").update({"last_message_at": datetime.utcnow().isoformat(), "unread_count": (convs[0].get("unread_count") or 0) + 1}).eq("id", conv_id).execute()
            return {"ok": True, "action": "updated_existing", "conv_id": conv_id, "tid": tid, "phone": phone, "msg_id": msg[0]["id"] if msg else None}
        else:
            insert_data = {"contact_id": contact_id, "tenant_id": tid, "status": "open", "kanban_stage": "new", "unread_count": 1}
            if instance_name:
                insert_data["instance_name"] = instance_name
            conv = supabase.table("conversations").insert(insert_data).execute().data
            if not conv:
                return {"ok": False, "reason": "failed to create conversation"}
            conv_id = conv[0]["id"]
            msg = supabase.table("messages").insert({"conversation_id": conv_id, "tenant_id": tid, "direction": "inbound", "content": content, "type": "text"}).execute().data
            return {"ok": True, "action": "created_new_conv", "conv_id": conv_id, "tid": tid, "phone": phone}
    except Exception as e:
        return {"ok": False, "error": str(e), "traceback": tb_module.format_exc()}

@app.get("/conversations")
async def list_conversations(tenant_id: str = Depends(enforce_tenant), status: Optional[str] = None, user_id: Optional[str] = None, before: Optional[str] = None, limit: int = 50, instance_name: Optional[str] = None, is_group: Optional[bool] = None):
    """Lista conversas com paginação por cursor (before = last_message_at do último item)."""
    limit = min(limit, 100)
    cache_key = f"convs:{tenant_id}:{status}:{user_id}:{instance_name or 'all'}:{before or 'first'}:{is_group}"

    # Só usa cache na primeira página (sem cursor)
    if not before:
        stale = cache_get(cache_key, stale_ok=True)
        if stale and not cache_is_stale(cache_key):
            return {"conversations": stale, "has_more": len(stale) == limit}
        if stale:
            asyncio.create_task(_refresh_conversations(tenant_id, status, user_id))
            return {"conversations": stale, "has_more": len(stale) == limit}

    def _query():
        allowed = None
        if user_id:
            try:
                _ur = supabase.table("users").select("allowed_instances,role").eq("id", user_id).limit(1).execute().data
                u = _ur[0] if _ur else None
                if u and u.get("role") != "admin" and u.get("allowed_instances"):
                    allowed = u["allowed_instances"]
            except Exception:
                pass  # allowed_instances column may not exist yet — skip filter

        q = supabase.table("conversations").select("*, contacts(id,name,phone,tags,profile_picture_url), users!assigned_to(id,name,avatar_color)").eq("tenant_id", tenant_id)
        if status and status != "all": q = q.eq("status", status)
        if before: q = q.lt("last_message_at", before)
        if instance_name: q = q.eq("instance_name", instance_name)
        if is_group is True: q = q.eq("is_group", True)
        elif is_group is False: q = q.or_("is_group.is.null,is_group.eq.false")  # normais: null ou false
        convs = q.order("last_message_at", desc=True).limit(limit).execute().data
        if not convs:
            return convs

        if allowed:
            inst_rows = supabase.table("gateway_instances").select("id,instance_name").in_("id", allowed).execute().data
            allowed_names = {r["instance_name"] for r in inst_rows}
            # Include convs with null instance_name — legacy rows before multi-instance tracking
            convs = [c for c in convs if not c.get("instance_name") or c.get("instance_name") in allowed_names]

        # Labels: lê direto do array conversations.labels[] + busca detalhes da tabela labels
        conv_ids = [c["id"] for c in convs]
        # Coletar todos os label_ids únicos usados nas convs
        all_label_ids = set()
        for c in convs:
            for lid in (c.get("labels") or []):
                all_label_ids.add(lid)
        # Buscar detalhes dos labels (nome, cor)
        label_details = {}
        if all_label_ids:
            lb_rows = supabase.table("labels").select("id,name,color").eq("tenant_id", tenant_id).execute().data
            label_details = {lb["id"]: lb for lb in lb_rows}
        # Montar labels enriquecidos para cada conversa
        for c in convs:
            raw_ids = c.get("labels") or []
            c["labels"] = [label_details[lid] for lid in raw_ids if lid in label_details]
        return convs

    convs = await run_sync(_query)
    if not before:
        cache_set(cache_key, convs, ttl=15)  # TTL curto — webhook invalida ativamente
    return {"conversations": convs, "has_more": len(convs) == limit}

async def _refresh_conversations(tenant_id, status, user_id):
    """Background task: silently refresh first page cache."""
    try:
        await list_conversations(tenant_id, status, user_id)
    except Exception:
        pass


@app.delete("/conversations/{conv_id}", dependencies=[Depends(verify_key)])
async def delete_conversation(conv_id: str):
    """Delete a conversation and all its messages/tasks."""
    try:
        supabase.table("tasks").delete().eq("conversation_id", conv_id).execute()
    except: pass
    try:
        supabase.table("messages").delete().eq("conversation_id", conv_id).execute()
    except: pass
    supabase.table("conversations").delete().eq("id", conv_id).execute()
    return {"ok": True}

@app.get("/conversations/{conv_id}/messages")
async def get_messages(conv_id: str, before: str = None, limit: int = 10, user: dict = Depends(get_current_user)):
    # Validate: conversation must belong to the user's tenant
    conv_check = supabase.table("conversations").select("tenant_id").eq("id", conv_id).maybe_single().execute()
    if not conv_check.data or conv_check.data.get("tenant_id") != user["tenant_id"]:
        raise HTTPException(status_code=403, detail="Acesso negado")
    limit = min(limit, 50)
    cache_key = f"msgs:{conv_id}:{before or 'latest'}"
    cached = cache_get(cache_key)
    if cached:
        asyncio.create_task(run_sync(lambda: supabase.table("conversations").update({"unread_count": 0}).eq("id", conv_id).execute()))
        return {"messages": cached, "has_more": len(cached) >= limit}
    q = supabase.table("messages").select("*").eq("conversation_id", conv_id)
    if before:
        q = q.lt("created_at", before)
    def _fetch():
        try:
            data = q.order("created_at", desc=True).limit(limit).execute().data
            try: supabase.table("conversations").update({"unread_count": 0}).eq("id", conv_id).execute()
            except: pass
            return list(reversed(data))
        except Exception as e:
            print(f"[MESSAGES] DB error for {conv_id}: {e}")
            return []
    try:
        msgs = await run_sync(_fetch)
        cache_set(cache_key, msgs, ttl=30)
        return {"messages": msgs, "has_more": len(msgs) == limit}
    except Exception as e:
        print(f"[MESSAGES] Unexpected error for {conv_id}: {e}")
        return {"messages": [], "has_more": False}


@app.get("/users/me/preferences", dependencies=[Depends(verify_key)])
async def get_user_preferences(user_id: str):
    def _get():
        _up = supabase.table("users").select("preferences").eq("id", user_id).limit(1).execute().data
        u = _up[0] if _up else None
        return u.get("preferences") or {} if u else {}
    prefs = await run_sync(_get)
    return {"preferences": prefs}

@app.put("/users/me/preferences", dependencies=[Depends(verify_key)])
async def save_user_preferences(user_id: str, body: dict = Body(...)):
    def _save():
        _up = supabase.table("users").select("preferences").eq("id", user_id).limit(1).execute().data
        u = _up[0] if _up else None
        current = (u.get("preferences") or {}) if u else {}
        current.update(body)
        supabase.table("users").update({"preferences": current}).eq("id", user_id).execute()
        return current
    prefs = await run_sync(_save)
    return {"preferences": prefs}


@app.post("/contacts/sync-names", dependencies=[Depends(verify_key)])
async def sync_contact_names(tenant_id: str):
    """Busca nomes reais dos contatos via WAHA Plus /chats e atualiza no banco. Roda em background."""
    async def _run():
        updated = 0
        try:
            instances = supabase.table("gateway_instances").select("instance_name").eq("tenant_id", tenant_id).execute().data
            # Only update contacts whose name is still the raw phone
            all_contacts = supabase.table("contacts").select("id,phone,name").eq("tenant_id", tenant_id).execute().data or []
            needs_update = {c["phone"]: c for c in all_contacts
                           if not c.get("name") or c["name"] == c["phone"]
                           or "@" in (c.get("name") or "")
                           or (c.get("name") or "").replace("+","").replace(" ","").replace("-","").replace("(","").replace(")","").isdigit()}
            print(f"[SYNC_NAMES] {len(needs_update)} contacts need name update for tenant {tenant_id}")
            for inst in instances:
                iname = inst.get("instance_name")
                if not iname: continue
                try:
                    async with httpx.AsyncClient(timeout=20) as _hc:
                        # WAHA Plus: GET /api/{session}/chats returns name for each conversation
                        _hr = await _hc.get(f"{WAHA_URL}/api/{iname}/chats", headers=waha_headers())
                        chats = _hr.json() if _hr.status_code == 200 else []
                    # Build lookup map: various phone formats → display name
                    name_map = {}
                    for chat in (chats if isinstance(chats, list) else []):
                        cid = chat.get("id","")
                        if isinstance(cid, dict): cid = cid.get("_serialized","") or cid.get("id","")
                        name = chat.get("name","") or chat.get("pushname","")
                        if not cid or not name: continue
                        if name.replace("+","").replace(" ","").replace("-","").isdigit(): continue
                        clean_num = "".join(c for c in cid if c.isdigit())
                        name_map[cid] = name
                        name_map[clean_num] = name
                        if not cid.endswith("@c.us"): name_map[f"{clean_num}@c.us"] = name
                    # Update each contact that needs it
                    for phone, contact in needs_update.items():
                        clean = "".join(c for c in phone if c.isdigit())
                        name = name_map.get(phone) or name_map.get(clean) or name_map.get(f"{clean}@c.us")
                        if name:
                            supabase.table("contacts").update({"name": name}).eq("id", contact["id"]).execute()
                            updated += 1
                except Exception as e:
                    print(f"sync_contact_names error for {iname}: {e}")
        except Exception as e:
            print(f"sync_contact_names error: {e}")
        print(f"[SYNC_NAMES] Done — {updated} names updated")
    asyncio.create_task(_run())
    return {"ok": True, "message": "Sincronização de nomes iniciada em background"}


@app.get("/conversations/{conv_id}/history", dependencies=[Depends(verify_key)])
async def get_history(conv_id: str, limit: int = 50):
    """Serve mensagens do DB. Se DB vazio, faz sync WAHA síncrono antes de responder."""
    def _fetch_db():
        db_msgs = supabase.table("messages").select("*").eq("conversation_id", conv_id).order("created_at", desc=True).limit(limit).execute().data or []
        db_msgs = list(reversed(db_msgs))
        try: supabase.table("conversations").update({"unread_count": 0}).eq("id", conv_id).execute()
        except: pass
        return db_msgs

    msgs = await run_sync(_fetch_db)

    if not msgs:
        # DB vazio — faz sync síncrono do WAHA antes de responder
        await _waha_sync_chat_bg(conv_id, limit)
        msgs = await run_sync(_fetch_db)
    else:
        # DB tem mensagens — sync em background
        asyncio.create_task(_waha_sync_chat_bg(conv_id, limit))

    return {"messages": msgs, "has_more": len(msgs) == limit}

async def _waha_sync_chat_bg(conv_id: str, limit: int = 50):
    """Background task: busca mensagens do WAHA e salva no DB sem bloquear o cliente."""
    try:
        def _get_conv():
            _cr = supabase.table("conversations").select("*, contacts(phone)").eq("id", conv_id).limit(1).execute().data
            return _cr[0] if _cr else None
        conv = await run_sync(_get_conv)
        if not conv: return
        phone = (conv.get("contacts") or {}).get("phone", "")
        instance_name = conv.get("instance_name", "")
        if not phone or not instance_name: return
        if "@" in phone:
            chat_id = phone
        else:
            clean = "".join(c for c in phone if c.isdigit())
            chat_id = f"{clean}@c.us"
        async with httpx.AsyncClient(timeout=15) as client:
            raw = None
            for url in [
                f"{WAHA_URL}/api/{instance_name}/chats/{chat_id}/messages?limit={limit}&downloadMedia=false",
                f"{WAHA_URL}/api/messages?session={instance_name}&chatId={chat_id}&limit={limit}&downloadMedia=false",
                f"{WAHA_URL}/api/chats/{chat_id}/messages?session={instance_name}&limit={limit}",
            ]:
                try:
                    r = await client.get(url, headers=waha_headers())
                    if r.status_code == 400:
                        break  # NOWEB store not enabled — skip silently, don't crash session
                    if r.status_code == 200:
                        d = r.json()
                        raw = d if isinstance(d, list) else d.get("messages", d.get("data", []))
                        if raw: break
                except: continue
            if not raw: return
        def _get_existing():
            return {m["waha_id"] for m in supabase.table("messages").select("waha_id").eq("conversation_id", conv_id).execute().data or [] if m.get("waha_id")}
        existing_ids = await run_sync(_get_existing)
        to_insert = []
        for m in raw:
            waha_id = m.get("id", "")
            if isinstance(waha_id, dict): waha_id = waha_id.get("id", waha_id.get("_serialized", ""))
            if waha_id and waha_id in existing_ids: continue
            ts = m.get("timestamp", 0)
            from_me = m.get("fromMe", False)
            body = m.get("body") or m.get("text") or ""
            if not body:
                t = m.get("type", "")
                body = "[Imagem]" if "image" in t else "[Áudio]" if "audio" in t or t=="ptt" else "[Vídeo]" if "video" in t else "[Documento]" if "document" in t else ""
            if not body: continue
            row = {"conversation_id": conv_id, "tenant_id": conv.get("tenant_id"), "direction": "outbound" if from_me else "inbound",
                   "content": body, "type": "text", "created_at": (datetime.utcfromtimestamp(ts).isoformat() + "+00:00") if ts else (datetime.utcnow().isoformat() + "+00:00")}
            if waha_id: row["waha_id"] = waha_id
            to_insert.append(row)
        if to_insert:
            def _save():
                for i in range(0, len(to_insert), 50):
                    try: supabase.table("messages").insert(to_insert[i:i+50]).execute()
                    except: pass
                # Update last_message_preview with the most recent message
                try:
                    last = sorted(to_insert, key=lambda m: m.get("created_at",""))[-1]
                    preview = last.get("content","")[:100]
                    supabase.table("conversations").update({
                        "last_message_preview": preview,
                        "last_message_at": last.get("created_at")
                    }).eq("id", conv_id).execute()
                except: pass
            await run_sync(_save)
            # Invalidate cache so next poll picks up new messages
            cache_del(f"msgs:{conv_id}:latest")
            # Clear conversation cache so preview updates on next poll
            for k in list(_cache.keys()):
                if k.startswith(f"convs:{conv.get('tenant_id')}"): cache_del(k)
    except Exception as e:
        print(f"[WAHA_BG_SYNC] {conv_id}: {e}")


@app.post("/conversations/{conv_id}/messages", dependencies=[Depends(verify_key)])
async def send_message(conv_id: str, body: SendMessage, bg: BackgroundTasks):
    conv = supabase.table("conversations").select("*, contacts(phone)").eq("id", conv_id).single().execute().data
    msg = supabase.table("messages").insert({"conversation_id": conv_id, "tenant_id": conv.get("tenant_id"), "direction": "outbound", "content": body.text, "type": "text", "sent_by": body.sent_by, "is_internal_note": body.is_internal_note}).execute().data[0]
    # Invalidate message cache so next poll fetches fresh (including this outbound msg)
    cache_del(f"msgs:{conv_id}:latest")
    # Pausa auto-pilot por 30min quando atendente responde manualmente
    # (sent_by preenchido = atendente humano; ausente = sistema/auto-pilot)
    if body.sent_by and not body.is_internal_note:
        autopilot_pause(conv_id)
        print(f"[AUTOPILOT] Pausado 30min — atendente {body.sent_by[:8]} respondeu manualmente em conv={conv_id[:8]}")
    if not body.is_internal_note:
        supabase.table("conversations").update({"last_message_at": datetime.utcnow().isoformat(), "last_message_preview": "✓ " + body.text[:78]}).eq("id", conv_id).execute()
        session = conv.get("instance_name") or "default"
        phone_raw = (conv.get("contacts") or {}).get("phone", "")
        digits_only = "".join(c for c in str(phone_raw) if c.isdigit())
        # LID: número > 13 dígitos não-BR = usa @lid, senão @c.us normal
        if len(digits_only) > 13 and not digits_only.startswith("55"):
            async def _send_lid(p=digits_only, t=body.text, s=session):
                try:
                    async with httpx.AsyncClient(timeout=15) as _c:
                        r = await _c.post(f"{WAHA_URL}/api/sendText", headers=waha_headers(),
                            json={"session": s, "chatId": f"{p}@lid", "text": t})
                        print(f"[SEND_MANUAL] LID status={r.status_code}")
                except Exception as e:
                    print(f"[SEND_MANUAL] LID erro: {e}")
            bg.add_task(_send_lid)
        else:
            bg.add_task(waha_send_msg, phone_raw, body.text, session)
    return {"message": msg}

async def watchguard_loop():
    """
    🛡 Watch Guard — roda a cada 5 minutos.
    Para cada instância com copilot_watchguard=True, verifica conversas abertas
    onde a última mensagem é do cliente há mais de 6 minutos e não há resposta da IA.
    Se encontrar, dispara a IA para garantir que o cliente não fique sem resposta.
    """
    import datetime as _dt, time as _time
    await asyncio.sleep(60)  # aguarda 1 min no startup para tudo estabilizar
    print("[WATCHGUARD] Loop iniciado")
    while True:
        try:
            await _watchguard_scan()
        except Exception as e:
            print(f"[WATCHGUARD] Erro no scan: {e}")
        await asyncio.sleep(300)  # 5 minutos


async def _watchguard_scan():
    import datetime as _dt, time as _time
    now_utc = _dt.datetime.utcnow()
    cutoff = (now_utc - _dt.timedelta(minutes=6)).isoformat()

    # Busca todas as instâncias com watchguard ativo
    instances = supabase.table("gateway_instances").select(
        "instance_name,tenant_id,copilot_prompt,copilot_auto_mode,copilot_watchguard"
    ).eq("copilot_watchguard", True).execute().data

    if not instances:
        return

    for inst in instances:
        inst_name = inst["instance_name"]
        tenant_id = inst["tenant_id"]
        try:
            # Verifica créditos e plano do tenant
            tenant = supabase.table("tenants").select("plan,ai_credits,copilot_prompt,is_blocked").eq("id", tenant_id).single().execute().data
            if not tenant or tenant.get("is_blocked"):
                continue
            if tenant.get("plan") == "starter":
                continue
            if (tenant.get("ai_credits") or 0) <= 0:
                continue

            # Busca conversas abertas desta instância onde last_message_at < cutoff (mais de 6min atrás)
            convs = supabase.table("conversations").select(
                "id,status,copilot_auto_mode,last_message_at,unread_count"
            ).eq("instance_name", inst_name).eq("tenant_id", tenant_id).eq("status", "open").lt("last_message_at", cutoff).execute().data

            for conv in convs:
                conv_id = conv["id"]
                # Não interfere se a conversa está pausada pelo atendente
                if autopilot_is_paused(conv_id):
                    continue
                if conv.get("copilot_auto_mode") is False or conv.get("copilot_auto_mode") == "false":
                    continue

                # Verifica se o Redis tem um watcher ativo (debounce em andamento — não duplicar)
                r = _get_redis()
                if r and r.exists(f"7crm:ap_lock:{conv_id}"):
                    continue

                # Verifica se a última mensagem é inbound (cliente sem resposta)
                last_msgs = supabase.table("messages").select("direction,created_at").eq("conversation_id", conv_id).order("created_at", desc=True).limit(1).execute().data
                if not last_msgs:
                    continue
                last = last_msgs[0]
                if last.get("direction") != "inbound":
                    continue  # última msg já é outbound — cliente respondido

                # Verifica se a chave de watchguard já foi usada recentemente (anti-spam: 1 resposta por hora por conversa)
                wg_key = f"7crm:wg_sent:{conv_id}"
                if r and r.exists(wg_key):
                    continue

                print(f"[WATCHGUARD] ⚠️ Cliente sem resposta → conv={conv_id[:8]} inst={inst_name}")

                # Dispara IA diretamente (sem debounce — Watch Guard é o safety net final)
                asyncio.create_task(_watchguard_reply(conv_id, tenant_id, inst_name, inst, tenant))

        except Exception as e:
            print(f"[WATCHGUARD] Erro instância {inst_name}: {e}")


async def _watchguard_reply(conv_id: str, tenant_id: str, instance_name: str, inst_cfg: dict, tenant_data: dict):
    """Dispara resposta da IA via Watch Guard. Mesma lógica do auto-pilot watcher."""
    import time as _time
    r = _get_redis()
    lock_key = f"7crm:ap_lock:{conv_id}"
    wg_key = f"7crm:wg_sent:{conv_id}"

    # Adquire lock para não conflitar com debounce
    if r:
        acquired = r.set(lock_key, "wg", nx=True, ex=120)
        if not acquired:
            print(f"[WATCHGUARD] Lock já ativo para conv={conv_id[:8]}, abortando")
            return

    try:
        ok, remaining, err = consume_credit(tenant_id, 1)
        if not ok:
            print(f"[WATCHGUARD] Sem créditos: {err}")
            return

        all_msgs = supabase.table("messages").select(
            "id,direction,content,type,is_internal_note,media_url,created_at"
        ).eq("conversation_id", conv_id).order("created_at").execute().data

        # Bloco de msgs inbound sem resposta
        last_outbound_idx = -1
        for idx, m in enumerate(all_msgs):
            if m.get("direction") == "outbound" and not m.get("is_internal_note"):
                last_outbound_idx = idx
        pending_inbound = [m for m in all_msgs[last_outbound_idx + 1:] if m.get("direction") == "inbound"]
        if not pending_inbound:
            return

        history_lines = []
        for m in all_msgs[-(40):]:
            if m.get("is_internal_note"): continue
            role = "Atendente" if m.get("direction") == "outbound" else "Cliente"
            history_lines.append(f"{role}: {m.get('content','')}")
        history_text = "\n".join(history_lines)

        company_prompt = inst_cfg.get("copilot_prompt") or tenant_data.get("copilot_prompt") or ""
        msgs_text = "\n".join([f"- {m.get('content','')}" for m in pending_inbound])

        ai_messages = [
            {"role": "user", "content": f"{company_prompt}\n\n---\nHistórico:\n{history_text}\n\n---\nMensagens do cliente sem resposta:\n{msgs_text}\n\nResponda agora de forma natural e direta."}
        ]
        resp = anthropic_client.messages.create(model="claude-opus-4-5", max_tokens=500, messages=ai_messages)
        reply_text = resp.content[0].text.strip()

        if not reply_text:
            return

        phone = pending_inbound[-1].get("content","")
        conv_full = supabase.table("conversations").select("contacts(phone)").eq("id", conv_id).single().execute().data
        contact_phone = (conv_full.get("contacts") or {}).get("phone","")
        if not contact_phone:
            return

        waha_resp = requests.post(f"{WAHA_URL}/api/sendText", json={
            "session": instance_name, "chatId": f"{contact_phone}@s.whatsapp.net", "text": reply_text
        }, headers={"X-Api-Key": WAHA_KEY}, timeout=15)

        if waha_resp.status_code == 201:
            supabase.table("messages").insert({
                "conversation_id": conv_id, "direction": "outbound", "content": reply_text,
                "type": "text", "ai_suggestion": True
            }).execute()
            supabase.table("conversations").update({"last_message_at": "now()", "unread_count": 0}).eq("id", conv_id).execute()
            # Anti-spam: marca que já respondeu nesta conversa (TTL 1h)
            if r:
                r.setex(wg_key, 3600, "1")
            print(f"[WATCHGUARD] ✅ Resposta enviada → conv={conv_id[:8]}")
        else:
            print(f"[WATCHGUARD] ❌ Erro WAHA {waha_resp.status_code}: {waha_resp.text[:100]}")

    except Exception as e:
        print(f"[WATCHGUARD] Erro em _watchguard_reply conv={conv_id[:8]}: {e}")
    finally:
        if r:
            r.delete(lock_key)


async def _autopilot_debounce_trigger_async(conv_id: str, tenant_id: str, instance_name: str):
    """Versão async do trigger — garante que asyncio.create_task funcione corretamente."""
    import time as _time
    r = _get_redis()
    window = random.randint(120, 240)
    deadline = _time.time() + window
    deadline_key = f"7crm:ap_deadline:{conv_id}"
    lock_key = f"7crm:ap_lock:{conv_id}"

    if r:
        r.setex(deadline_key, 600, str(deadline))
        lock_exists = r.exists(lock_key)
        if lock_exists:
            print(f"[AUTOPILOT] Debounce: prazo atualizado +{window}s para conv={conv_id[:8]} (watcher já ativo)")
            return
    print(f"[AUTOPILOT] Iniciando watcher para conv={conv_id[:8]} instância={instance_name} janela={window}s")
    asyncio.create_task(_auto_pilot_watcher(conv_id, tenant_id, instance_name))


def _autopilot_debounce_trigger(conv_id: str, tenant_id: str, instance_name: str):
    """
    Debounce trigger: cada mensagem nova empurra o prazo de resposta para frente.
    Usa Redis para coordenar:
      ap_deadline:{conv_id}  — timestamp Unix (float) de quando responder
      ap_lock:{conv_id}      — lock: só 1 watcher roda por conversa
    Se o watcher já está rodando (lock ativo), apenas atualiza o deadline.
    Se não está, cria uma nova task watcher.
    """
    import time as _time
    r = _get_redis()
    # Janela de debounce: 2-4 min aleatório (re-randomizado a cada mensagem nova)
    window = random.randint(120, 240)
    deadline = _time.time() + window
    deadline_key = f"7crm:ap_deadline:{conv_id}"
    lock_key = f"7crm:ap_lock:{conv_id}"

    if r:
        # Sempre atualiza o deadline (empurra a janela pra frente)
        r.setex(deadline_key, 600, str(deadline))  # TTL 10min
        # Só cria nova task se não há watcher ativo
        lock_exists = r.exists(lock_key)
        if lock_exists:
            print(f"[AUTOPILOT] Debounce: prazo atualizado +{window}s para conv={conv_id[:8]} (watcher já ativo)")
            return
    # Cria watcher (sem Redis: dispara direto com delay fixo)
    asyncio.create_task(_auto_pilot_watcher(conv_id, tenant_id, instance_name))


async def _auto_pilot_watcher(conv_id: str, tenant_id: str, instance_name: str):
    """
    Watcher: aguarda a janela de debounce fechar, acumula todas as mensagens
    recebidas nesse período e envia UMA resposta só para todas elas.
    """
    import datetime as _dt, time as _time
    lock_key = f"7crm:ap_lock:{conv_id}"
    deadline_key = f"7crm:ap_deadline:{conv_id}"
    r = _get_redis()

    # Adquire lock (sem Redis: prossegue direto)
    if r:
        acquired = r.set(lock_key, "1", nx=True, ex=600)
        if not acquired:
            print(f"[AUTOPILOT] Watcher: lock já ativo para conv={conv_id[:8]}, abortando")
            return
    print(f"[AUTOPILOT] Watcher iniciado para conv={conv_id[:8]}")

    try:
        # ── Fase 1: aguarda a janela fechar (polling a cada 10s) ──────────────
        while True:
            await asyncio.sleep(10)
            if r:
                raw = r.get(deadline_key)
                if raw:
                    deadline = float(raw.decode() if isinstance(raw, bytes) else raw)
                    remaining = deadline - _time.time()
                    if remaining > 0:
                        print(f"[AUTOPILOT] conv={conv_id[:8]} aguardando {remaining:.0f}s restantes (novas msgs podem ter chegado)")
                        continue  # deadline foi empurrado por nova mensagem — espera mais
                # Deadline passou (ou chave expirou) → hora de responder
            break

        # ── Fase 2: verifica condições ───────────────────────────────────────
        # Pausa por atendente?
        if autopilot_is_paused(conv_id):
            print(f"[AUTOPILOT] Pausa de atendente ativa — conv={conv_id[:8]}, abortando")
            return

        conv = supabase.table("conversations").select(
            "id,status,copilot_auto_mode,contacts(phone),tenants(plan,ai_credits,copilot_prompt,copilot_auto_mode,copilot_schedule_start,copilot_schedule_end)"
        ).eq("id", conv_id).single().execute().data
        if not conv:
            return

        if conv.get("copilot_auto_mode") is False or conv.get("copilot_auto_mode") == "false":
            print(f"[AUTOPILOT] Pausado nesta conversa {conv_id[:8]}")
            return

        inst_cfg = {}
        if instance_name:
            _rows = supabase.table("gateway_instances").select(
                "copilot_prompt,copilot_auto_mode,copilot_schedule_start,copilot_schedule_end"
            ).eq("instance_name", instance_name).limit(1).execute().data
            if _rows:
                inst_cfg = _rows[0]

        tenant_data = conv.get("tenants") or {}
        mode = inst_cfg.get("copilot_auto_mode") or "off"
        sched_start = inst_cfg.get("copilot_schedule_start") or "18:00"
        sched_end   = inst_cfg.get("copilot_schedule_end")   or "09:00"
        company_prompt_override = inst_cfg.get("copilot_prompt") or tenant_data.get("copilot_prompt")

        # Verifica modo
        should_reply = False
        if mode == "always":
            should_reply = True
        elif mode == "per_conv":
            should_reply = True
        elif mode == "schedule":
            now_brt = _dt.datetime.utcnow() - _dt.timedelta(hours=3)
            sh, sm = map(int, sched_start.split(":"))
            eh, em = map(int, sched_end.split(":"))
            now_mins = now_brt.hour * 60 + now_brt.minute
            start_mins = sh * 60 + sm
            end_mins   = eh * 60 + em
            if start_mins > end_mins:
                should_reply = now_mins >= start_mins or now_mins < end_mins
            else:
                should_reply = start_mins <= now_mins < end_mins

        print(f"[AUTOPILOT] conv={conv_id[:8]} mode={mode} should_reply={should_reply}")
        if not should_reply:
            return

        plan = tenant_data.get("plan", "trial")
        if plan == "starter":
            print(f"[AUTOPILOT] Plano starter — sem auto-pilot")
            return

        # Verifica se já tem outbound recente (atendente pode ter respondido durante a espera)
        recent = supabase.table("messages").select("id,direction,created_at") \
            .eq("conversation_id", conv_id).order("created_at", desc=True).limit(1).execute().data
        if recent and recent[0].get("direction") == "outbound":
            print(f"[AUTOPILOT] Atendente respondeu durante a janela — abortando conv={conv_id[:8]}")
            return

        # ── Fase 3: consome crédito ──────────────────────────────────────────
        ok, remaining, err = consume_credit(tenant_id, 1)
        if not ok:
            print(f"[AUTOPILOT] Sem créditos: {err}")
            return

        # ── Fase 4: coleta TODAS as mensagens inbound acumuladas ─────────────
        all_msgs = supabase.table("messages").select(
            "id,direction,content,type,is_internal_note,media_url,created_at"
        ).eq("conversation_id", conv_id).order("created_at").execute().data

        # Identifica o bloco de mensagens inbound recentes (desde o último outbound)
        last_outbound_idx = -1
        for idx, m in enumerate(all_msgs):
            if m.get("direction") == "outbound" and not m.get("is_internal_note"):
                last_outbound_idx = idx
        pending_inbound = [
            m for m in all_msgs[last_outbound_idx + 1:]
            if m.get("direction") == "inbound" and not m.get("is_internal_note")
        ]
        print(f"[AUTOPILOT] conv={conv_id[:8]}: {len(pending_inbound)} msgs inbound acumuladas na janela")

        # Transcreve áudios pendentes
        for amsg in pending_inbound[:3]:
            if amsg.get("type") in ("audio", "ptt") and amsg.get("content", "").startswith("[Áudio"):
                try:
                    media_url = amsg.get("media_url")
                    if media_url:
                        async with httpx.AsyncClient(timeout=20) as _c:
                            ar = await _c.get(media_url, headers=waha_headers())
                            if ar.status_code == 200:
                                import openai as _oai, io
                                _oai_client = _oai.AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY", ""))
                                if _oai_client.api_key:
                                    transcript = await _oai_client.audio.transcriptions.create(
                                        model="whisper-1",
                                        file=("audio.ogg", io.BytesIO(ar.content), "audio/ogg"),
                                        language="pt"
                                    )
                                    transcribed = transcript.text.strip()
                                    if transcribed:
                                        amsg["content"] = transcribed
                                        supabase.table("messages").update({"content": transcribed}).eq("id", amsg["id"]).execute()
                except Exception as _ae:
                    print(f"[AUTOPILOT] Whisper erro: {_ae}")

        # ── Fase 5: monta contexto e chama IA ────────────────────────────────
        history_str, recent_msgs, old_msgs = trim_context(all_msgs, max_msgs=15)
        existing_summary = supabase.table("conversations").select("ai_summary").eq("id", conv_id).single().execute().data or {}
        summary = await get_or_generate_summary(conv_id, old_msgs, existing_summary.get("ai_summary"))

        # Mensagens acumuladas → texto único para contexto final da IA
        if pending_inbound:
            accumulated_text = "\n".join(
                f"- {m.get('content', '')}" for m in pending_inbound if m.get("content")
            )
        else:
            accumulated_text = (all_msgs[-1].get("content") or "") if all_msgs else ""

        system_prompt = f"""Você é um assistente de atendimento via WhatsApp. Responda de forma natural, direta e humana.
REGRAS IMPORTANTES:
- Responda TODAS as perguntas e mensagens do cliente em UMA ÚNICA mensagem
- Se o cliente enviou várias mensagens seguidas, agrupe sua resposta
- Seja conciso mas completo
- Use linguagem informal e amigável (você, não tu)
- NUNCA diga que é uma IA
- NÃO use markdown, asteriscos ou formatação especial
{f"CONTEXTO DA EMPRESA:\n{company_prompt_override}" if company_prompt_override else ""}
{f"RESUMO DO HISTÓRICO:\n{summary}" if summary else ""}"""

        messages_for_ai = []
        for m in recent_msgs:
            if m.get("is_internal_note"): continue
            role = "assistant" if m.get("direction") == "outbound" else "user"
            text = m.get("content", "")
            if text: messages_for_ai.append({"role": role, "content": text})

        # Garante que o último user message é o bloco acumulado
        if pending_inbound and len(pending_inbound) > 1:
            # Remove msgs inbound duplicadas do final e substitui pelo bloco acumulado
            while messages_for_ai and messages_for_ai[-1]["role"] == "user":
                messages_for_ai.pop()
            messages_for_ai.append({"role": "user", "content": accumulated_text})

        if not messages_for_ai:
            return

        import anthropic as _ant
        _ant_client = _ant.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))
        response = await _ant_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            system=system_prompt,
            messages=messages_for_ai,
        )
        reply_text = response.content[0].text.strip() if response.content else ""
        if not reply_text:
            print(f"[AUTOPILOT] IA retornou resposta vazia")
            return

        print(f"[AUTOPILOT] IA respondeu ({len(pending_inbound)} msgs acumuladas): {reply_text[:80]}")

        # ── Fase 6: salva e envia ─────────────────────────────────────────────
        supabase.table("messages").insert({
            "conversation_id": conv_id,
            "tenant_id": tenant_id,
            "direction": "outbound",
            "content": reply_text,
            "type": "text",
        }).execute()
        supabase.table("conversations").update({
            "last_message_at": _dt.datetime.utcnow().isoformat(),
            "last_message_preview": "✓ " + reply_text[:78],
        }).eq("id", conv_id).execute()

        phone = (conv.get("contacts") or {}).get("phone", "")
        if phone and instance_name:
            digits_only = "".join(c for c in str(phone) if c.isdigit())
            is_lid = len(digits_only) > 13 and not digits_only.startswith("55")
            if is_lid:
                print(f"[AUTOPILOT] LID detectado — tentando @lid para {phone}")
                try:
                    async with httpx.AsyncClient(timeout=15) as _c:
                        _r = await _c.post(f"{WAHA_URL}/api/sendText", headers=waha_headers(),
                            json={"session": instance_name, "chatId": f"{digits_only}@lid", "text": reply_text})
                        print(f"[AUTOPILOT] LID send status={_r.status_code}")
                except Exception as _se:
                    print(f"[AUTOPILOT] LID send erro: {_se}")
            else:
                await waha_send_msg(phone, reply_text, instance_name)

    except Exception as _e:
        print(f"[AUTOPILOT] Erro: {_e}")
        import traceback; traceback.print_exc()
    finally:
        # Sempre libera o lock e limpa o deadline
        try:
            _rf = _get_redis()
            if _rf:
                _rf.delete(lock_key)
                _rf.delete(deadline_key)
        except: pass


async def waha_send_msg(phone: str, text: str, session: str = "default"):
    """Envia mensagem via WAHA — usa session correta da conversa"""
    try:
        # Normaliza phone: remove @suffix e re-adiciona @c.us
        phone_clean = phone.split("@")[0] if "@" in phone else phone
        chat_id = f"{phone_clean}@c.us"
        payload = {"session": session, "chatId": chat_id, "text": text}
        print(f"[WAHA_SEND] session={session} chatId={chat_id} textlen={len(text)}")
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                f"{WAHA_URL}/api/sendText",
                headers=waha_headers(),
                json=payload
            )
            print(f"[WAHA_SEND] status={r.status_code} body={r.text[:300]}")
            return r
    except Exception as _e:
        print(f"[WAHA_SEND] ERRO: {_e}")

@app.post("/conversations/{conv_id}/media", dependencies=[Depends(verify_key)])
async def send_media(conv_id: str, bg: BackgroundTasks, file: UploadFile = File(...), caption: str = Form(""), sent_by: str = Form(None)):
    """Envia imagem ou documento via WhatsApp"""
    conv = supabase.table("conversations").select("*, contacts(phone)").eq("id", conv_id).single().execute().data
    data = await file.read()
    b64 = base64.b64encode(data).decode()
    ftype = file.content_type or "application/octet-stream"
    fname = file.filename or "arquivo"
    # Determine message type
    if ftype.startswith("image/"):
        msg_type = "image"
        display = f"[📷 {fname}]" + (f" {caption}" if caption else "")
    elif ftype.startswith("audio/") or ftype.startswith("video/"):
        msg_type = "video" if ftype.startswith("video/") else "audio"
        display = f"[🎵 {fname}]"
    else:
        msg_type = "document"
        display = f"[📎 {fname}]" + (f" {caption}" if caption else "")
    msg = supabase.table("messages").insert({
        "conversation_id": conv_id, "tenant_id": conv.get("tenant_id"),
        "direction": "outbound", "content": display, "type": msg_type,
        "sent_by": sent_by, "is_internal_note": False
    }).execute().data[0]
    # Pausa auto-pilot por 30min quando atendente envia mídia
    if sent_by:
        autopilot_pause(conv_id)
        print(f"[AUTOPILOT] Pausado 30min — atendente {sent_by[:8]} enviou mídia em conv={conv_id[:8]}")
    supabase.table("conversations").update({"last_message_at": datetime.utcnow().isoformat()}).eq("id", conv_id).execute()
    session = conv.get("instance_name") or "default"
    phone = conv["contacts"]["phone"]
    bg.add_task(waha_send_media, phone, b64, ftype, fname, caption, msg_type, session)
    return {"message": msg}

async def waha_send_media(phone: str, b64: str, mimetype: str, filename: str, caption: str, msg_type: str, session: str = "default"):
    """Envia mídia via WAHA"""
    try:
        chat_id = phone if "@" in phone else f"{phone}@c.us"
        file_obj = {"mimetype": mimetype, "filename": filename, "data": b64}
        if msg_type == "image":
            endpoint = "/api/sendImage"
            payload = {"session": session, "chatId": chat_id, "caption": caption, "file": file_obj}
        else:
            endpoint = "/api/sendFile"
            payload = {"session": session, "chatId": chat_id, "caption": caption, "file": file_obj}
        async with httpx.AsyncClient(timeout=30) as client:
            await client.post(f"{WAHA_URL}{endpoint}", headers=waha_headers(), json=payload)
    except Exception as e:
        print(f"[waha_send_media] error: {e}")

@app.put("/conversations/{conv_id}/assign", dependencies=[Depends(verify_key)])
async def assign_conversation(conv_id: str, body: AssignConversation):
    return supabase.table("conversations").update({"assigned_to": body.user_id, "updated_at": datetime.utcnow().isoformat()}).eq("id", conv_id).execute().data[0]

@app.put("/conversations/{conv_id}/kanban", dependencies=[Depends(verify_key)])
async def update_kanban(conv_id: str, body: UpdateKanban):
    return supabase.table("conversations").update({"kanban_stage": body.stage, "updated_at": datetime.utcnow().isoformat()}).eq("id", conv_id).execute().data[0]

@app.post("/contacts/{contact_id}/block", dependencies=[Depends(verify_key)])
async def block_contact(contact_id: str):
    """Bloqueia contato no WhatsApp via WAHA"""
    try:
        contact = supabase.table("contacts").select("phone, tenant_id").eq("id", contact_id).single().execute().data
        if not contact:
            return {"ok": False, "error": "Contact not found"}
        phone = contact["phone"]
        clean = "".join(c for c in phone if c.isdigit())
        chat_id = f"{clean}@c.us"
        # Find active instance for this tenant
        inst = supabase.table("gateway_instances").select("instance_name").eq("tenant_id", contact["tenant_id"]).limit(1).execute().data
        session = inst[0]["instance_name"] if inst else "default"
        async with httpx.AsyncClient(timeout=10) as client:
            # Try WAHA block endpoint
            r = await client.post(f"{WAHA_URL}/api/contacts/block",
                headers=waha_headers(),
                json={"session": session, "contactId": chat_id}
            )
            if r.status_code in (200, 201):
                supabase.table("contacts").update({"tags": supabase.table("contacts").select("tags").eq("id", contact_id).single().execute().data.get("tags", []) + ["bloqueado"]}).eq("id", contact_id).execute()
                return {"ok": True}
            return {"ok": False, "error": f"WAHA {r.status_code}: {r.text[:100]}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/conversations/{conv_id}/read", dependencies=[Depends(verify_key)])
async def mark_conversation_read(conv_id: str):
    supabase.table("conversations").update({"unread_count": 0}).eq("id", conv_id).execute()
    return {"ok": True}

@app.post("/conversations/{conv_id}/unread", dependencies=[Depends(verify_key)])
async def mark_conversation_unread(conv_id: str):
    supabase.table("conversations").update({"unread_count": 1}).eq("id", conv_id).execute()
    return {"ok": True}

@app.post("/conversations/{conv_id}/unread", dependencies=[Depends(verify_key)])
async def mark_conversation_unread(conv_id: str):
    supabase.table("conversations").update({"unread_count": 1}).eq("id", conv_id).execute()
    return {"ok": True}

@app.post("/messages/{msg_id}/react", dependencies=[Depends(verify_key)])
async def react_to_message(msg_id: str, body: dict):
    """Envia reação a uma mensagem via WAHA. body: {reaction: '👍', tenant_id, conversation_id}"""
    reaction = body.get("reaction", "")
    conv_id = body.get("conversation_id")
    if not conv_id:
        raise HTTPException(status_code=400, detail="conversation_id obrigatório")
    # Busca mensagem e conversa
    msg = supabase.table("messages").select("waha_id").eq("id", msg_id).single().execute().data
    if not msg or not msg.get("waha_id"):
        return {"ok": False, "error": "Mensagem sem waha_id — não é possível reagir"}
    conv = supabase.table("conversations").select("instance_name, contacts(phone)").eq("id", conv_id).single().execute().data
    if not conv:
        raise HTTPException(status_code=404, detail="Conversa não encontrada")
    session = conv.get("instance_name") or "default"
    waha_id = msg["waha_id"]
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.put(
                f"{WAHA_URL}/api/reaction",
                headers=waha_headers(),
                json={"session": session, "messageId": waha_id, "reaction": reaction}
            )
            if r.status_code in (200, 201, 204):
                # Salva reação no banco para exibir localmente
                supabase.table("messages").update({"reaction": reaction, "reaction_by": "me"}).eq("id", msg_id).execute()
                return {"ok": True}
            return {"ok": False, "error": f"WAHA {r.status_code}: {r.text[:100]}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def resolve_conversation(conv_id: str):
    return supabase.table("conversations").update({"status": "resolved", "kanban_stage": "resolved", "updated_at": datetime.utcnow().isoformat()}).eq("id", conv_id).execute().data[0]

@app.put("/conversations/{conv_id}/reopen", dependencies=[Depends(verify_key)])
async def reopen_conversation(conv_id: str):
    return supabase.table("conversations").update({"status": "open", "kanban_stage": "new", "updated_at": datetime.utcnow().isoformat()}).eq("id", conv_id).execute().data[0]

@app.put("/conversations/{conv_id}/pending", dependencies=[Depends(verify_key)])
async def pending_conversation(conv_id: str):
    return supabase.table("conversations").update({"status": "pending", "updated_at": datetime.utcnow().isoformat()}).eq("id", conv_id).execute().data[0]

@app.put("/conversations/{conv_id}/labels")
async def update_conversation_labels(conv_id: str, body: UpdateLabels):
    # Salva label_ids direto no array conversations.labels[]
    supabase.table("conversations").update({"labels": body.label_ids or []}).eq("id", conv_id).execute()
    # Invalidar cache
    for key in [f"convs:{conv_id}", "convs:*"]:
        try: cache_del(key)
        except: pass
    # Sync label names to contact.tags so label follows the phone number
    try:
        conv = supabase.table("conversations").select("contact_id").eq("id", conv_id).single().execute().data
        if conv and body.label_ids:
            label_names = [r["name"] for r in supabase.table("labels").select("name").in_("id", body.label_ids).execute().data]
            supabase.table("contacts").update({"tags": label_names}).eq("id", conv["contact_id"]).execute()
        elif conv and not body.label_ids:
            supabase.table("contacts").update({"tags": []}).eq("id", conv["contact_id"]).execute()
    except Exception as e:
        print(f"label sync to contact err: {e}")
    return {"ok": True}

# ── CONTACTS ─────────────────────────────────────────────
# ── LABELS CRUD ──────────────────────────────────────────
@app.get("/labels")
async def get_labels(tenant_id: str = Depends(enforce_tenant)):
    rows = supabase.table("labels").select("*").eq("tenant_id", tenant_id).order("name").execute().data
    return {"labels": rows}

@app.post("/labels")
async def create_label(body: dict):
    tenant_id = body.get("tenant_id")
    name = (body.get("name") or "").strip()
    color = body.get("color", "#00a884")
    if not name: raise HTTPException(400, "Nome obrigatório")
    row = supabase.table("labels").insert({"tenant_id": tenant_id, "name": name, "color": color}).execute().data[0]
    return {"label": row}

@app.put("/labels/{label_id}", dependencies=[Depends(verify_key)])
async def update_label(label_id: str, body: dict):
    updates = {}
    if "name" in body: updates["name"] = body["name"]
    if "color" in body: updates["color"] = body["color"]
    row = supabase.table("labels").update(updates).eq("id", label_id).execute().data
    return {"ok": True, "label": row[0] if row else None}

@app.delete("/labels/{label_id}", dependencies=[Depends(verify_key)])
async def delete_label(label_id: str):
    # Remove from all conversations first
    # Remove o label_id do array labels[] de todas as conversas do tenant
    try:
        all_convs = supabase.table("conversations").select("id,labels").eq("tenant_id", body.get("tenant_id","")).execute().data
        for c in all_convs:
            raw = c.get("labels") or []
            if label_id in raw:
                supabase.table("conversations").update({"labels": [l for l in raw if l != label_id]}).eq("id", c["id"]).execute()
    except Exception as e:
        print(f"cleanup label from convs err: {e}")
    except: pass
    supabase.table("labels").delete().eq("id", label_id).execute()
    return {"ok": True}


@app.get("/contacts")
async def list_contacts(tenant_id: str = Depends(enforce_tenant)):
    return {"contacts": supabase.table("contacts").select("*").eq("tenant_id", tenant_id).order("name").execute().data}

@app.put("/contacts/{contact_id}", dependencies=[Depends(verify_key)])
async def update_contact(contact_id: str, body: dict):
    """Atualiza nome e/ou observações de um contato."""
    update = {}
    if "name" in body and body["name"] is not None:
        name = body["name"].strip()
        if name:
            update["name"] = name
    if "notes" in body:
        update["notes"] = body["notes"]
    if "phone" in body and body["phone"]:
        update["phone"] = body["phone"].strip()
    if "tags" in body:
        update["tags"] = body["tags"]
    if not update:
        raise HTTPException(status_code=400, detail="Nenhum campo para atualizar")
    update["updated_at"] = datetime.utcnow().isoformat()
    result = supabase.table("contacts").update(update).eq("id", contact_id).execute()
    if not result.data:
        raise HTTPException(status_code=404, detail="Contato não encontrado")
    return {"contact": result.data[0]}

@app.get("/contacts/profile-picture", dependencies=[Depends(verify_key)])
async def get_profile_picture(phone: str, instance: str = "default"):
    """
    Busca foto de perfil de um contato via WAHA.
    Retorna {"url": "..."} ou {"url": null} se não tiver foto.
    """
    if not WAHA_URL:
        return {"url": None}
    try:
        # Normaliza o chat_id
        if "@" in phone:
            chat_id = phone  # mantém @lid ou @c.us original
        else:
            clean = "".join(c for c in phone if c.isdigit())
            chat_id = f"{clean}@c.us"

        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(
                f"{WAHA_URL}/api/contacts/profile-picture",
                headers=waha_headers(),
                params={"session": instance, "contactId": chat_id}
            )
            if r.status_code == 200:
                data = r.json()
                url = data.get("eurl") or data.get("url") or data.get("profilePictureUrl") or data.get("profilePictureURL")
                # Persist to DB so next load is instant (no WAHA call)
                if url and phone:
                    try:
                        clean = "".join(c for c in phone if c.isdigit())
                        supabase.table("contacts").update({"profile_picture_url": url, "last_sync_at": datetime.utcnow().isoformat()}).eq("phone", clean).execute()
                    except: pass
                return {"url": url}
    except Exception as e:
        print(f"[profile_picture] erro {phone}: {e}")
    return {"url": None}

# ── TASKS ────────────────────────────────────────────────
@app.get("/tasks/completed")
async def list_completed_tasks(tenant_id: str = Depends(enforce_tenant), days: int = 7):
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    res = supabase.table("tasks").select("*, conversations(id,tenant_id,contacts(name,phone)), users!assigned_to(name)").eq("done", True).gte("done_at", since).order("done_at", desc=True).execute()
    return {"tasks": [t for t in (res.data or []) if (t.get("conversations") or {}).get("tenant_id") == tenant_id]}

@app.get("/tasks")
async def list_tasks(tenant_id: str = Depends(enforce_tenant), conversation_id: Optional[str] = None):
    q = supabase.table("tasks").select("*, conversations(id,tenant_id), users!assigned_to(name)").eq("done", False)
    if conversation_id: q = q.eq("conversation_id", conversation_id)
    res = q.order("created_at", desc=True).execute()
    return {"tasks": [t for t in (res.data or []) if (t.get("conversations") or {}).get("tenant_id") == tenant_id]}

@app.post("/tasks")
async def create_task(body: CreateTask):
    return {"task": supabase.table("tasks").insert({"conversation_id": body.conversation_id, "title": body.title, "description": body.description, "assigned_to": body.assigned_to, "due_at": body.due_at, "done": False}).execute().data[0]}

@app.put("/tasks/{task_id}/done", dependencies=[Depends(verify_key)])
async def complete_task(task_id: str):
    supabase.table("tasks").update({"done": True, "done_at": datetime.utcnow().isoformat()}).eq("id", task_id).execute()
    return {"ok": True}

@app.post("/tasks/{task_id}/updates", dependencies=[Depends(verify_key)])
async def add_task_update(task_id: str, body: CreateTaskUpdate):
    return {"update": supabase.table("task_updates").insert({"task_id": task_id, "content": body.content, "author": body.author}).execute().data[0]}

@app.get("/tasks/{task_id}/updates", dependencies=[Depends(verify_key)])
async def get_task_updates(task_id: str):
    return {"updates": supabase.table("task_updates").select("*").eq("task_id", task_id).order("created_at").execute().data}

# ── USERS ────────────────────────────────────────────────
@app.get("/users")
async def list_users(tenant_id: str = Depends(enforce_tenant)):
    return {"users": supabase.table("users").select("id,name,email,role,avatar_color").eq("tenant_id", tenant_id).eq("is_active", True).execute().data}

# ── QUICK REPLIES ────────────────────────────────────────
@app.get("/quick-replies")
async def list_quick_replies(tenant_id: str = Depends(enforce_tenant)):
    return {"quick_replies": supabase.table("quick_replies").select("*").eq("tenant_id", tenant_id).execute().data}

@app.post("/quick-replies")
async def create_quick_reply(body: CreateQuickReply, tenant_id: str):
    return supabase.table("quick_replies").insert({"tenant_id": tenant_id, "title": body.title, "content": body.content}).execute().data[0]

# ── CO-PILOT ─────────────────────────────────────────────
@app.get("/conversations/{conv_id}/suggest", dependencies=[Depends(verify_key)])
async def ai_suggest(conv_id: str, tenant_id: str = None):
    # Valida IA disponível
    if not OPENAI_API_KEY and not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="Nenhuma API de IA configurada")

    # Resolve tenant_id
    if not tenant_id:
        conv_data = supabase.table("conversations").select("tenant_id").eq("id", conv_id).single().execute().data
        tenant_id = conv_data.get("tenant_id") if conv_data else None

    # Consome crédito
    if tenant_id:
        ok, remaining, err = consume_credit(tenant_id, 1)
        if not ok: raise HTTPException(status_code=402, detail=err)

    # Busca todas as mensagens + dados da conversa/tenant
    all_msgs = supabase.table("messages")         .select("direction,content,is_internal_note,created_at")         .eq("conversation_id", conv_id)         .order("created_at").execute().data

    conv = supabase.table("conversations")         .select("ai_summary, tenant_id, instance_name, tenants(copilot_prompt, name)")         .eq("id", conv_id).single().execute().data

    # Divide em recentes (últimas 15) e antigas
    history_str, recent_msgs, old_msgs = trim_context(all_msgs, max_msgs=15)

    # Resumo das mensagens antigas (gerado 1x, reutilizado sempre)
    existing_summary = conv.get("ai_summary")
    summary = await get_or_generate_summary(conv_id, old_msgs, existing_summary)

    # Monta system prompt — prioridade: instância > tenant > fallback
    tenant_data = conv.get("tenants") or {}
    instance_prompt = None
    if conv.get("instance_name"):
        inst_row = supabase.table("gateway_instances").select("copilot_prompt").eq("instance_name", conv["instance_name"]).limit(1).execute().data
        if inst_row:
            instance_prompt = inst_row[0].get("copilot_prompt")
    company_prompt = instance_prompt or tenant_data.get("copilot_prompt") or         f"Você é atendente da empresa {tenant_data.get('name', '')}. Seja simpático e objetivo."

    system = company_prompt + "\n\nSugira apenas a próxima resposta do atendente, sem explicações. Breve e natural. Máximo 2 frases."

    # Monta contexto: resumo (se houver) + últimas 15 mensagens
    context_parts = []
    if summary:
        context_parts.append(f"[RESUMO DO INÍCIO DA CONVERSA]\n{summary}\n[FIM DO RESUMO]")
    context_parts.append(f"[ÚLTIMAS MENSAGENS]\n{history_str}")
    full_context = "\n\n".join(context_parts)

    suggestion = await call_ai(
        system,
        f"{full_context}\n\nSugira a próxima resposta do atendente:",
        max_tokens=200
    )
    return {"suggestion": suggestion, "used_summary": bool(summary), "msgs_in_context": len(recent_msgs)}

# ── TENANT ───────────────────────────────────────────────
@app.get("/tenant")
async def get_tenant(tenant_id: str = Depends(enforce_tenant)):
    tenant = supabase.table("tenants").select("id,name,plan,copilot_prompt,copilot_prompt_summary,copilot_auto_mode,copilot_schedule_start,copilot_schedule_end,ai_credits,ai_credits_reset_at,trial_ends_at").eq("id", tenant_id).single().execute().data
    if tenant:
        credits, plan, limit = get_tenant_credits(tenant_id)
        tenant["ai_credits"] = credits
        tenant["ai_credits_limit"] = limit
        tenant["ai_credits_pct"] = round((credits / limit * 100) if limit > 0 else 100)
    return tenant

@app.get("/credits")
async def get_credits(tenant_id: str = Depends(enforce_tenant)):
    credits, plan, limit = get_tenant_credits(tenant_id)
    return {"credits": credits, "limit": limit, "plan": plan,
            "pct": round(credits / limit * 100) if limit > 0 else 100,
            "warning": credits < limit * 0.25,
            "resets_monthly": PLAN_RESETS.get(plan, False)}

@app.post("/credits/buy", dependencies=[Depends(verify_key)])
async def buy_credits(body: dict):
    tenant_id = body.get("tenant_id")
    amount = int(body.get("amount", 500))
    if amount not in [500, 1000, 2000]: raise HTTPException(400, "Pacote inválido")
    supabase.table("tenants").update({"ai_credits": supabase.table("tenants").select("ai_credits").eq("id", tenant_id).single().execute().data.get("ai_credits", 0) + amount, "ai_credits_purchased": (supabase.table("tenants").select("ai_credits_purchased").eq("id", tenant_id).single().execute().data.get("ai_credits_purchased") or 0) + amount}).eq("id", tenant_id).execute()
    return {"ok": True, "added": amount}

@app.put("/tenant/copilot-prompt", dependencies=[Depends(verify_key)])
async def update_copilot_prompt(body: UpdateCopilotPrompt):
    """Salva config de IA — SEMPRE por instância. Sem fallback para tenant."""
    if not body.instance_name:
        raise HTTPException(400, "instance_name obrigatório — cada configuração é por número")
    update_data = {
        "copilot_auto_mode": body.copilot_auto_mode,
        "copilot_schedule_start": body.copilot_schedule_start,
        "copilot_schedule_end": body.copilot_schedule_end,
        "copilot_watchguard": body.copilot_watchguard,
    }
    if body.copilot_prompt is not None:
        update_data["copilot_prompt"] = body.copilot_prompt
    res = supabase.table("gateway_instances").update(update_data).eq("instance_name", body.instance_name).eq("tenant_id", body.tenant_id).execute()
    print(f"[CONFIG_IA] Salvo instance={body.instance_name} mode={body.copilot_auto_mode} watchguard={body.copilot_watchguard}")
    return {"ok": True, "saved_to": "instance", "instance": body.instance_name}

@app.get("/whatsapp/instance-config", dependencies=[Depends(verify_key)])
async def get_instance_config(tenant_id: str, instance_name: str):
    """Retorna config de IA de uma instância específica."""
    row = supabase.table("gateway_instances").select(
        "instance_name,label,phone,copilot_prompt,copilot_auto_mode,copilot_schedule_start,copilot_schedule_end,copilot_watchguard"
    ).eq("tenant_id", tenant_id).eq("instance_name", instance_name).limit(1).execute().data
    if not row:
        raise HTTPException(404, "Instância não encontrada")
    return row[0]

@app.put("/whatsapp/instance-config", dependencies=[Depends(verify_key)])
async def update_instance_config(body: dict):
    """Salva config de IA (prompt + modo + horário) por instância."""
    tenant_id = body.get("tenant_id")
    instance_name = body.get("instance_name")
    if not tenant_id or not instance_name:
        raise HTTPException(400, "tenant_id e instance_name obrigatórios")
    update_data = {}
    for field in ["copilot_prompt", "copilot_auto_mode", "copilot_schedule_start", "copilot_schedule_end"]:
        if field in body:
            update_data[field] = body[field]
    res = supabase.table("gateway_instances").update(update_data).eq("instance_name", instance_name).eq("tenant_id", tenant_id).execute()
    return {"ok": True, "instance": instance_name}

# ── WHATSAPP CONNECTION (WAHA) ────────────────────────────
@app.get("/whatsapp/status", dependencies=[Depends(verify_key)])
async def whatsapp_status(instance: str = "default"):
    """Consulta status da sessão WAHA"""
    if not WAHA_URL:
        raise HTTPException(status_code=503, detail="WAHA não configurada")
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(f"{WAHA_URL}/api/sessions/{instance}", headers=waha_headers())
            if r.status_code == 404:
                return {"state": "not_found", "connected": False, "instance": instance, "phone": ""}
            data = r.json()
            status = data.get("status", "STOPPED")
            connected = status == "WORKING"
            me = data.get("me") or {}
            phone = me.get("id", "").replace("@c.us", "").replace("@s.whatsapp.net", "")
            return {"state": status, "connected": connected, "instance": instance, "phone": phone}
    except Exception as e:
        return {"state": "error", "connected": False, "error": str(e)}

@app.get("/whatsapp/check-connected", dependencies=[Depends(verify_key)])
async def whatsapp_check_connected(instance: str = "default"):
    """Polling leve — só verifica se sessão já está WORKING. Usado pelo frontend após exibir QR."""
    if not WAHA_URL:
        return {"connected": False}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{WAHA_URL}/api/sessions/{instance}", headers=waha_headers())
            if r.status_code == 200:
                data = r.json()
                status = data.get("status", "")
                if status == "WORKING":
                    me = data.get("me") or {}
                    phone = me.get("id", "").replace("@c.us", "").replace("@s.whatsapp.net", "")
                    # Configura webhook imediatamente ao detectar conexão
                    asyncio.create_task(ensure_webhooks())
                    return {"connected": True, "phone": phone}
        return {"connected": False}
    except:
        return {"connected": False}

@app.get("/whatsapp/qrcode", dependencies=[Depends(verify_key)])
async def whatsapp_qrcode(instance: str = "default"):
    """Retorna QR Code para conexão via WAHA screenshot"""
    if not WAHA_URL:
        raise HTTPException(status_code=503, detail="WAHA não configurada")
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            # Verifica se sessão existe
            r = await client.get(f"{WAHA_URL}/api/sessions/{instance}", headers=waha_headers())

            if r.status_code == 404:
                # Sessão não existe — cria agora
                create_r = await client.post(f"{WAHA_URL}/api/sessions", headers=waha_headers(),
                    json={"name": instance, "start": True})
                if create_r.status_code not in (200, 201):
                    return {"qr_code": "", "state": "error", "error": f"Erro ao criar sessão: {create_r.text}"}
                await asyncio.sleep(4)

            elif r.status_code == 200:
                data = r.json()
                status = data.get("status", "")
                if status == "WORKING":
                    me = data.get("me") or {}
                    phone = me.get("id", "").replace("@c.us", "").replace("@s.whatsapp.net", "")
                    return {"qr_code": "", "state": "open", "connected": True, "phone": phone}
                if status in ("STOPPED", "FAILED", "STARTING"):
                    await client.post(f"{WAHA_URL}/api/sessions/{instance}/restart", headers=waha_headers())
                    # Wait until session reaches SCAN_QR_CODE (up to 15s)
                    for _ in range(10):
                        await asyncio.sleep(2)
                        s2 = await client.get(f"{WAHA_URL}/api/sessions/{instance}", headers=waha_headers())
                        if s2.status_code == 200:
                            st2 = s2.json().get("status", "")
                            if st2 == "SCAN_QR_CODE": break
                            if st2 == "WORKING":
                                me = s2.json().get("me") or {}
                                phone = me.get("id", "").replace("@c.us", "").replace("@s.whatsapp.net", "")
                                return {"qr_code": "", "state": "open", "connected": True, "phone": phone}
                elif status == "SCAN_QR_CODE":
                    pass  # already ready for QR

            # WAHA Plus: endpoint /api/{session}/auth/qr retorna PNG puro (QR limpo, sem screenshot)
            for attempt in range(6):
                qr_r = await client.get(f"{WAHA_URL}/api/{instance}/auth/qr", headers=waha_headers())
                if qr_r.status_code == 200 and qr_r.content and qr_r.headers.get("content-type","").startswith("image"):
                    b64 = base64.b64encode(qr_r.content).decode()
                    return {"qr_code": f"data:image/png;base64,{b64}", "state": "SCAN_QR_CODE"}
                # Fallback: screenshot (WAHA Core)
                screenshot = await client.get(f"{WAHA_URL}/api/screenshot?session={instance}", headers=waha_headers())
                if screenshot.status_code == 200 and screenshot.content:
                    b64 = base64.b64encode(screenshot.content).decode()
                    return {"qr_code": f"data:image/png;base64,{b64}", "state": "SCAN_QR_CODE", "screenshot": True}
                await asyncio.sleep(3)

            return {"qr_code": "", "state": "timeout", "error": "QR Code não disponível — tente novamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/whatsapp/disconnect", dependencies=[Depends(verify_key)])
async def whatsapp_disconnect(body: dict):
    """Desconecta sessão WAHA"""
    instance = body.get("instance", "default")
    if not WAHA_URL:
        raise HTTPException(status_code=503, detail="WAHA não configurada")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(f"{WAHA_URL}/api/sessions/{instance}/stop", headers=waha_headers())
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/whatsapp/instances", dependencies=[Depends(verify_key)])
async def whatsapp_instances():
    """Lista todas as sessões WAHA"""
    if not WAHA_URL:
        raise HTTPException(status_code=503, detail="WAHA não configurada")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{WAHA_URL}/api/sessions", headers=waha_headers())
            return {"instances": r.json()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/whatsapp/tenant-instances")
async def whatsapp_tenant_instances(tenant_id: str = Depends(enforce_tenant)):
    """Lista instâncias do tenant com status real-time — requests paralelos"""
    # Check cache (30s TTL) to avoid hammering WAHA on every poll
    cache_key = f"instances:{tenant_id}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    tenant = supabase.table("tenants").select("plan").eq("id", tenant_id).single().execute().data
    plan = tenant.get("plan", "starter")
    max_numbers = PLAN_FEATURES.get(plan, PLAN_FEATURES["starter"])["numbers"]
    db_instances = supabase.table("gateway_instances").select("*").eq("tenant_id", tenant_id).order("created_at").execute().data

    async def get_waha_status(inst):
        inst_name = inst.get("instance_name") or inst["id"]
        if not WAHA_URL:
            return {**inst, "connected": False, "phone": inst.get("phone", "")}
        try:
            async with httpx.AsyncClient(timeout=4) as client:
                r = await client.get(f"{WAHA_URL}/api/sessions/{inst_name}", headers=waha_headers())
                if r.status_code == 200:
                    data = r.json()
                    connected = data.get("status") == "WORKING"
                    me = data.get("me") or {}
                    phone = me.get("id","").replace("@c.us","").replace("@s.whatsapp.net","")
                    return {**inst, "connected": connected, "phone": phone or inst.get("phone", "")}
        except: pass
        return {**inst, "connected": False, "phone": inst.get("phone", "")}

    # Fetch all instances IN PARALLEL
    result = await asyncio.gather(*[get_waha_status(inst) for inst in db_instances])
    response = {"instances": list(result), "max_numbers": max_numbers, "plan": plan}
    cache_set(cache_key, response, ttl=25)  # cache 25s
    return response

@app.post("/whatsapp/create-instance", dependencies=[Depends(verify_key)])
async def whatsapp_create_instance(body: dict):
    """Cria nova instância WhatsApp para o tenant"""
    tenant_id = body.get("tenant_id")
    label = (body.get("label") or "Número").strip()[:40]
    tenant = supabase.table("tenants").select("plan").eq("id", tenant_id).single().execute().data
    plan = tenant.get("plan", "starter")
    max_numbers = PLAN_FEATURES.get(plan, PLAN_FEATURES["starter"])["numbers"]
    current = supabase.table("gateway_instances").select("id").eq("tenant_id", tenant_id).execute().data
    if len(current) >= max_numbers:
        raise HTTPException(400, f"Limite de {max_numbers} número(s) atingido para o plano {plan}")
    import uuid
    inst_name = f"t{tenant_id[:6]}-{uuid.uuid4().hex[:6]}"
    if WAHA_URL:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                # Verificar se sessão já existe no WAHA antes de criar (evita duplicatas)
                check = await client.get(f"{WAHA_URL}/api/sessions/{inst_name}", headers=waha_headers())
                if check.status_code != 200:
                    # Criar sessão nova — Store + FullSync habilitados por padrão
                    await client.post(f"{WAHA_URL}/api/sessions", headers=waha_headers(),
                        json={"name": inst_name, "config": {
                            "noweb": {
                                "store": {"enabled": True, "fullSync": True}
                            },
                            "webhooks": [{
                                "url": f"{BACKEND_URL}/webhook/inbox",
                                "events": ["message", "message.any", "session.status"],
                                "customHeaders": [{"name": "x-api-key", "value": WEBHOOK_SECRET}]
                            }]
                        }})
                    print(f"[CREATE-INSTANCE] Sessao {inst_name} criada no WAHA")
                else:
                    print(f"[CREATE-INSTANCE] Sessao {inst_name} ja existe no WAHA, reutilizando")
        except Exception as e:
            print(f"[CREATE-INSTANCE] WAHA error: {e}")
    db_row = supabase.table("gateway_instances").insert({
        "tenant_id": tenant_id, "instance_name": inst_name,
        "label": label, "status": "disconnected", "plan": plan
    }).execute().data[0]
    return {"ok": True, "instance": db_row}

@app.delete("/whatsapp/delete-instance", dependencies=[Depends(verify_key)])
async def whatsapp_delete_instance(body: dict):
    """Remove instância do tenant e apaga todo o histórico vinculado"""
    tenant_id = body.get("tenant_id")
    instance_id = body.get("instance_id")
    inst_name = body.get("instance_name")
    delete_history = body.get("delete_history", True)  # default: apaga histórico

    # 1. Desconectar no WAHA
    if WAHA_URL and inst_name:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.delete(f"{WAHA_URL}/api/sessions/{inst_name}", headers=waha_headers())
        except: pass

    # 2. Cascata: apagar conversas/mensagens vinculadas a esta instância
    if delete_history and tenant_id:
        def _cascade():
            convs = supabase.table("conversations").select("id").eq("tenant_id", tenant_id).eq("instance_name", inst_name).execute().data
            conv_ids = [c["id"] for c in convs]
            if conv_ids:
                try: supabase.table("tasks").delete().in_("conversation_id", conv_ids).execute()
                except Exception as e: print(f"delete tasks err: {e}")
                try: supabase.table("messages").delete().in_("conversation_id", conv_ids).execute()
                except Exception as e: print(f"delete messages err: {e}")
                # labels agora são array na tabela conversations — não há conversation_labels
                except Exception: pass
                try: supabase.table("conversations").delete().in_("id", conv_ids).execute()
                except Exception as e: print(f"delete convs err: {e}")
        await run_sync(_cascade)

    # 3. Remove a instância do banco
    try:
        supabase.table("gateway_instances").delete().eq("id", instance_id).eq("tenant_id", tenant_id).execute()
    except Exception as e:
        print(f"delete gateway_instances err: {e}")
    return {"ok": True}


# ── TEMP CLEANUP — remove after use ─────────────────────
@app.post("/admin/reset-tenant", dependencies=[Depends(verify_key)])
async def reset_tenant(body: dict, admin=Depends(require_super_admin)):
    """Limpa todo o histórico de um tenant e reseta senha do usuário admin"""
    tenant_id = body.get("tenant_id")
    new_password = body.get("new_password", "")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id required")

    def _wipe():
        import traceback
        results = {}
        # 1. Busca todas as conversas do tenant
        try:
            convs = supabase.table("conversations").select("id").eq("tenant_id", tenant_id).execute().data
            conv_ids = [c["id"] for c in convs]
            results["conversations_found"] = len(conv_ids)
        except Exception as e:
            results["conv_fetch_err"] = str(e)
            conv_ids = []

        if conv_ids:
            # Deleta em lotes de 100
            for i in range(0, len(conv_ids), 100):
                chunk = conv_ids[i:i+100]
                try: supabase.table("messages").delete().in_("conversation_id", chunk).execute()
                except Exception as e: results[f"msg_del_{i}"] = str(e)
                try: supabase.table("tasks").delete().in_("conversation_id", chunk).execute()
                except Exception: pass
                # labels agora são array na tabela conversations — não há conversation_labels
                except Exception: pass
                try: supabase.table("conversations").delete().in_("id", chunk).execute()
                except Exception as e: results[f"conv_del_{i}"] = str(e)

        # 2. Deleta contatos do tenant
        try:
            supabase.table("contacts").delete().eq("tenant_id", tenant_id).execute()
            results["contacts"] = "deleted"
        except Exception as e: results["contacts_err"] = str(e)

        # 3. Deleta instâncias WhatsApp do tenant
        try:
            supabase.table("gateway_instances").delete().eq("tenant_id", tenant_id).execute()
            results["instances"] = "deleted"
        except Exception as e: results["instances_err"] = str(e)

        # 4. Reseta senha do usuário admin se fornecida
        if new_password:
            try:
                import bcrypt
                hashed = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
                supabase.table("users").update({"password_hash": hashed}).eq("tenant_id", tenant_id).execute()
                results["password"] = "reset"
            except Exception as e: results["password_err"] = str(e)

        return results

    result = await run_sync(_wipe)
    return {"ok": True, "result": result}

# ── BROADCASTS ───────────────────────────────────────────
@app.post("/broadcasts/suggest-message", dependencies=[Depends(verify_key)])
async def suggest_broadcast_message(body: dict):
    if not ANTHROPIC_API_KEY: raise HTTPException(status_code=503, detail="Anthropic API key não configurada")
    tenant = supabase.table("tenants").select("copilot_prompt,name").eq("id", body.get("tenant_id")).single().execute().data
    system = (tenant.get("copilot_prompt") or f"Você é atendente da empresa {tenant['name']}.") + "\n\nCrie mensagens de WhatsApp curtas e naturais. Use {nome} para personalizar."
    suggestion = await call_ai(system, f"Crie uma mensagem de disparo para WhatsApp com objetivo: {body.get('objective', '')}\n\nRetorne apenas a mensagem, sem explicações.", max_tokens=400)
    return {"suggestion": suggestion}

@app.post("/broadcasts/preview-resume", dependencies=[Depends(verify_key)])
async def preview_resume_message(body: dict):
    """Gera preview da mensagem 'Retomar conversa' — consome 3 créditos."""
    if not ANTHROPIC_API_KEY: raise HTTPException(status_code=503, detail="Anthropic API key não configurada")
    tenant_id = body.get("tenant_id")
    ok_credit, credits_left, err = consume_credit(tenant_id, 3)
    if not ok_credit:
        raise HTTPException(status_code=402, detail=err or "Créditos insuficientes")
    msg = await generate_resume_message(
        tenant_id,
        body.get("conversation_id"),
        body.get("contact_name") or "",
        body.get("contact_phone") or "",
        body.get("objective") or ""
    )
    return {"preview": msg, "credits_left": credits_left}

@app.post("/broadcasts")
async def create_broadcast(body: CreateBroadcast, bg: BackgroundTasks):
    bcast = supabase.table("broadcasts").insert({"tenant_id": body.tenant_id, "name": body.name, "message": body.message, "status": "scheduled" if body.scheduled_at else "pending", "scheduled_at": body.scheduled_at, "interval_min": max(body.interval_min, 60), "interval_max": max(body.interval_max, 90), "total_recipients": len(body.recipients), "sent_count": 0, "failed_count": 0, "ai_personalize": body.ai_personalize, "resume_conversation": body.resume_conversation, "instance_name": body.instance_name}).execute().data[0]
    if body.recipients:
        supabase.table("broadcast_recipients").insert([{"broadcast_id": bcast["id"], "phone": r["phone"], "name": r.get("name"), "contact_id": r.get("contact_id"), "conversation_id": r.get("conversation_id"), "status": "pending"} for r in body.recipients]).execute()
    if not body.scheduled_at:
        bg.add_task(run_broadcast, bcast["id"], body.interval_min, body.interval_max)
    return {"broadcast": bcast}

@app.get("/broadcasts")
async def list_broadcasts(tenant_id: str = Depends(enforce_tenant)):
    return {"broadcasts": supabase.table("broadcasts").select("*").eq("tenant_id", tenant_id).order("created_at", desc=True).execute().data}

@app.put("/broadcasts/{broadcast_id}/cancel", dependencies=[Depends(verify_key)])
async def cancel_broadcast(broadcast_id: str):
    supabase.table("broadcasts").update({"status": "cancelled"}).eq("id", broadcast_id).execute()
    return {"ok": True}

async def generate_personalized_message(tenant_id: str, conversation_id: str, contact_name: str, contact_phone: str) -> str:
    """Gera mensagem personalizada via Claude usando histórico + prompt do tenant."""
    try:
        # Busca tenant prompt
        tenant = supabase.table("tenants").select("copilot_prompt, name").eq("id", tenant_id).single().execute().data
        tenant_prompt = tenant.get("copilot_prompt") or f"Você é um atendente de {tenant.get('name', 'nossa empresa')}. Seja cordial e objetivo."
        # Busca últimas mensagens da conversa
        history = []
        if conversation_id:
            msgs = supabase.table("messages").select("direction,content,created_at").eq("conversation_id", conversation_id).order("created_at", desc=True).limit(20).execute().data
            for m in reversed(msgs):
                role = "Cliente" if m["direction"] == "inbound" else "Atendente"
                history.append(f"{role}: {m.get('content','')}")
        hist_text = "\n".join(history) if history else "Sem histórico disponível."
        system = f"{tenant_prompt}\n\nVocê deve criar uma mensagem de reengajamento curta e personalizada para retomar o contato com este cliente que está inativo há alguns dias. A mensagem deve ser natural, não soar como disparo em massa, e fazer referência ao contexto da última conversa quando possível. Use {'{nome}'} se quiser incluir o nome."
        user_msg = f"Nome do cliente: {contact_name or 'Cliente'}\nTelefone: {contact_phone}\n\nÚltimas mensagens:\n{hist_text}\n\nGere uma mensagem de reengajamento curta (máx 2 parágrafos) para enviar agora via WhatsApp."
        msg = await call_ai(system, user_msg, max_tokens=300)
        return msg.replace("{nome}", contact_name or "").replace("{telefone}", contact_phone or "")
    except Exception as e:
        print(f"[AI personalize error] {e}")
        return f"Olá {contact_name or ''}! Tudo bem? Gostaríamos de retomar nosso contato. 😊"

async def generate_resume_message(tenant_id: str, conversation_id: str, contact_name: str, contact_phone: str, objective: str) -> str:
    """
    🔄 Retomar conversa — lê histórico COMPLETO (até 80 msgs) e gera mensagem
    personalizada para reengajar o cliente. Consome 3 créditos.
    """
    try:
        tenant = supabase.table("tenants").select("copilot_prompt, name").eq("id", tenant_id).single().execute().data
        company_prompt = tenant.get("copilot_prompt") or f"Você é um atendente de {tenant.get('name', 'nossa empresa')}. Seja cordial e objetivo."

        history = []
        if conversation_id:
            msgs = supabase.table("messages").select("direction,content,type,created_at").eq("conversation_id", conversation_id).order("created_at", desc=True).limit(80).execute().data
            for m in reversed(msgs):
                if m.get("is_internal_note"): continue
                role = "Cliente" if m["direction"] == "inbound" else "Atendente/IA"
                content = m.get("content", "")
                if content and not content.startswith("["):
                    history.append(f"{role}: {content}")

        hist_text = "\n".join(history) if history else "Sem histórico de mensagens anteriores."

        objective_block = f"\n\nObjetivo desta mensagem:\n{objective}" if objective.strip() else ""

        system = f"""{company_prompt}

Você vai retomar uma conversa com um cliente que não respondeu há algum tempo. 
Sua tarefa é gerar UMA mensagem de reengajamento personalizada, curta (máx 3 linhas), natural e humana — que faça o cliente querer responder.
A mensagem deve:
- Referenciar o contexto real da última conversa (não ser genérica)
- Soar como uma continuação natural, não como disparo em massa
- Ter um gancho claro: pergunta, proposta ou próximo passo{objective_block}

Responda APENAS com o texto da mensagem, sem aspas, sem explicações."""

        user_msg = f"Nome do cliente: {contact_name or 'Cliente'}\n\nHistórico completo da conversa:\n{hist_text}\n\nGere a mensagem de reengajamento agora."

        msg = await call_ai(system, user_msg, max_tokens=250)
        return msg.replace("{nome}", contact_name or "").strip()
    except Exception as e:
        print(f"[RESUME_CONV error] {e}")
        return f"Olá {contact_name or ''}! Passando para dar continuidade à nossa conversa. Tudo bem? 😊"


async def run_broadcast(broadcast_id: str, interval_min: int, interval_max: int):
    supabase.table("broadcasts").update({"status": "sending", "started_at": datetime.utcnow().isoformat()}).eq("id", broadcast_id).execute()
    bcast = supabase.table("broadcasts").select("*").eq("id", broadcast_id).single().execute().data
    recipients = supabase.table("broadcast_recipients").select("*").eq("broadcast_id", broadcast_id).eq("status", "pending").execute().data
    ai_personalize = bcast.get("ai_personalize", False)
    resume_conversation = bcast.get("resume_conversation", False)
    instance_name = bcast.get("instance_name") or "default"
    sent = 0; failed = 0
    for rec in recipients:
        if sent % 5 == 0:
            bcast_status = supabase.table("broadcasts").select("status").eq("id", broadcast_id).single().execute().data
            if bcast_status and bcast_status["status"] == "cancelled": break

        # Gera mensagem
        if resume_conversation:
            # 3 créditos por contato
            ok_credit, _, err = consume_credit(bcast["tenant_id"], 3)
            if not ok_credit:
                print(f"[BROADCAST] Sem créditos para retomar conversa: {err}")
                supabase.table("broadcast_recipients").update({"status": "failed", "error": f"Sem créditos: {err}"}).eq("id", rec["id"]).execute()
                failed += 1
                continue
            msg = await generate_resume_message(
                bcast["tenant_id"],
                rec.get("conversation_id"),
                rec.get("name") or "",
                rec.get("phone") or "",
                bcast.get("message") or ""
            )
        elif ai_personalize:
            ok_credit, _, err = consume_credit(bcast["tenant_id"], 1)
            if not ok_credit:
                supabase.table("broadcast_recipients").update({"status": "failed", "error": f"Sem créditos: {err}"}).eq("id", rec["id"]).execute()
                failed += 1
                continue
            msg = await generate_personalized_message(
                bcast["tenant_id"],
                rec.get("conversation_id"),
                rec.get("name") or "",
                rec.get("phone") or ""
            )
        else:
            msg = bcast["message"].replace("{nome}", rec.get("name") or "").replace("{telefone}", rec.get("phone") or "")

        ok = False
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post(f"{WAHA_URL}/api/sendText", headers=waha_headers(), json={"session": instance_name, "chatId": f"{rec['phone']}@c.us", "text": msg})
                ok = r.status_code in [200, 201]
        except: pass
        supabase.table("broadcast_recipients").update({"status": "sent" if ok else "failed", "sent_at": datetime.utcnow().isoformat() if ok else None, "sent_message": msg}).eq("id", rec["id"]).execute()
        if ok: sent += 1
        else: failed += 1
        supabase.table("broadcasts").update({"sent_count": sent, "failed_count": failed}).eq("id", broadcast_id).execute()
        await asyncio.sleep(random.randint(interval_min, interval_max))
    supabase.table("broadcasts").update({"status": "done", "finished_at": datetime.utcnow().isoformat()}).eq("id", broadcast_id).execute()

# ── SCHEDULED MESSAGES ───────────────────────────────────
@app.post("/scheduled-messages")
async def create_scheduled_message(body: ScheduledMessageCreate):
    return {"scheduled_message": supabase.table("scheduled_messages").insert({"tenant_id": body.tenant_id, "conversation_id": body.conversation_id, "contact_name": body.contact_name, "contact_phone": body.contact_phone, "message": body.message, "scheduled_at": body.scheduled_at, "recurrence": body.recurrence, "status": "pending"}).execute().data[0]}

@app.get("/scheduled-messages")
async def list_scheduled_messages(tenant_id: str = Depends(enforce_tenant), status: Optional[str] = None):
    q = supabase.table("scheduled_messages").select("*").eq("tenant_id", tenant_id)
    if status: q = q.eq("status", status)
    return {"scheduled_messages": q.order("scheduled_at").execute().data}

@app.delete("/scheduled-messages/{msg_id}", dependencies=[Depends(verify_key)])
async def delete_scheduled_message(msg_id: str):
    supabase.table("scheduled_messages").delete().eq("id", msg_id).execute()
    return {"ok": True}

# ── TRIAL & BILLING ───────────────────────────────────────
@app.get("/tenant/trial-status")
async def trial_status(tenant_id: str = Depends(enforce_tenant)):
    tenant = supabase.table("tenants").select("id, name, plan, trial_ends_at, trial_used, is_blocked").eq("id", tenant_id).single().execute().data
    now = datetime.utcnow()
    trial_ends_at = tenant.get("trial_ends_at")
    plan = tenant.get("plan", "trial")
    is_blocked = tenant.get("is_blocked", False)
    if plan not in ["trial", None]:
        return {"status": "paid", "plan": plan, "is_blocked": False, "days_left": None, "trial_ends_at": None}
    if not trial_ends_at:
        ends = now + timedelta(days=7)
        supabase.table("tenants").update({"trial_ends_at": ends.isoformat(), "trial_used": True, "plan": "trial"}).eq("id", tenant_id).execute()
        return {"status": "trial", "plan": "trial", "is_blocked": False, "days_left": 7, "trial_ends_at": ends.isoformat()}
    ends_dt = datetime.fromisoformat(trial_ends_at.replace("Z", ""))
    days_left = max(0, (ends_dt - now).days)
    expired = now > ends_dt
    if expired and not is_blocked:
        supabase.table("tenants").update({"is_blocked": True}).eq("id", tenant_id).execute()
    return {"status": "expired" if expired else "trial", "plan": "trial", "is_blocked": expired, "days_left": days_left, "trial_ends_at": trial_ends_at}

@app.post("/tenant/activate-plan", dependencies=[Depends(verify_key)])
async def activate_plan(body: dict, admin=Depends(require_super_admin)):
    tenant_id = body.get("tenant_id")
    plan = body.get("plan")
    if plan not in ["starter", "pro", "business"]:
        raise HTTPException(status_code=400, detail="Plano inválido")
    supabase.table("tenants").update({"plan": plan, "is_blocked": False, "activated_at": datetime.utcnow().isoformat()}).eq("id", tenant_id).execute()
    return {"ok": True, "plan": plan}

# ── ONBOARDING INTELIGENTE ────────────────────────────────
@app.get("/onboarding/conv-count", dependencies=[Depends(verify_key)])
async def onboarding_conv_count(tenant_id: str, instance_name: str = None):
    """Retorna quantas conversas com conteúdo existem para uma instância — usado para mostrar indicador de qualidade."""
    try:
        q = supabase.table("conversations").select("id", count="exact").eq("tenant_id", tenant_id)
        if instance_name:
            q = q.eq("instance_name", instance_name)
        result = q.execute()
        total = result.count or 0
        # Qualidade: 0% a 100% baseado em 50 conversas
        quality = min(100, round((total / 50) * 100))
        label = "Ruim" if quality < 20 else "Básico" if quality < 50 else "Bom" if quality < 80 else "Ótimo"
        return {"total": total, "quality_pct": quality, "quality_label": label, "target": 50}
    except Exception as e:
        return {"total": 0, "quality_pct": 0, "quality_label": "Sem dados", "target": 50}

@app.post("/onboarding/analyze", dependencies=[Depends(verify_key)])
async def onboarding_analyze(body: dict):
    tenant_id = body.get("tenant_id")
    days = body.get("days", 90)
    instance_name = body.get("instance_name")  # OBRIGATÓRIO — cada número é independente
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="Anthropic API key não configurada")
    tenant = supabase.table("tenants").select("id, name, plan, copilot_prompt, onboarding_last_run").eq("id", tenant_id).single().execute().data
    plan = tenant.get("plan")
    # Todos os planos recebem o mesmo prompt de máxima qualidade
    conv_limit = 300
    # Check and consume 1000 credits — no monthly limit, use as many times as you have credits
    ok, remaining, err = consume_credit(tenant_id, 1000)
    if not ok: raise HTTPException(status_code=402, detail=f"São necessários 1.000 créditos para esta análise. {err}")
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    # Filtrar por instância — cada número é independente
    q = supabase.table("conversations").select("id, contacts(name, phone)").eq("tenant_id", tenant_id).gte("created_at", since)
    if instance_name:
        q = q.eq("instance_name", instance_name)
    conversations = q.limit(conv_limit).execute().data
    if not conversations:
        raise HTTPException(status_code=404, detail=f"Nenhuma conversa encontrada{' para este número' if instance_name else ''} nos últimos {days} dias.")
    supabase.table("tenants").update({"onboarding_last_run": datetime.utcnow().isoformat()}).eq("id", tenant_id).execute()
    all_samples = []
    for conv in conversations[:conv_limit]:
        msgs = supabase.table("messages").select("direction, content").eq("conversation_id", conv["id"]).eq("is_internal_note", False).order("created_at").limit(30).execute().data
        if len(msgs) < 2: continue
        contact_name = (conv.get("contacts") or {}).get("name", "Cliente")
        sample = f"[Conversa com {contact_name}]\n"
        for m in msgs:
            role = "Atendente" if m["direction"] == "outbound" else "Cliente"
            sample += f"{role}: {m['content']}\n"
        all_samples.append(sample)
    if not all_samples:
        raise HTTPException(status_code=404, detail="Nenhuma conversa com conteúdo suficiente.")
    combined = "\n---\n".join(all_samples[:conv_limit])
    if len(combined) > 300000: combined = combined[:300000]
    total_convs = len(all_samples)
    analysis_prompt = f"""Você é um especialista em atendimento ao cliente e CRM.\n\nAnalise as conversas abaixo da empresa "{tenant['name']}" e gere um prompt de sistema detalhado para um Co-pilot de IA.\n\nO prompt deve incluir:\n1. Tom de voz\n2. Produtos/serviços\n3. Perguntas frequentes\n4. Fluxo de vendas\n5. Regras importantes\n6. Instruções para o Co-pilot\n\nCONVERSAS ({total_convs} conversas, últimos {days} dias):\n\n{combined}\n\nPROMPT GERADO:"""
    # Onboarding usa GPT-4o Mini (mais barato, suficiente para análise de histórico)
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=503, detail="OPENAI_API_KEY não configurada no servidor")
    generated_prompt = await call_ai(
        "Você é um especialista em atendimento ao cliente e CRM. Analise conversas e gere prompts de sistema detalhados.",
        analysis_prompt,
        max_tokens=3000,
        prefer_openai=True,
        timeout=90
    )

    # Save real prompt to DB — per instance if instance_name provided, otherwise fallback to tenant
    if instance_name:
        supabase.table("gateway_instances").update({
            "copilot_prompt": generated_prompt
        }).eq("tenant_id", tenant_id).eq("instance_name", instance_name).execute()
    else:
        supabase.table("tenants").update({
            "copilot_prompt": generated_prompt,
            "onboarding_last_run": datetime.utcnow().isoformat()
        }).eq("id", tenant_id).execute()

    # Generate a summary (shown to user — protects IP)
    summary_prompt = f"""Baseado no prompt abaixo, gere um resumo executivo em bullet points para mostrar ao usuário o que a IA aprendeu sobre o negócio dele.

REGRAS DO RESUMO:
- Máximo 6 bullet points
- Cada bullet = 1 linha curta
- NÃO revelar instruções técnicas, regras de sistema ou estrutura do prompt
- Apenas mostrar: tom de voz, principais assuntos, produtos/serviços identificados, estilo de atendimento
- Formato: "• [item]"
- Escreva em português

PROMPT (NÃO REVELAR):
{generated_prompt[:2000]}

RESUMO:"""

    summary = await call_ai(
        "Você cria resumos concisos em bullet points. Nunca revele o conteúdo original.",
        summary_prompt, max_tokens=300
    )

    # Save summary too
    supabase.table("tenants").update({"copilot_prompt_summary": summary}).eq("id", tenant_id).execute()

    return {"summary": summary, "conversations_analyzed": total_convs, "days_analyzed": days, "tenant_name": tenant["name"], "credits_remaining": remaining}

@app.post("/onboarding/save-prompt", dependencies=[Depends(verify_key)])
async def onboarding_save_prompt(body: dict, admin=Depends(require_admin)):
    tenant_id = body.get("tenant_id")
    prompt = body.get("prompt")
    instance_name = body.get("instance_name")
    if instance_name:
        supabase.table("gateway_instances").update({"copilot_prompt": prompt}).eq("instance_name", instance_name).eq("tenant_id", tenant_id).execute()
    else:
        supabase.table("tenants").update({"copilot_prompt": prompt, "onboarding_done": True, "updated_at": datetime.utcnow().isoformat()}).eq("id", tenant_id).execute()
    return {"ok": True}

@app.post("/onboarding/questionnaire", dependencies=[Depends(verify_key)])
async def onboarding_questionnaire(body: dict, admin=Depends(require_admin)):
    """Gera prompt do Co-pilot via questionário guiado — não precisa de histórico de conversas."""
    tenant_id = body.get("tenant_id")
    answers = body.get("answers", {})  # dict com as respostas do formulário

    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="Anthropic API key não configurada")
    if not answers:
        raise HTTPException(status_code=400, detail="Respostas do questionário são obrigatórias")

    tenant = supabase.table("tenants").select("id, name, plan, ai_credits").eq("id", tenant_id).single().execute().data
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant não encontrado")

    # Questionário é gratuito — faz parte do onboarding, não consome créditos

    # Monta contexto das respostas
    empresa         = answers.get("empresa", "")
    segmento        = answers.get("segmento", "")
    tom             = answers.get("tom", "")
    produtos        = answers.get("produtos", "")
    duvidas_comuns  = answers.get("duvidas_comuns", "")
    regras          = answers.get("regras", "")
    objetivo        = answers.get("objetivo", "")

    user_prompt = f"""Você é um especialista em atendimento ao cliente e CRM.

Com base nas informações abaixo fornecidas pelo dono da empresa, gere um prompt de sistema completo e detalhado para um Co-pilot de IA que vai sugerir respostas para os atendentes desta empresa no WhatsApp.

INFORMAÇÕES DA EMPRESA:
- Nome / Segmento: {empresa} — {segmento}
- Tom de voz desejado: {tom}
- Produtos / Serviços oferecidos: {produtos}
- Dúvidas mais comuns dos clientes: {duvidas_comuns}
- Regras importantes de atendimento: {regras}
- Principal objetivo do atendimento: {objetivo}

INSTRUÇÕES PARA O PROMPT GERADO:
1. Escreva na perspectiva de instruções para a IA (ex: "Você é o assistente de atendimento da empresa X...")
2. Inclua tom de voz, como cumprimentar, como se despedir
3. Liste os produtos/serviços com detalhes úteis para o atendente
4. Inclua respostas sugeridas para as dúvidas mais comuns
5. Deixe claro as regras e limites do atendimento
6. Instrua a IA a sempre perguntar o nome do cliente se não souber
7. O prompt deve ser em português brasileiro
8. Seja específico e prático — o Co-pilot vai usar isso para sugerir respostas reais

PROMPT DO CO-PILOT:"""

    generated_prompt = await call_ai(
        "Você é especialista em criar prompts de sistema para assistentes de atendimento ao cliente via WhatsApp. Seus prompts são específicos, práticos e em português brasileiro.",
        user_prompt,
        max_tokens=2000,
        timeout=60
    )

    # Salva o prompt no banco
    supabase.table("tenants").update({
        "copilot_prompt": generated_prompt,
        "onboarding_done": True,
        "onboarding_last_run": datetime.utcnow().isoformat()
    }).eq("id", tenant_id).execute()

    # Gera resumo para mostrar ao usuário
    summary_prompt = f"""Baseado no prompt abaixo, gere um resumo executivo em bullet points para mostrar ao usuário o que a IA vai fazer pelo atendimento dele.

REGRAS:
- Máximo 5 bullet points curtos
- Tom positivo e animador
- NÃO revelar instruções técnicas do prompt
- Mostre: tom de voz, assuntos que sabe responder, o que a IA vai fazer
- Formato: "• [item]"
- Em português

PROMPT:
{generated_prompt[:1500]}

RESUMO:"""

    summary = await call_ai(
        "Você cria resumos concisos em bullet points.",
        summary_prompt,
        max_tokens=200
    )

    supabase.table("tenants").update({"copilot_prompt_summary": summary}).eq("id", tenant_id).execute()

    return {
        "ok": True,
        "summary": summary
    }


# ── PLAN FEATURES ────────────────────────────────────────
PLAN_FEATURES = {
    "trial":     {"agents": 3,   "numbers": 1,   "ai_credits": 1000,  "disparos": True,  "copilot": True,  "onboarding": True,  "white_label": False},
    "starter":   {"agents": 3,   "numbers": 1,   "ai_credits": 0,     "disparos": True,  "copilot": False, "onboarding": False, "white_label": False},
    "pro":       {"agents": 8,   "numbers": 2,   "ai_credits": 300,   "disparos": True,  "copilot": True,  "onboarding": True,  "white_label": False},
    "business":  {"agents": 20,  "numbers": 5,   "ai_credits": 1000,  "disparos": True,  "copilot": True,  "onboarding": True,  "white_label": False},
    "enterprise":{"agents": 999, "numbers": 999, "ai_credits": 99999, "disparos": True,  "copilot": True,  "onboarding": True,  "white_label": True},
}

@app.get("/plan/features")
async def get_plan_features(tenant_id: str = Depends(enforce_tenant)):
    tenant = supabase.table("tenants").select("plan").eq("id", tenant_id).single().execute().data
    plan = tenant.get("plan", "trial")
    return {"plan": plan, "features": PLAN_FEATURES.get(plan, PLAN_FEATURES["starter"])}

# ── SYNC HISTÓRICO ────────────────────────────────────────



@app.post("/whatsapp/fix-webhook", dependencies=[Depends(verify_key)])
async def fix_webhook(body: dict):
    """Força reconfiguração do webhook do WAHA para uma instância específica."""
    instance_name = body.get("instance_name")
    if not instance_name:
        raise HTTPException(400, "instance_name required")
    webhook_url = f"{BACKEND_URL}/webhook/message"
    result = {}
    async with httpx.AsyncClient(timeout=15) as client:
        # 1. Check current session status
        try:
            r = await client.get(f"{WAHA_URL}/api/sessions/{instance_name}", headers=waha_headers())
            result["session_before"] = r.json() if r.status_code == 200 else r.text[:200]
        except Exception as e:
            result["session_check_error"] = str(e)

        # 2. Force update webhook via PUT
        webhook_payload = {"config": {"webhooks": [{
            "url": webhook_url,
            "events": ["message", "message.any", "session.status"],
            "customHeaders": [{"name": "x-api-key", "value": WEBHOOK_SECRET}]
        }]}}
        for put_url in [
            f"{WAHA_URL}/api/sessions/{instance_name}",
            f"{WAHA_URL}/api/session/{instance_name}",
        ]:
            try:
                r = await client.put(put_url, headers=waha_headers(), json=webhook_payload)
                result[f"PUT {put_url.split('/')[-2]}/{put_url.split('/')[-1]}"] = {"status": r.status_code, "body": r.text[:300]}
                if r.status_code in (200, 201, 204):
                    result["webhook_configured"] = True
                    break
            except Exception as e:
                result[f"PUT_error"] = str(e)

        # 3. Verify after update
        try:
            r2 = await client.get(f"{WAHA_URL}/api/sessions/{instance_name}", headers=waha_headers())
            after = r2.json() if r2.status_code == 200 else r2.text[:200]
            result["session_after"] = after
            if isinstance(after, dict):
                webhooks = after.get("config", {}).get("webhooks", [])
                result["webhook_url_now"] = webhooks[0].get("url") if webhooks else "none"
                result["webhook_ok"] = any(w.get("url") == webhook_url for w in webhooks)
        except Exception as e:
            result["verify_error"] = str(e)

    result["expected_webhook_url"] = webhook_url
    result["webhook_secret_prefix"] = WEBHOOK_SECRET[:4] + "..."
    return result

@app.get("/whatsapp/debug-session", dependencies=[Depends(verify_key)])
async def debug_waha_session(instance: str):
    """Diagnóstico completo de uma sessão WAHA — testa todas as rotas possíveis"""
    results = {}
    async with httpx.AsyncClient(timeout=15) as client:
        # 1. Session status
        for route in [f"{WAHA_URL}/api/sessions/{instance}", f"{WAHA_URL}/api/session?session={instance}"]:
            try:
                r = await client.get(route, headers=waha_headers())
                results[f"session_status ({route.split('/')[-1]})"] = {"status": r.status_code, "body": r.json() if r.status_code == 200 else r.text[:300]}
                if r.status_code == 200: break
            except Exception as e:
                results[f"session_status"] = {"error": str(e)}

        # 2. Chats
        chat_routes = [
            f"{WAHA_URL}/api/{instance}/chats",
            f"{WAHA_URL}/api/chats?session={instance}",
            f"{WAHA_URL}/api/chats?session={instance}&limit=5",
            f"{WAHA_URL}/api/{instance}/chats?limit=5",
        ]
        for route in chat_routes:
            try:
                r = await client.get(route, headers=waha_headers(), timeout=10)
                body = r.json() if r.status_code == 200 else r.text[:200]
                count = len(body) if isinstance(body, list) else "N/A"
                results[f"chats [{route.split('waha')[1] if 'waha' in route else route[-40:]}]"] = {
                    "status": r.status_code, "count": count, "sample": body[:2] if isinstance(body, list) else body
                }
                if r.status_code == 200 and isinstance(body, list) and body:
                    break
            except Exception as e:
                results[f"chats_error"] = str(e)

        # 3. Messages
        msg_routes = [
            f"{WAHA_URL}/api/messages?session={instance}&limit=5&downloadMedia=false",
            f"{WAHA_URL}/api/{instance}/messages?limit=5",
        ]
        for route in msg_routes:
            try:
                r = await client.get(route, headers=waha_headers(), timeout=10)
                body = r.json() if r.status_code == 200 else r.text[:200]
                count = len(body) if isinstance(body, list) else "N/A"
                results[f"messages [{route[-50:]}]"] = {
                    "status": r.status_code, "count": count, "sample": body[:1] if isinstance(body, list) else body
                }
            except Exception as e:
                results[f"messages_error"] = str(e)

        # 4. All sessions list
        try:
            r = await client.get(f"{WAHA_URL}/api/sessions", headers=waha_headers())
            sessions = r.json() if r.status_code == 200 else []
            match = [s for s in (sessions if isinstance(sessions, list) else []) if s.get("name") == instance]
            results["all_sessions_match"] = match[0] if match else {"not_found": True, "all_names": [s.get("name") for s in (sessions if isinstance(sessions, list) else [])]}
        except Exception as e:
            results["all_sessions_error"] = str(e)

    return {"instance": instance, "waha_url": WAHA_URL, "diagnosis": results}

# ── DEEP SYNC PROGRESSIVO ─────────────────────────────────────────────────────
# Fluxo:
#   Estágio 1 (rápido ~5s): busca todos os chats → cria contatos + conversas no banco
#   Estágio 2 (background): para cada conversa, busca 500 mensagens (10 por vez)
# O frontend atualiza em tempo real via Supabase Realtime.

async def _deep_sync_progressive(tenant_id: str, instance: str):
    """Deep sync em 2 estágios: contatos primeiro, mensagens depois aos poucos."""
    sync_key = f"deep_sync:{tenant_id}:{instance}"
    try:
        r = _get_redis()
        if r:
            r.set(sync_key, json.dumps({"stage": 1, "chats_total": 0, "chats_done": 0,
                "contacts_created": 0, "conversations_created": 0,
                "messages_saved": 0, "status": "running"}), ex=3600)

        tenant = supabase.table("tenants").select("plan").eq("id", tenant_id).single().execute().data
        plan = tenant.get("plan", "starter") if tenant else "starter"
        plan_days = {"starter": 30, "pro": 90, "business": 180, "trial": 90, "enterprise": 365}
        days_limit = plan_days.get(plan, 30)
        since_ts = int((datetime.utcnow() - timedelta(days=days_limit)).timestamp() * 1000)

        # ── ESTÁGIO 1: Busca todos os chats e cria contatos + conversas ─────────
        print(f"[DEEP-SYNC] Estágio 1: buscando chats para {instance}")
        chats = []
        async with httpx.AsyncClient(timeout=30) as client:
            for route in [
                f"{WAHA_URL}/api/{instance}/chats?limit=200",
                f"{WAHA_URL}/api/{instance}/chats",
                f"{WAHA_URL}/api/chats?session={instance}&limit=200",
            ]:
                try:
                    r2 = await client.get(route, headers=waha_headers(), timeout=20)
                    if r2.status_code == 200:
                        data = r2.json()
                        chats = data if isinstance(data, list) else data.get("chats", data.get("data", []))
                        if chats:
                            print(f"[DEEP-SYNC] {len(chats)} chats via {route}")
                            break
                except Exception as e:
                    print(f"[DEEP-SYNC] Route {route} failed: {e}")

        # Filtra apenas @lid (Facebook-linked) — grupos agora são incluídos
        chats = [c for c in chats if "@lid" not in str(c.get("id",""))]
        chats = sorted(chats, key=lambda c: c.get("timestamp", 0), reverse=True)

        stats = {"chats_total": len(chats), "chats_done": 0,
                 "contacts_created": 0, "conversations_created": 0, "messages_saved": 0}

        # Cria todos os contatos e conversas sem mensagens (rápido)
        conv_queue = []  # lista de (conv_id, chat_id, contact_name) para buscar mensagens depois
        for chat in chats:
            try:
                raw_id = chat.get("id", "")
                phone_raw = raw_id.get("user", "") if isinstance(raw_id, dict) else str(raw_id)
                is_chat_group = (chat.get("isGroup") or "@g.us" in str(raw_id) or
                                 (isinstance(raw_id, dict) and raw_id.get("server") == "g.us"))

                if is_chat_group:
                    # Para grupos: phone = "groupid@g.us", chat_id = "groupid@g.us"
                    if isinstance(raw_id, dict):
                        group_raw = raw_id.get("user", "") or raw_id.get("_serialized", "")
                    else:
                        group_raw = str(raw_id).replace("@g.us", "")
                    phone = f"{group_raw}@g.us"
                    chat_id = phone
                else:
                    phone = "".join(c for c in phone_raw if c.isdigit())
                    chat_id = f"{phone}@c.us"

                if not phone or len(phone) < 4:
                    continue
                name = chat.get("name") or chat.get("subject") or ""
                # Para grupos sem nome, busca subject via API de grupos
                if not name and is_chat_group:
                    try:
                        async with httpx.AsyncClient(timeout=5) as _gc:
                            _gr = await _gc.get(f"{WAHA_URL}/api/{instance}/groups/{phone}", headers=waha_headers())
                            if _gr.status_code == 200 and _gr.text.strip():
                                name = _gr.json().get("subject", "") or ""
                    except: pass
                if not name:
                    name = phone

                # Upsert contact
                existing = supabase.table("contacts").select("id,name").eq("tenant_id", tenant_id).eq("phone", phone).execute().data
                if existing:
                    contact_id = existing[0]["id"]
                    if name and name != phone and not existing[0].get("name"):
                        supabase.table("contacts").update({"name": name}).eq("id", contact_id).execute()
                else:
                    try:
                        contact_id = supabase.table("contacts").insert({"tenant_id": tenant_id, "phone": phone, "name": name}).execute().data[0]["id"]
                        stats["contacts_created"] += 1
                    except Exception as dup:
                        if "duplicate" in str(dup).lower() or "23505" in str(dup):
                            ex2 = supabase.table("contacts").select("id").eq("tenant_id", tenant_id).eq("phone", phone).single().execute().data
                            contact_id = ex2["id"] if ex2 else None
                        else:
                            continue
                if not contact_id:
                    continue

                # Upsert conversation
                existing_conv = supabase.table("conversations").select("id,instance_name").eq("tenant_id", tenant_id).eq("contact_id", contact_id).execute().data
                if existing_conv:
                    conv_id = existing_conv[0]["id"]
                    if not existing_conv[0].get("instance_name"):
                        supabase.table("conversations").update({"instance_name": instance}).eq("id", conv_id).execute()
                else:
                    last_ts = chat.get("timestamp", 0)
                    last_msg_at = datetime.utcfromtimestamp(last_ts).isoformat() if last_ts else None
                    conv_row = {"tenant_id": tenant_id, "contact_id": contact_id,
                                "status": "open", "kanban_stage": "new",
                                "unread_count": chat.get("unreadCount", 0),
                                "instance_name": instance}
                    if last_msg_at:
                        conv_row["last_message_at"] = last_msg_at
                    if is_chat_group:
                        conv_row["is_group"] = True
                    conv_id = supabase.table("conversations").insert(conv_row).execute().data[0]["id"]
                    stats["conversations_created"] += 1

                conv_queue.append((conv_id, chat_id, name))
            except Exception as e:
                print(f"[DEEP-SYNC] Erro ao criar contato/conversa: {e}")
                continue

        print(f"[DEEP-SYNC] Estágio 1 concluído: {stats['contacts_created']} contatos, {stats['conversations_created']} conversas")

        # Atualiza Redis: aguardando estágio 2
        # Usuário já vê todos os contatos no inbox nesse momento ✅
        rr = _get_redis()
        if rr:
            rr.set(sync_key, json.dumps({"stage": 1, **stats, "status": "waiting_messages"}), ex=3600)

        # Aguarda 10s — deixa WAHA estabilizar antes de buscar mensagens
        print(f"[DEEP-SYNC] Contatos visíveis no inbox. Aguardando 10s para iniciar busca de mensagens...")
        await asyncio.sleep(10)

        # Atualiza Redis: estágio 2 começa
        rr = _get_redis()
        if rr:
            rr.set(sync_key, json.dumps({"stage": 2, **stats, "status": "running"}), ex=3600)

        # ── ESTÁGIO 2: Busca mensagens em lotes de 10 conversas ──────────────────
        print(f"[DEEP-SYNC] Estágio 2: buscando mensagens de {len(conv_queue)} conversas")
        BATCH = 10
        for batch_start in range(0, len(conv_queue), BATCH):
            batch = conv_queue[batch_start:batch_start + BATCH]
            async with httpx.AsyncClient(timeout=30) as client:
                for conv_id, chat_id, name in batch:
                    try:
                        msgs_data = []
                        for msg_route in [
                            f"{WAHA_URL}/api/{instance}/chats/{chat_id}/messages?limit=500&downloadMedia=false",
                            f"{WAHA_URL}/api/messages?session={instance}&chatId={chat_id}&limit=500&downloadMedia=false",
                        ]:
                            try:
                                mr = await client.get(msg_route, headers=waha_headers(), timeout=20)
                                if mr.status_code == 400:
                                    break  # NOWEB store não habilitado
                                if mr.status_code == 200:
                                    md = mr.json()
                                    msgs_data = md if isinstance(md, list) else md.get("messages", md.get("data", []))
                                    if msgs_data:
                                        break
                            except:
                                continue

                        if not msgs_data:
                            stats["chats_done"] += 1
                            continue

                        # Dedup com mensagens já existentes
                        existing_msgs = supabase.table("messages").select("waha_id").eq("conversation_id", conv_id).execute().data or []
                        existing_ids = {m["waha_id"] for m in existing_msgs if m.get("waha_id")}

                        to_insert = []
                        for msg in msgs_data:
                            try:
                                msg_ts = msg.get("timestamp", 0) or msg.get("t", 0)
                                if msg_ts and msg_ts * 1000 < since_ts:
                                    continue
                                ext_id = msg.get("id") or msg.get("_serialized") or ""
                                if isinstance(ext_id, dict):
                                    ext_id = ext_id.get("_serialized", ext_id.get("id", ""))
                                if ext_id and ext_id in existing_ids:
                                    continue
                                body_text = msg.get("body") or msg.get("text") or msg.get("caption") or ""
                                type_map = {"chat": "text", "image": "image", "audio": "audio",
                                            "ptt": "audio", "video": "video", "document": "document", "sticker": "image"}
                                our_type = type_map.get(msg.get("type", "chat"), "text")
                                from_me = msg.get("fromMe", False) or msg.get("from_me", False)
                                created_at = (datetime.utcfromtimestamp(msg_ts).isoformat() + "+00:00") if msg_ts else (datetime.utcnow().isoformat() + "+00:00")
                                row = {"conversation_id": conv_id, "tenant_id": tenant_id,
                                       "direction": "outbound" if from_me else "inbound",
                                       "content": body_text, "type": our_type, "created_at": created_at}
                                if ext_id:
                                    row["waha_id"] = ext_id
                                to_insert.append(row)
                            except:
                                continue

                        # Insere em chunks de 50
                        for i in range(0, len(to_insert), 50):
                            chunk = to_insert[i:i+50]
                            try:
                                supabase.table("messages").insert(chunk).execute()
                                stats["messages_saved"] += len(chunk)
                            except:
                                pass

                        stats["chats_done"] += 1
                    except Exception as e:
                        print(f"[DEEP-SYNC] Erro ao buscar mensagens de {chat_id}: {e}")
                        stats["chats_done"] += 1
                        continue

            # Atualiza progresso no Redis após cada lote
            rr = _get_redis()
            if rr:
                rr.set(sync_key, json.dumps({"stage": 2, **stats, "status": "running"}), ex=3600)
            print(f"[DEEP-SYNC] Lote {batch_start//BATCH + 1}: {stats['chats_done']}/{stats['chats_total']} conversas, {stats['messages_saved']} mensagens")

            # Pequena pausa entre lotes para não sobrecarregar o WAHA
            await asyncio.sleep(1)

        # Finalizado
        rr = _get_redis()
        if rr:
            rr.set(sync_key, json.dumps({"stage": 2, **stats, "status": "done"}), ex=3600)
        print(f"[DEEP-SYNC] Concluído: {stats['messages_saved']} mensagens de {stats['chats_done']} conversas")

    except Exception as e:
        print(f"[DEEP-SYNC] Erro geral: {e}")
        rr = _get_redis()
        if rr:
            rr.set(sync_key, json.dumps({"status": "error", "error": str(e)}), ex=3600)


@app.get("/whatsapp/deep-sync-status")
async def deep_sync_status(tenant_id: str, instance: str = "default", user=Depends(get_current_user)):
    """Retorna o progresso do deep sync em tempo real."""
    if user["tenant_id"] != tenant_id and user["role"] != "admin":
        raise HTTPException(403)
    sync_key = f"deep_sync:{tenant_id}:{instance}"
    rr = _get_redis()
    if not rr:
        return {"status": "unavailable", "message": "Redis não disponível"}
    try:
        raw = rr.get(sync_key)
        if not raw:
            return {"status": "idle", "message": "Nenhum sync em andamento"}
        return json.loads(raw)
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/whatsapp/trigger-deep-sync", dependencies=[Depends(verify_key)])
async def trigger_deep_sync_bg(body: dict):
    """Dispara deep sync progressivo como background task — retorna imediatamente."""
    tenant_id = body.get("tenant_id")
    instance  = body.get("instance_name") or body.get("instance", "default")
    if not tenant_id or not instance:
        raise HTTPException(status_code=400, detail="tenant_id e instance obrigatórios")
    asyncio.create_task(_deep_sync_progressive(tenant_id, instance))
    return {"ok": True, "message": f"Deep sync enfileirado para {instance}"}

@app.post("/whatsapp/sync", dependencies=[Depends(verify_key)])
async def whatsapp_sync(body: dict):
    tenant_id = body.get("tenant_id")
    instance  = body.get("instance", "default")
    tenant = supabase.table("tenants").select("plan, name").eq("id", tenant_id).single().execute().data
    plan = tenant.get("plan", "starter")
    plan_days = {"starter": 30, "pro": 90, "business": 180, "trial": 90, "enterprise": 365}
    days_limit = plan_days.get(plan, 30)
    since_ts = int((datetime.utcnow() - timedelta(days=days_limit)).timestamp() * 1000)
    stats = {"chats": 0, "contacts_created": 0, "conversations_created": 0, "messages_saved": 0, "skipped": 0}

    async with httpx.AsyncClient(timeout=60) as client:
        # ── 1. Fetch chats list — tries multiple WAHA routes ──────
        chats = []
        # WAHA Plus: primary route is /api/{session}/chats
        waha_routes = [
            f"{WAHA_URL}/api/{instance}/chats?limit=50",
            f"{WAHA_URL}/api/{instance}/chats",
            f"{WAHA_URL}/api/chats?session={instance}&limit=50",
            f"{WAHA_URL}/api/chats?session={instance}",
        ]
        for route in waha_routes:
            try:
                r = await client.get(route, headers=waha_headers(), timeout=20)
                if r.status_code == 200:
                    data = r.json()
                    chats = data if isinstance(data, list) else data.get("chats", data.get("data", []))
                    if chats:
                        print(f"[SYNC] Got {len(chats)} chats from {route}")
                        break
            except Exception as e:
                print(f"[SYNC] Route {route} failed: {e}")
                continue

        # ── 1b. Fallback: use existing contacts in Supabase ────────
        # If WAHA /api/chats is empty (not available in free version),
        # fall back to syncing messages for contacts already in our DB
        if not chats:
            print(f"[SYNC] WAHA /api/chats returned empty — using Supabase contacts as fallback")
            existing_contacts = supabase.table("contacts").select("id,phone,name").eq("tenant_id", tenant_id).limit(50).execute().data or []
            if existing_contacts:
                # Build fake chat objects from existing contacts so the loop below works
                # Fallback: only sync contacts that already have a conversation for THIS instance
                # Do NOT create new conversations for contacts that haven't messaged this instance
                existing_convs = supabase.table("conversations").select("id,contact_id,contacts(phone,name)").eq("tenant_id", tenant_id).eq("instance_name", instance).execute().data or []
                chats = []
                for conv in existing_convs:
                    phone = (conv.get("contacts") or {}).get("phone", "")
                    name = (conv.get("contacts") or {}).get("name", "")
                    if phone:
                        chats.append({"id": f"{phone}@c.us", "name": name, "timestamp": 0, "_from_db": True})
                print(f"[SYNC] Fallback: syncing {len(chats)} contacts with existing convs for instance {instance}")
            else:
                # Truly nothing to work with — try to fetch recent messages directly from WAHA
                # This handles the case where WAHA has chats but no /api/chats endpoint
                try:
                    r = await client.get(f"{WAHA_URL}/api/messages?session={instance}&limit=200&downloadMedia=false", headers=waha_headers(), timeout=30)
                    if r.status_code == 200:
                        msgs = r.json()
                        if isinstance(msgs, list) and msgs:
                            # Extract unique chat IDs from messages
                            seen = set()
                            for msg in msgs:
                                cid = msg.get("chatId") or msg.get("from") or msg.get("to", "")
                                if cid and "@c.us" in str(cid) and cid not in seen:
                                    seen.add(cid)
                                    phone = "".join(x for x in str(cid).split("@")[0] if x.isdigit())
                                    if phone and len(phone) >= 8:
                                        chats.append({"id": cid, "name": phone, "timestamp": 0})
                            print(f"[SYNC] Extracted {len(chats)} unique chats from messages endpoint")
                except Exception as e:
                    print(f"[SYNC] Messages fallback failed: {e}")

        if not chats:
            # Return success with 0 stats instead of 502 error
            return {"ok": True, "stats": stats, "note": "WAHA returned no chats. Session may need a few minutes after first connection to load history. Try again in 1-2 min."}
        # Ordena por timestamp desc e limita aos 30 mais recentes para não travar
        chats = sorted(chats, key=lambda c: c.get("timestamp", 0), reverse=True)[:30]
        stats["chats"] = len(chats)

        # ── 2. For each chat: upsert contact + conversation + messages ──
        for chat in chats:
            try:
                raw_id = chat.get("id", "")
                if isinstance(raw_id, dict):
                    phone_raw = raw_id.get("user", ""); is_group = raw_id.get("server", "") == "g.us"
                else:
                    phone_raw = str(raw_id); is_group = "@g.us" in phone_raw
                if is_group or chat.get("isGroup"):
                    # Grupos: phone = "groupid@g.us"
                    if isinstance(raw_id, dict):
                        g_user = raw_id.get("user", "") or raw_id.get("_serialized", "")
                    else:
                        g_user = phone_raw.replace("@g.us", "")
                    phone = f"{g_user}@g.us"
                    chat_id = phone
                else:
                    phone = "".join(c for c in phone_raw if c.isdigit())
                    chat_id = f"{phone}@c.us"
                if not phone or len(phone) < 4: stats["skipped"] += 1; continue
                name = chat.get("name") or chat.get("subject") or ""
                if not name and (is_group or chat.get("isGroup")):
                    try:
                        async with httpx.AsyncClient(timeout=5) as _gc:
                            _gr = await _gc.get(f"{WAHA_URL}/api/{instance}/groups/{phone}", headers=waha_headers())
                            if _gr.status_code == 200 and _gr.text.strip():
                                name = _gr.json().get("subject", "") or ""
                    except: pass
                if not name: name = phone

                # Upsert contact — always update name if it came from WhatsApp
                existing = supabase.table("contacts").select("id,name").eq("tenant_id", tenant_id).eq("phone", phone).execute().data
                if existing:
                    contact_id = existing[0]["id"]
                    # Update name if currently blank or is just the phone number
                    existing_name = existing[0].get("name","")
                    if name and name != phone and (not existing_name or existing_name == phone):
                        supabase.table("contacts").update({"name": name}).eq("id", contact_id).execute()
                else:
                    try:
                        contact_id = supabase.table("contacts").insert({"tenant_id": tenant_id, "phone": phone, "name": name}).execute().data[0]["id"]
                    except Exception as _dup:
                        if "duplicate" in str(_dup).lower() or "23505" in str(_dup):
                            existing = supabase.table("contacts").select("id").eq("tenant_id", tenant_id).eq("phone", phone).single().execute().data
                            contact_id = existing["id"] if existing else None
                        else:
                            raise
                    stats["contacts_created"] += 1

                # Upsert conversation
                existing_conv = supabase.table("conversations").select("id,instance_name").eq("tenant_id", tenant_id).eq("contact_id", contact_id).execute().data
                if existing_conv:
                    conv_id = existing_conv[0]["id"]
                    if not existing_conv[0].get("instance_name"):
                        supabase.table("conversations").update({"instance_name": instance}).eq("id", conv_id).execute()
                else:
                    conv_insert = {"tenant_id": tenant_id, "contact_id": contact_id, "status": "open", "kanban_stage": "new", "unread_count": chat.get("unreadCount", 0), "instance_name": instance}
                    if is_group or chat.get("isGroup"):
                        conv_insert["is_group"] = True
                    conv_id = supabase.table("conversations").insert(conv_insert).execute().data[0]["id"]
                    stats["conversations_created"] += 1
                last_ts = chat.get("timestamp", 0)
                if last_ts:
                    supabase.table("conversations").update({"last_message_at": datetime.utcfromtimestamp(last_ts).isoformat()}).eq("id", conv_id).execute()

                # ── 3. Fetch messages for this chat ──────────────
                msgs_data = []
                for msg_route in [
                    f"{WAHA_URL}/api/{instance}/chats/{chat_id}/messages?limit=100&downloadMedia=false",
                    f"{WAHA_URL}/api/messages?session={instance}&chatId={chat_id}&limit=100&downloadMedia=false",
                    f"{WAHA_URL}/api/chats/{chat_id}/messages?session={instance}&limit=100",
                ]:
                    try:
                        mr = await client.get(msg_route, headers=waha_headers(), timeout=15)
                        if mr.status_code == 400:
                            break  # NOWEB store not enabled — skip silently
                        if mr.status_code == 200:
                            md = mr.json()
                            msgs_data = md if isinstance(md, list) else md.get("messages", md.get("data", []))
                            if msgs_data: break
                    except: continue

                if not msgs_data:
                    continue

                # Get existing message IDs to avoid duplicates
                existing_msg_ids = set()
                existing_msgs = supabase.table("messages").select("waha_id").eq("conversation_id", conv_id).execute().data
                existing_msg_ids = {m["waha_id"] for m in existing_msgs if m.get("waha_id")}

                to_insert = []
                for msg in msgs_data:
                    try:
                        msg_ts = msg.get("timestamp", 0) or msg.get("t", 0)
                        # Skip messages older than plan limit
                        if msg_ts and msg_ts * 1000 < since_ts: continue
                        ext_id = msg.get("id") or msg.get("_serialized") or ""
                        if isinstance(ext_id, dict): ext_id = ext_id.get("_serialized", ext_id.get("id", ""))
                        if ext_id and ext_id in existing_msg_ids: continue

                        body_text = msg.get("body") or msg.get("text") or msg.get("caption") or ""
                        msg_type = msg.get("type", "chat")
                        # Map WAHA types to our types
                        type_map = {"chat": "text", "image": "image", "audio": "audio", "ptt": "audio",
                                    "video": "video", "document": "document", "sticker": "image"}
                        our_type = type_map.get(msg_type, "text")
                        # Determine direction
                        from_me = msg.get("fromMe", False) or msg.get("from_me", False)
                        direction = "outbound" if from_me else "inbound"
                        created_at = (datetime.utcfromtimestamp(msg_ts).isoformat() + "+00:00") if msg_ts else (datetime.utcnow().isoformat() + "+00:00")

                        row = {
                            "conversation_id": conv_id,
                            "tenant_id": tenant_id,
                            "direction": direction,
                            "content": body_text,
                            "type": our_type,
                            "created_at": created_at,
                        }
                        if ext_id:
                            row["external_id"] = ext_id
                        to_insert.append(row)
                    except: continue

                # Batch insert in chunks of 50
                if to_insert:
                    for i in range(0, len(to_insert), 50):
                        chunk = to_insert[i:i+50]
                        try:
                            supabase.table("messages").insert(chunk).execute()
                            stats["messages_saved"] += len(chunk)
                        except: pass

            except: stats["skipped"] += 1; continue

    return {"ok": True, "stats": stats}

@app.get("/whatsapp/sync-status")
async def sync_status(tenant_id: str = Depends(enforce_tenant)):
    convs = supabase.table("conversations").select("id", count="exact").eq("tenant_id", tenant_id).execute()
    msgs = supabase.table("messages").select("id", count="exact").in_("conversation_id", [c["id"] for c in (supabase.table("conversations").select("id").eq("tenant_id", tenant_id).execute().data or [])]).execute()
    return {"conversations": convs.count, "messages": msgs.count}

@app.post("/whatsapp/backfill-instances", dependencies=[Depends(verify_key)])
async def backfill_instances(body: dict):
    """
    Backfill instance_name on existing conversations that have it null.
    Queries WAHA for each instance's chats and matches by phone number.
    Call once per instance: { tenant_id, instance }
    """
    tenant_id = body.get("tenant_id")
    instance  = body.get("instance", "default")
    updated = 0
    async with httpx.AsyncClient(timeout=60) as client:
        chats = []
        for route in [f"{WAHA_URL}/api/{instance}/chats", f"{WAHA_URL}/api/chats?session={instance}"]:
            try:
                r = await client.get(route, headers=waha_headers(), timeout=15)
                if r.status_code == 200:
                    data = r.json()
                    chats = data if isinstance(data, list) else data.get("chats", data.get("data", []))
                    if chats: break
            except: continue
    if not chats:
        raise HTTPException(status_code=502, detail="Não foi possível buscar chats do WAHA.")
    for chat in chats:
        try:
            raw_id = chat.get("id", "")
            if isinstance(raw_id, dict):
                phone_raw = raw_id.get("user", ""); is_group = raw_id.get("server", "") == "g.us"
            else:
                phone_raw = str(raw_id); is_group = "@g.us" in phone_raw
            if is_group or chat.get("isGroup"):
                g_user = phone_raw.replace("@g.us", "") if not isinstance(raw_id, dict) else raw_id.get("user", phone_raw)
                phone = f"{g_user}@g.us"
            else:
                phone = "".join(c for c in phone_raw if c.isdigit())
            if not phone or len(phone) < 4: continue
            # Find contact then conversation
            _ctr = supabase.table("contacts").select("id").eq("tenant_id", tenant_id).eq("phone", phone).limit(1).execute().data
            contact = _ctr[0] if _ctr else None
            if not contact: continue
            _cvr = supabase.table("conversations").select("id,instance_name").eq("tenant_id", tenant_id).eq("contact_id", contact["id"]).limit(1).execute().data
            conv = _cvr[0] if _cvr else None
            if not conv: continue
            if not conv.get("instance_name"):
                supabase.table("conversations").update({"instance_name": instance}).eq("id", conv["id"]).execute()
                updated += 1
        except: continue
    return {"ok": True, "instance": instance, "chats_checked": len(chats), "updated": updated}

# ── DEEP SYNC — importa TODO histórico do WAHA store página por página ────────
@app.post("/whatsapp/deep-sync", dependencies=[Depends(verify_key)])
async def deep_sync(body: dict):
    """
    Varre todos os chats do WAHA store e importa TODAS as mensagens para o banco.
    Busca página por página (100 msgs por vez) até zerar cada chat.
    Ideal para rodar após reconexão com fullSync.
    { tenant_id, instance }
    """
    tenant_id = body.get("tenant_id")
    instance  = body.get("instance")
    if not tenant_id or not instance:
        raise HTTPException(status_code=400, detail="tenant_id e instance obrigatórios")

    stats = {"chats": 0, "conversations_created": 0, "contacts_created": 0, "messages_saved": 0, "errors": 0}

    async with httpx.AsyncClient(timeout=60) as client:
        # 1. Buscar todos os chats do store
        chats = []
        for route in [
            f"{WAHA_URL}/api/{instance}/chats?limit=500",
            f"{WAHA_URL}/api/chats?session={instance}&limit=500",
        ]:
            try:
                r = await client.get(route, headers=waha_headers(), timeout=20)
                if r.status_code == 200:
                    data = r.json()
                    chats = data if isinstance(data, list) else data.get("chats", data.get("data", []))
                    if chats: break
            except: continue

        if not chats:
            return {"ok": True, "stats": stats, "message": "Store vazio — fullSync ainda em andamento"}

        stats["chats"] = len(chats)

        def _get_or_create_contact(phone_raw: str, name: str):
            # Normalize phone
            if "@lid" in phone_raw:
                phone = phone_raw  # keep @lid
            else:
                phone = phone_raw.replace("@s.whatsapp.net","").replace("@c.us","")
            existing = supabase.table("contacts").select("id").eq("tenant_id", tenant_id).eq("phone", phone).limit(1).execute().data
            if existing:
                return existing[0]["id"], False
            new_c = supabase.table("contacts").insert({"tenant_id": tenant_id, "phone": phone, "name": name or phone}).execute().data
            return new_c[0]["id"], True

        def _get_or_create_conv(contact_id: str):
            existing = supabase.table("conversations").select("id").eq("tenant_id", tenant_id).eq("contact_id", contact_id).eq("instance_name", instance).limit(1).execute().data
            if existing:
                return existing[0]["id"], False
            new_c = supabase.table("conversations").insert({"tenant_id": tenant_id, "contact_id": contact_id, "instance_name": instance, "status": "open", "kanban_stage": "new"}).execute().data
            return new_c[0]["id"], True

        # 2. Para cada chat, buscar mensagens página por página
        for chat in chats:
            chat_id = chat.get("id", "")
            if not chat_id or "@g.us" in chat_id:  # skip grupos
                continue
            chat_name = chat.get("name") or chat_id.split("@")[0]

            try:
                contact_id, c_created = await run_sync(lambda p=chat_id.split("@")[0], n=chat_name: _get_or_create_contact(p, n))
                if c_created: stats["contacts_created"] += 1
                conv_id, cv_created = await run_sync(lambda cid=contact_id: _get_or_create_conv(cid))
                if cv_created: stats["conversations_created"] += 1
            except Exception as e:
                stats["errors"] += 1
                continue

            # Buscar mensagens em páginas de 100
            last_msg_id = None
            total_for_chat = 0
            MAX_PAGES = 50  # máximo 5000 mensagens por chat

            for _ in range(MAX_PAGES):
                try:
                    url = f"{WAHA_URL}/api/{instance}/chats/{chat_id}/messages?limit=100&downloadMedia=false"
                    if last_msg_id:
                        url += f"&before={last_msg_id}"
                    r = await client.get(url, headers=waha_headers(), timeout=30)
                    if r.status_code != 200: break
                    raw = r.json()
                    page_msgs = raw if isinstance(raw, list) else raw.get("messages", raw.get("data", []))
                    if not page_msgs: break

                    # Get existing waha_ids to dedup
                    def _get_existing():
                        return {m["waha_id"] for m in supabase.table("messages").select("waha_id").eq("conversation_id", conv_id).execute().data or [] if m.get("waha_id")}
                    existing_ids = await run_sync(_get_existing)

                    to_insert = []
                    for m in page_msgs:
                        waha_id = m.get("id", "")
                        if isinstance(waha_id, dict): waha_id = waha_id.get("id", waha_id.get("_serialized", ""))
                        if waha_id and waha_id in existing_ids: continue
                        ts = m.get("timestamp", 0)
                        from_me = m.get("fromMe", False)
                        body_text = m.get("body") or m.get("text") or ""
                        if not body_text:
                            t = m.get("type","")
                            body_text = "[Imagem]" if "image" in t else "[Áudio]" if "audio" in t or t=="ptt" else "[Vídeo]" if "video" in t else "[Documento]" if "document" in t else "[Mensagem]"
                        if not body_text: continue
                        to_insert.append({
                            "conversation_id": conv_id,
                            "direction": "outbound" if from_me else "inbound",
                            "content": body_text,
                            "type": "text",
                            "waha_id": waha_id or None,
                            "created_at": datetime.utcfromtimestamp(ts).isoformat() + "+00:00" if ts else None,
                        })

                    if to_insert:
                        def _insert(rows=to_insert):
                            for i in range(0, len(rows), 50):
                                try: supabase.table("messages").insert(rows[i:i+50]).execute()
                                except: pass
                        await run_sync(_insert)
                        stats["messages_saved"] += len(to_insert)
                        total_for_chat += len(to_insert)

                    # Cursor para próxima página
                    last_item = page_msgs[-1]
                    last_id = last_item.get("id","")
                    if isinstance(last_id, dict): last_id = last_id.get("id", last_id.get("_serialized",""))
                    if last_id == last_msg_id or len(page_msgs) < 100: break
                    last_msg_id = last_id

                except Exception as e:
                    stats["errors"] += 1
                    break

            # Atualizar last_message_at e preview da conversa
            if total_for_chat > 0:
                try:
                    last_msg = supabase.table("messages").select("content,created_at").eq("conversation_id", conv_id).order("created_at", desc=True).limit(1).execute().data
                    if last_msg:
                        supabase.table("conversations").update({
                            "last_message_at": last_msg[0]["created_at"],
                            "last_message_preview": last_msg[0]["content"][:100]
                        }).eq("id", conv_id).execute()
                except: pass

    return {"ok": True, "stats": stats}


# ── RESOLVE LIDs — resolve LIDs para números reais e mescla duplicatas ─────────
@app.post("/whatsapp/resolve-lids", dependencies=[Depends(verify_key)])
async def resolve_lids(body: dict):
    """
    1. Detecta LIDs por comprimento do número (>13 dígitos) — não depende de sufixo @lid
       pois o banco armazena os dígitos puros (ex: 237039245074536)
    2. Tenta resolver para número real via WAHA contacts store (testa múltiplos formatos)
    3. Mescla conversas duplicadas do mesmo contato na mesma instância
    { tenant_id, instance }
    """
    tenant_id = body.get("tenant_id")
    instance  = body.get("instance")
    if not tenant_id or not instance:
        raise HTTPException(status_code=400, detail="tenant_id e instance obrigatórios")

    stats = {"lids_found": 0, "lids_resolved": 0, "duplicates_merged": 0, "errors": 0, "detail": []}

    def _is_lid(phone: str) -> bool:
        """LID = número com mais de 13 dígitos OU com sufixo @lid"""
        digits = phone.replace("@lid","").replace("@c.us","").replace("@s.whatsapp.net","").strip()
        return len(digits) > 13 or "@lid" in phone

    # 1. Buscar TODOS os contatos desta instância e filtrar LIDs por tamanho
    def _get_lid_contacts():
        convs = supabase.table("conversations").select(
            "id,contact_id,contacts(id,phone,name)"
        ).eq("tenant_id", tenant_id).eq("instance_name", instance).execute().data or []
        lid_convs = [c for c in convs if _is_lid((c.get("contacts") or {}).get("phone",""))]
        return lid_convs

    lid_convs = await run_sync(_get_lid_contacts)
    stats["lids_found"] = len(lid_convs)
    print(f"[RESOLVE-LIDS] inst={instance} lids_encontrados={len(lid_convs)}")

    # 2. Tentar resolver via WAHA contacts store — testa 3 formatos de chatId
    async with httpx.AsyncClient(timeout=30) as client:
        for conv in lid_convs:
            contact = conv.get("contacts") or {}
            lid_phone = contact.get("phone","")
            lid_digits = lid_phone.replace("@lid","").replace("@c.us","").replace("@s.whatsapp.net","").strip()
            if not lid_digits:
                continue

            resolved = False
            # Tenta os 3 formatos que o WAHA aceita para LID
            candidates = [
                f"{lid_digits}@lid",
                f"{lid_digits}@c.us",
                lid_digits,
            ]

            for chat_id in candidates:
                try:
                    r = await client.get(
                        f"{WAHA_URL}/api/{instance}/contacts/check-exists",
                        params={"phone": chat_id},
                        headers=waha_headers(), timeout=10
                    )
                    if r.status_code == 200:
                        data = r.json()
                        real_phone = (data.get("id") or data.get("jid") or "").replace("@c.us","").replace("@s.whatsapp.net","").strip()
                        name = data.get("name") or data.get("pushName") or contact.get("name","")
                        if real_phone and len(real_phone) <= 13 and real_phone.isdigit():
                            def _update(cid=contact["id"], phone=real_phone, n=name):
                                supabase.table("contacts").update({"phone": phone, "name": n}).eq("id", cid).execute()
                            await run_sync(_update)
                            stats["lids_resolved"] += 1
                            stats["detail"].append({"lid": lid_digits, "resolved_to": real_phone, "name": name})
                            print(f"[RESOLVE-LIDS] ✅ {lid_digits} → {real_phone} ({name})")
                            resolved = True
                            break
                except Exception as e:
                    pass

                # Tenta também endpoint direto /contacts/{chatId}
                if not resolved:
                    try:
                        r2 = await client.get(
                            f"{WAHA_URL}/api/{instance}/contacts/{chat_id}",
                            headers=waha_headers(), timeout=10
                        )
                        if r2.status_code == 200:
                            data2 = r2.json()
                            real_phone = (data2.get("id") or "").replace("@c.us","").replace("@s.whatsapp.net","").strip()
                            name = data2.get("name") or data2.get("pushName") or contact.get("name","")
                            if real_phone and len(real_phone) <= 13 and real_phone.isdigit():
                                def _update2(cid=contact["id"], phone=real_phone, n=name):
                                    supabase.table("contacts").update({"phone": phone, "name": n}).eq("id", cid).execute()
                                await run_sync(_update2)
                                stats["lids_resolved"] += 1
                                stats["detail"].append({"lid": lid_digits, "resolved_to": real_phone, "name": name})
                                print(f"[RESOLVE-LIDS] ✅ {lid_digits} → {real_phone} ({name}) [via /contacts/]")
                                resolved = True
                                break
                    except Exception as e:
                        pass

            if not resolved:
                stats["detail"].append({"lid": lid_digits, "resolved_to": None, "name": contact.get("name","")})
                print(f"[RESOLVE-LIDS] ❌ Não resolveu: {lid_digits} ({contact.get('name','')})")

    # 3. Mesclar conversas duplicadas (mesmo contact_id + instance_name)
    def _find_duplicates():
        convs = supabase.table("conversations").select(
            "id,contact_id,last_message_at,created_at"
        ).eq("tenant_id", tenant_id).eq("instance_name", instance).order("last_message_at", desc=True).execute().data or []
        seen = {}
        dups = []
        for c in convs:
            key = c["contact_id"]
            if key in seen:
                dups.append((seen[key], c["id"]))  # (keep_id, merge_id)
            else:
                seen[key] = c["id"]
        return dups

    duplicates = await run_sync(_find_duplicates)

    for keep_id, merge_id in duplicates:
        try:
            def _merge(kid=keep_id, mid=merge_id):
                supabase.table("messages").update({"conversation_id": kid}).eq("conversation_id", mid).execute()
                try: supabase.table("tasks").update({"conversation_id": kid}).eq("conversation_id", mid).execute()
                except: pass
                supabase.table("conversations").delete().eq("id", mid).execute()
            await run_sync(_merge)
            stats["duplicates_merged"] += 1
            print(f"[RESOLVE-LIDS] 🔀 Merge: {merge_id[:8]} → {keep_id[:8]}")
        except Exception as e:
            stats["errors"] += 1
            print(f"[RESOLVE-LIDS] Erro merge: {e}")

    print(f"[RESOLVE-LIDS] Concluído: {stats}")
    return {"ok": True, "stats": stats}


# ── AUTH — Recuperação de Senha ───────────────────────────
@app.post("/auth/forgot-password")
async def forgot_password(body: dict):
    email = (body.get("email") or "").lower().strip()
    if not email: raise HTTPException(status_code=400, detail="Email obrigatório")
    users = supabase.table("users").select("id,name,email,tenant_id").eq("email", email).eq("is_active", True).execute().data
    if not users: return {"ok": True, "message": "Se o email existir, você receberá as instruções."}
    user = users[0]
    token = "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=48))
    expires_at = (datetime.utcnow() + timedelta(hours=2)).isoformat()
    supabase.table("password_resets").upsert({"user_id": user["id"], "token": token, "expires_at": expires_at, "used": False}, on_conflict="user_id").execute()
    reset_url = f"https://7zap-inbox-frontend.vercel.app/?reset={token}"
    SMTP_HOST = os.getenv("SMTP_HOST", ""); SMTP_USER = os.getenv("SMTP_USER", ""); SMTP_PASS = os.getenv("SMTP_PASS", "")
    email_sent = False
    if SMTP_HOST and SMTP_USER and SMTP_PASS:
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            msg = MIMEMultipart("alternative")
            msg["Subject"] = "Recuperação de senha — 7CRM"
            msg["From"] = f"7CRM <{SMTP_USER}>"; msg["To"] = user["email"]
            html = f'<div style="font-family:sans-serif;padding:32px"><h2>🔐 Recuperar senha</h2><p>Olá, <strong>{user["name"]}</strong>!</p><p>Clique para redefinir sua senha (expira em 2h):</p><a href="{reset_url}" style="background:#00c853;color:#000;padding:14px 28px;border-radius:8px;text-decoration:none;font-weight:700">Redefinir senha</a></div>'
            msg.attach(MIMEText(html, "html"))
            with smtplib.SMTP_SSL(SMTP_HOST, 465) as smtp:
                smtp.login(SMTP_USER, SMTP_PASS); smtp.sendmail(SMTP_USER, user["email"], msg.as_string())
            email_sent = True
        except: pass
    return {"ok": True, "message": "Se o email existir, você receberá as instruções.", "reset_url": reset_url if not email_sent else None, "email_sent": email_sent}

@app.post("/auth/forgot-password-whatsapp")
async def forgot_password_whatsapp(body: dict):
    """
    Recuperação de senha via WhatsApp.
    O usuário informa o telefone cadastrado na conta e recebemos o link via WhatsApp.
    """
    phone_raw = (body.get("phone") or "").strip()
    if not phone_raw:
        raise HTTPException(status_code=400, detail="Telefone obrigatório")

    # Normaliza: remove tudo que não é dígito
    phone_clean = "".join(c for c in phone_raw if c.isdigit())
    if len(phone_clean) < 8:
        raise HTTPException(status_code=400, detail="Telefone inválido")

    # Busca usuário pelo telefone (coluna phone na tabela users)
    # Tenta com e sem código do país para flexibilidade
    user = None
    for variant in [phone_clean, phone_clean[-11:], phone_clean[-10:]]:
        rows = supabase.table("users").select("id,name,email,tenant_id,phone")             .eq("is_active", True).execute().data
        # Filtra manualmente para suportar variações do número
        for r in rows:
            stored = "".join(c for c in (r.get("phone") or "") if c.isdigit())
            if stored and (stored == variant or stored.endswith(variant) or variant.endswith(stored)):
                user = r
                break
        if user:
            break

    # Resposta genérica para não vazar se o número existe
    if not user:
        return {"ok": True, "message": "Se o telefone estiver cadastrado, você receberá o link no WhatsApp."}

    # Gera token de reset
    token = "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=48))
    expires_at = (datetime.utcnow() + timedelta(hours=2)).isoformat()
    supabase.table("password_resets").upsert(
        {"user_id": user["id"], "token": token, "expires_at": expires_at, "used": False},
        on_conflict="user_id"
    ).execute()

    reset_url = f"https://7zap-inbox-frontend.vercel.app/?reset={token}"

    # Busca qualquer instância WAHA ativa do tenant para enviar
    waha_sent = False
    try:
        instances = supabase.table("gateway_instances")             .select("instance_name").eq("tenant_id", user["tenant_id"]).execute().data
        session_to_use = None
        if instances:
            # Verifica qual instância está WORKING
            async with httpx.AsyncClient(timeout=8) as client:
                for inst in instances:
                    iname = inst.get("instance_name")
                    if not iname:
                        continue
                    try:
                        r = await client.get(f"{WAHA_URL}/api/sessions/{iname}", headers=waha_headers())
                        if r.status_code == 200 and r.json().get("status") == "WORKING":
                            session_to_use = iname
                            break
                    except:
                        continue

        if session_to_use:
            msg_text = (
                "🔐 *Recuperação de senha — 7CRM*\n\n"
                f"Olá, *{user['name']}*! Recebemos uma solicitação de redefinição de senha.\n\n"
                "Clique no link abaixo para criar uma nova senha (expira em 2h):\n\n"
                f"{reset_url}\n\n"
                "Se não foi você, ignore esta mensagem."
            )
            # phone_clean já tem só dígitos
            await waha_send_msg(phone_clean, msg_text, session_to_use)
            waha_sent = True
    except Exception as e:
        print(f"[forgot_password_whatsapp] erro ao enviar WhatsApp: {e}")

    return {
        "ok": True,
        "message": "Se o telefone estiver cadastrado, você receberá o link no WhatsApp.",
        "whatsapp_sent": waha_sent,
        # Fallback manual se WhatsApp falhar (para debug/suporte)
        "reset_url": reset_url if not waha_sent else None,
    }

@app.post("/auth/reset-password")
async def reset_password(body: dict):
    token = (body.get("token") or "").strip()
    new_password = body.get("password") or ""
    if not token or not new_password: raise HTTPException(status_code=400, detail="Token e senha obrigatórios")
    if len(new_password) < 6: raise HTTPException(status_code=400, detail="Senha deve ter pelo menos 6 caracteres")
    resets = supabase.table("password_resets").select("*").eq("token", token).eq("used", False).execute().data
    if not resets: raise HTTPException(status_code=400, detail="Link inválido ou expirado")
    reset = resets[0]
    if datetime.utcnow() > datetime.fromisoformat(reset["expires_at"].replace("Z", "")): raise HTTPException(status_code=400, detail="Link expirado.")
    pw_hash = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
    supabase.table("users").update({"password_hash": pw_hash}).eq("id", reset["user_id"]).execute()
    supabase.table("password_resets").update({"used": True}).eq("token", token).execute()
    return {"ok": True, "message": "Senha redefinida com sucesso!"}

# ── CONVITES ──────────────────────────────────────────────
@app.post("/auth/invite", dependencies=[Depends(verify_key)])
async def create_invite(body: dict, admin=Depends(require_admin)):
    tenant_id = admin["tenant_id"]
    code = "".join(random.choices("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", k=8))
    expires_at = (datetime.utcnow() + timedelta(days=7)).isoformat()
    supabase.table("invite_codes").insert({"code": code, "tenant_id": tenant_id, "created_by": admin["sub"], "expires_at": expires_at, "used": False}).execute()
    invite_url = f"https://7zap-inbox-frontend.vercel.app/?invite={code}"
    return {"ok": True, "code": code, "invite_url": invite_url, "expires_at": expires_at}

@app.get("/auth/invite/{code}")
async def validate_invite(code: str):
    invites = supabase.table("invite_codes").select("*, tenants(name)").eq("code", code.upper()).eq("used", False).execute().data
    if not invites: raise HTTPException(status_code=404, detail="Convite inválido ou já utilizado")
    invite = invites[0]
    if datetime.utcnow() > datetime.fromisoformat(invite["expires_at"].replace("Z", "")): raise HTTPException(status_code=400, detail="Convite expirado")
    return {"ok": True, "tenant_name": (invite.get("tenants") or {}).get("name", ""), "tenant_id": invite["tenant_id"], "code": code.upper()}

@app.post("/auth/register")
async def register_with_invite(body: dict):
    code = (body.get("invite_code") or "").strip().upper()
    name = (body.get("name") or "").strip()
    email = (body.get("email") or "").lower().strip()
    password = body.get("password") or ""
    if not all([code, name, email, password]): raise HTTPException(status_code=400, detail="Todos os campos são obrigatórios")
    if len(password) < 6: raise HTTPException(status_code=400, detail="Senha deve ter pelo menos 6 caracteres")
    invites = supabase.table("invite_codes").select("*").eq("code", code).eq("used", False).execute().data
    if not invites: raise HTTPException(status_code=404, detail="Convite inválido ou já utilizado")
    invite = invites[0]
    if datetime.utcnow() > datetime.fromisoformat(invite["expires_at"].replace("Z", "")): raise HTTPException(status_code=400, detail="Convite expirado")
    if supabase.table("users").select("id").eq("email", email).execute().data: raise HTTPException(status_code=400, detail="Email já cadastrado")
    colors = ["#00c853", "#2979ff", "#ff6d00", "#e91e63", "#9c27b0", "#00bcd4"]
    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    user = supabase.table("users").insert({"tenant_id": invite["tenant_id"], "name": name, "email": email, "role": "agent", "password_hash": pw_hash, "is_active": True, "avatar_color": random.choice(colors)}).execute().data[0]
    supabase.table("invite_codes").update({"used": True, "used_by": user["id"]}).eq("code", code).execute()
    user.pop("password_hash", None)
    return {"ok": True, "user": user, "message": "Conta criada com sucesso!"}

@app.post("/auth/register-trial")
async def register_trial(body: dict):
    """
    Auto-cadastro — cria tenant + admin sem convite.
    Plano: trial (7 dias). Bloqueado após expirar.
    """
    company = (body.get("company") or "").strip()
    name    = (body.get("name") or "").strip()
    email   = (body.get("email") or "").lower().strip()
    password = body.get("password") or ""
    phone   = (body.get("phone") or "").strip()
    segment = (body.get("segment") or "outros").strip()  # academia | clinica | comercio | etc

    if not all([company, name, email, password]):
        raise HTTPException(400, "Nome da empresa, nome, email e senha são obrigatórios")
    if len(password) < 6:
        raise HTTPException(400, "Senha deve ter pelo menos 6 caracteres")
    if supabase.table("users").select("id").eq("email", email).execute().data:
        raise HTTPException(400, "Este email já está cadastrado")

    # Cria tenant
    trial_ends = (datetime.utcnow() + timedelta(days=7)).isoformat()
    colors = ["#00c853","#2979ff","#ff6d00","#e91e63","#9c27b0","#00bcd4"]
    tenant = supabase.table("tenants").insert({
        "name": company,
        "plan": "trial",
        "trial_ends_at": trial_ends,
        "trial_used": True,
        "segment": segment,
        "is_blocked": False,
        "ai_credits": 1000,
    }).execute().data[0]

    # Cria usuário admin
    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    user = supabase.table("users").insert({
        "tenant_id": tenant["id"],
        "name": name,
        "email": email,
        "role": "admin",
        "password_hash": pw_hash,
        "is_active": True,
        "avatar_color": random.choice(colors),
        "phone": phone or None,
    }).execute().data[0]

    user.pop("password_hash", None)
    print(f"[register_trial] Novo tenant: {company} ({tenant['id']}) | {email}")
    return {
        "ok": True,
        "tenant_id": tenant["id"],
        "user": user,
        "trial_ends_at": trial_ends,
        "message": f"Conta criada! Seu trial de 7 dias começa agora. 🚀"
    }

@app.get("/auth/invites", dependencies=[Depends(verify_key)])
async def list_invites(admin=Depends(require_admin)):
    invites = supabase.table("invite_codes").select("*, users!created_by(name)").eq("tenant_id", admin["tenant_id"]).order("created_at", desc=True).limit(20).execute().data
    return {"invites": invites}



# ══════════════════════════════════════════════════════════════════════════════
# SUPERADMIN — Painel da Andressa (read-only, tenant = SUPER_ADMIN_TENANT)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/superadmin/dashboard", dependencies=[Depends(verify_key)])
async def superadmin_dashboard(user=Depends(require_super_admin)):
    """Overview geral: tenants, MRR estimado, trials, mensagens."""
    tenants = supabase.table("tenants").select(
        "id,name,plan,created_at,trial_ends_at,is_blocked,segment,ai_credits,ai_credits_purchased"
    ).order("created_at", desc=True).execute().data or []

    plan_price = {"starter": 149, "pro": 299, "business": 599, "enterprise": 1200, "trial": 0}
    now = datetime.utcnow()

    stats = {
        "total_tenants": len(tenants),
        "active_paid": sum(1 for t in tenants if t.get("plan") not in ["trial", None] and not t.get("is_blocked")),
        "active_trials": sum(1 for t in tenants if t.get("plan") == "trial" and not t.get("is_blocked")),
        "expired_trials": sum(1 for t in tenants if t.get("is_blocked")),
        "mrr_estimate": sum(plan_price.get(t.get("plan"), 0) for t in tenants if not t.get("is_blocked")),
        "new_this_month": sum(1 for t in tenants if t.get("created_at", "") >= now.replace(day=1).isoformat()[:10]),
    }

    # Crescimento mês a mês (últimos 6 meses)
    monthly = {}
    for t in tenants:
        mo = (t.get("created_at") or "")[:7]
        if mo:
            monthly[mo] = monthly.get(mo, 0) + 1
    stats["monthly_signups"] = sorted([{"month": k, "count": v} for k, v in monthly.items()])[-6:]

    # Distribuição por plano
    plan_dist = {}
    for t in tenants:
        p = t.get("plan") or "trial"
        plan_dist[p] = plan_dist.get(p, 0) + 1
    stats["plan_distribution"] = [{"plan": k, "count": v} for k, v in plan_dist.items()]

    # Distribuição por segmento
    seg_dist = {}
    for t in tenants:
        s = t.get("segment") or "outros"
        seg_dist[s] = seg_dist.get(s, 0) + 1
    stats["segment_distribution"] = [{"segment": k, "count": v} for k, v in seg_dist.items()]

    return stats

@app.get("/superadmin/tenants", dependencies=[Depends(verify_key)])
async def superadmin_tenants(user=Depends(require_super_admin), page: int = 1, limit: int = 30, search: str = ""):
    """Lista todos os tenants com estatísticas."""
    tenants_raw = supabase.table("tenants").select(
        "id,name,plan,created_at,trial_ends_at,is_blocked,segment,ai_credits,ai_credits_purchased"
    ).order("created_at", desc=True).execute().data or []

    if search:
        tenants_raw = [t for t in tenants_raw if search.lower() in (t.get("name") or "").lower()]

    total = len(tenants_raw)
    offset = (page - 1) * limit
    page_tenants = tenants_raw[offset:offset + limit]

    # Para cada tenant, busca users e instâncias
    result = []
    for t in page_tenants:
        tid = t["id"]
        users = supabase.table("users").select("id,name,email,role,is_active,last_login").eq("tenant_id", tid).execute().data or []
        instances = supabase.table("gateway_instances").select("id,instance_name,phone,status").eq("tenant_id", tid).execute().data or []

        # Conta mensagens (aproximado via conversations)
        convs = supabase.table("conversations").select("id", count="exact").eq("tenant_id", tid).execute()
        conv_count = convs.count or 0

        # Trial info
        trial_info = None
        if t.get("plan") == "trial" and t.get("trial_ends_at"):
            ends = datetime.fromisoformat(t["trial_ends_at"].replace("Z", ""))
            days_left = (ends - datetime.utcnow()).days
            trial_info = {"ends_at": t["trial_ends_at"], "days_left": days_left, "expired": days_left < 0}

        result.append({
            **t,
            "users_count": len(users),
            "users": [{"id": u["id"], "name": u["name"], "email": u["email"], "role": u["role"], "is_active": u["is_active"], "last_login": u["last_login"]} for u in users],
            "instances_count": len(instances),
            "instances": instances,
            "connected_phones": sum(1 for i in instances if (i.get("status") or "").upper() in ["WORKING", "CONNECTED", "ONLINE"]),
            "conversations_count": conv_count,
            "trial_info": trial_info,
            "ai_credits_used": (1000 - (t.get("ai_credits") or 0)) if t.get("plan") == "trial" else None,
        })

    return {"tenants": result, "total": total, "page": page, "pages": (total + limit - 1) // limit}

@app.get("/superadmin/tenants/{tenant_id}", dependencies=[Depends(verify_key)])
async def superadmin_tenant_detail(tenant_id: str, user=Depends(require_super_admin)):
    """Detalhe completo de um tenant."""
    tenant = supabase.table("tenants").select("*").eq("id", tenant_id).single().execute().data
    if not tenant:
        raise HTTPException(404, "Tenant não encontrado")

    users = supabase.table("users").select("id,name,email,role,is_active,last_login,created_at").eq("tenant_id", tenant_id).execute().data or []
    instances = supabase.table("gateway_instances").select("*").eq("tenant_id", tenant_id).execute().data or []

    # Contagens
    convs_resp = supabase.table("conversations").select("id,status,created_at", count="exact").eq("tenant_id", tenant_id).execute()
    conv_count = convs_resp.count or 0
    conv_data = convs_resp.data or []

    # Broadcasts
    broadcasts = supabase.table("broadcasts").select("id,status,created_at").eq("tenant_id", tenant_id).order("created_at", desc=True).limit(5).execute().data or []

    # Activity (conversations por dia últimos 30 dias)
    since = (datetime.utcnow() - timedelta(days=30)).isoformat()
    recent_convs = supabase.table("conversations").select("created_at").eq("tenant_id", tenant_id).gte("created_at", since).execute().data or []
    daily = {}
    for c in recent_convs:
        day = (c.get("created_at") or "")[:10]
        if day: daily[day] = daily.get(day, 0) + 1

    return {
        "tenant": tenant,
        "users": users,
        "instances": instances,
        "connected_phones": sum(1 for i in instances if (i.get("status") or "").upper() in ["WORKING", "CONNECTED", "ONLINE"]),
        "conversations_count": conv_count,
        "open_conversations": sum(1 for c in conv_data if c.get("status") == "open"),
        "broadcasts": broadcasts,
        "activity_last_30d": sorted([{"date": k, "count": v} for k, v in daily.items()]),
    }

@app.post("/superadmin/tenants/{tenant_id}/block", dependencies=[Depends(verify_key)])
async def superadmin_block_tenant(tenant_id: str, body: dict, user=Depends(require_super_admin)):
    """Bloqueia ou desbloqueia um tenant. DESABILITADO — painel é somente leitura por ora."""
    raise HTTPException(status_code=403, detail="Ação não disponível no momento. Contate o desenvolvedor.")

@app.post("/superadmin/tenants/{tenant_id}/extend-trial", dependencies=[Depends(verify_key)])
async def superadmin_extend_trial(tenant_id: str, body: dict, user=Depends(require_super_admin)):
    """Estende o trial. DESABILITADO — painel é somente leitura por ora."""
    raise HTTPException(status_code=403, detail="Ação não disponível no momento. Contate o desenvolvedor.")

@app.post("/superadmin/tenants/{tenant_id}/upgrade-plan", dependencies=[Depends(verify_key)])
async def superadmin_upgrade_plan(tenant_id: str, body: dict, user=Depends(require_super_admin)):
    """Muda o plano. DESABILITADO — painel é somente leitura por ora."""
    raise HTTPException(status_code=403, detail="Ação não disponível no momento. Contate o desenvolvedor.")

# ── SOCIAL AUTH (Google, Facebook, Apple, Microsoft) ─────────────────────────
@app.post("/auth/social")
async def social_login(body: dict):
    """Recebe access_token do Supabase OAuth, verifica, cria conta/tenant se necessário."""
    access_token = body.get("access_token") or ""
    if not access_token:
        raise HTTPException(400, "access_token obrigatório")
    try:
        user_resp = supabase.auth.get_user(access_token)
        sb_user = user_resp.user
        if not sb_user:
            raise HTTPException(401, "Token inválido")
    except Exception as e:
        raise HTTPException(401, f"Token inválido: {str(e)}")

    email = (sb_user.email or "").lower().strip()
    if not email:
        raise HTTPException(400, "Email não disponível no provider")

    meta = sb_user.user_metadata or {}
    full_name = (meta.get("full_name") or meta.get("name") or email.split("@")[0]).strip()
    avatar_url = meta.get("avatar_url") or meta.get("picture") or ""
    provider = (sb_user.app_metadata or {}).get("provider", "google")
    colors = ["#00c853","#2979ff","#ff6d00","#e91e63","#9c27b0","#00bcd4"]

    # Usuário já existe?
    existing = supabase.table("users").select("*").eq("email", email).execute().data
    if existing:
        user = existing[0]
        if avatar_url and not user.get("avatar_url"):
            supabase.table("users").update({"avatar_url": avatar_url}).eq("id", user["id"]).execute()
            user["avatar_url"] = avatar_url
    else:
        # Novo usuário — cria tenant trial + admin
        trial_ends = (datetime.utcnow() + timedelta(days=14)).isoformat()
        tenant = supabase.table("tenants").insert({
            "name": f"Empresa de {full_name.split()[0]}",
            "plan": "trial",
            "ai_credits": 200,
            "ai_credits_reset_at": datetime.utcnow().isoformat(),
            "trial_ends_at": trial_ends,
        }).execute().data[0]
        user = supabase.table("users").insert({
            "tenant_id": tenant["id"],
            "name": full_name,
            "email": email,
            "role": "admin",
            "is_active": True,
            "avatar_color": random.choice(colors),
            "avatar_url": avatar_url,
            "auth_provider": provider,
        }).execute().data[0]

    user.pop("password_hash", None)
    import secrets as _s
    session_id = _s.token_hex(16)
    supabase.table("users").update({"session_id": session_id, "last_login": datetime.utcnow().isoformat()}).eq("id", user["id"]).execute()
    token = create_jwt(user, session_id)
    return {"token": token, "user": user, "tenant_id": user["tenant_id"]}

# ── RELATÓRIOS / ANALYTICS ────────────────────────────────

@app.get("/reports/messages")
async def report_messages(tenant_id: str = Depends(enforce_tenant), days: int = 30, instance_name: str = None):
    """Mensagens por dia e por hora — heatmap"""
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    # Get conversations for this tenant (optionally filtered by instance)
    q = supabase.table("conversations").select("id").eq("tenant_id", tenant_id)
    if instance_name:
        q = q.eq("instance_name", instance_name)
    convs = q.execute().data
    conv_ids = [c["id"] for c in convs]
    if not conv_ids:
        return {"by_day": [], "by_hour": [], "by_weekday": [], "total": 0}
    
    # Fetch messages
    msgs = []
    batch = 200
    for i in range(0, len(conv_ids), batch):
        chunk = conv_ids[i:i+batch]
        r = supabase.table("messages").select("id,direction,created_at,conversation_id").in_("conversation_id", chunk).gte("created_at", since).execute().data
        msgs.extend(r)
    
    # Aggregate by day
    by_day = {}
    by_hour = {str(h): 0 for h in range(24)}
    by_weekday = {str(d): 0 for d in range(7)}
    inbound = 0
    outbound = 0
    
    for m in msgs:
        try:
            dt = datetime.fromisoformat(m["created_at"].replace("Z","").replace("+00:00",""))
            day = dt.strftime("%Y-%m-%d")
            by_day[day] = by_day.get(day, 0) + 1
            by_hour[str(dt.hour)] = by_hour.get(str(dt.hour), 0) + 1
            by_weekday[str(dt.weekday())] = by_weekday.get(str(dt.weekday()), 0) + 1
            if m["direction"] == "inbound": inbound += 1
            else: outbound += 1
        except: pass
    
    by_day_list = [{"date": k, "count": v} for k,v in sorted(by_day.items())]
    by_hour_list = [{"hour": int(k), "count": v} for k,v in sorted(by_hour.items(), key=lambda x: int(x[0]))]
    by_weekday_list = [{"day": int(k), "label": ["Seg","Ter","Qua","Qui","Sex","Sáb","Dom"][int(k)], "count": v} for k,v in sorted(by_weekday.items(), key=lambda x: int(x[0]))]
    
    return {"by_day": by_day_list, "by_hour": by_hour_list, "by_weekday": by_weekday_list,
            "total": len(msgs), "inbound": inbound, "outbound": outbound}

@app.get("/reports/agents")
async def report_agents(tenant_id: str = Depends(enforce_tenant), days: int = 30, instance_name: str = None):
    """Performance por atendente"""
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    users = supabase.table("users").select("id,name,email,role,allowed_instances").eq("tenant_id", tenant_id).execute().data
    q = supabase.table("conversations").select("id,assigned_to,status,created_at,last_message_at").eq("tenant_id", tenant_id).gte("created_at", since)
    if instance_name:
        q = q.eq("instance_name", instance_name)
    convs = q.execute().data

    # When filtering by instance, only include users who have access to it
    # (admin always included; agent included if allowed_instances is empty OR contains this instance)
    if instance_name:
        # Get the gateway_instance id for this instance_name
        inst_row = supabase.table("gateway_instances").select("id").eq("tenant_id", tenant_id).eq("instance_name", instance_name).execute().data
        inst_id = inst_row[0]["id"] if inst_row else None
        def has_access(u):
            if u.get("role") == "admin": return True
            allowed = u.get("allowed_instances") or []
            if not allowed: return True  # empty = all instances
            return (instance_name in allowed) or (inst_id and inst_id in allowed)
        users = [u for u in users if has_access(u)]

    agent_map = {u["id"]: {"id": u["id"], "name": u["name"], "email": u["email"], "role": u["role"],
                           "total_convs": 0, "resolved": 0, "active": 0, "msgs_sent": 0} for u in users}
    
    conv_ids = [c["id"] for c in convs]
    msgs_sent = {}
    if conv_ids:
        for i in range(0, len(conv_ids), 200):
            chunk = conv_ids[i:i+200]
            ms = supabase.table("messages").select("conversation_id,direction").in_("conversation_id", chunk).eq("direction","outbound").execute().data
            for m in ms:
                msgs_sent[m["conversation_id"]] = msgs_sent.get(m["conversation_id"], 0) + 1
    
    for c in convs:
        aid = c.get("assigned_to")
        if aid and aid in agent_map:
            agent_map[aid]["total_convs"] += 1
            if c["status"] == "resolved": agent_map[aid]["resolved"] += 1
            else: agent_map[aid]["active"] += 1
            agent_map[aid]["msgs_sent"] += msgs_sent.get(c["id"], 0)
    
    agents_list = sorted(agent_map.values(), key=lambda x: x["total_convs"], reverse=True)
    return {"agents": agents_list, "period_days": days}

@app.get("/reports/broadcasts")
async def report_broadcasts(tenant_id: str = Depends(enforce_tenant), instance_name: str = None):
    """Relatório de disparos — enviados, entregues e ROI (conversas geradas)"""
    q = supabase.table("broadcasts").select("*").eq("tenant_id", tenant_id).order("created_at", desc=True).limit(20)
    if instance_name:
        q = q.eq("instance_name", instance_name)
    broadcasts = q.execute().data
    result = []
    for b in broadcasts:
        sent = b.get("total_recipients") or b.get("sent_count") or 0
        failed = b.get("failed_count") or 0
        delivered = sent - failed
        broadcast_id = b["id"]
        created_at = b.get("created_at", "")

        # Get phone numbers that were contacted in this broadcast
        recipients = supabase.table("broadcast_recipients").select("phone,contact_id,sent_at").eq("broadcast_id", broadcast_id).eq("status","sent").execute().data
        phones = [r["phone"] for r in recipients if r.get("phone")]
        
        # Count replies: contacts that sent at least 1 inbound message AFTER broadcast was sent
        replied = 0
        replied_names = []
        if phones and created_at:
            # Get contacts matching those phones in this tenant
            contacts = supabase.table("contacts").select("id,name,phone").eq("tenant_id", tenant_id).execute().data
            phone_to_contact = {c["phone"]: c for c in contacts}
            contact_ids = [phone_to_contact[p]["id"] for p in phones if p in phone_to_contact]
            
            if contact_ids:
                # Get conversations for those contacts
                convs = supabase.table("conversations").select("id,contact_id").in_("contact_id", contact_ids).execute().data
                conv_ids = [c["id"] for c in convs]
                
                if conv_ids:
                    # Count convs with at least 1 inbound message after broadcast sent_at
                    for cid in conv_ids:
                        msgs = supabase.table("messages").select("id").eq("conversation_id", cid).eq("direction","inbound").gte("created_at", created_at).limit(1).execute().data
                        if msgs:
                            replied += 1
                            # Find contact name
                            conv_contact_id = next((c["contact_id"] for c in convs if c["id"] == cid), None)
                            contact = next((c for c in contacts if c["id"] == conv_contact_id), None)
                            if contact:
                                replied_names.append(contact.get("name") or contact.get("phone",""))
        
        roi_pct = round(replied / delivered * 100) if delivered > 0 else 0
        result.append({
            "id": broadcast_id,
            "name": b.get("name") or "",
            "message": (b.get("message","") or "")[:100],
            "created_at": created_at,
            "status": b.get("status"),
            "sent": sent,
            "delivered": delivered,
            "failed": failed,
            "replied": replied,
            "roi_pct": roi_pct,
            "replied_sample": replied_names[:5]  # primeiros 5 nomes que responderam
        })
    return {"broadcasts": result}

@app.get("/reports/credits")
async def report_credits(tenant_id: str = Depends(enforce_tenant), days: int = 30):
    """Consumo de créditos de IA por período"""
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    # Get AI suggestions used (messages with ai_suggestion set)
    convs = supabase.table("conversations").select("id,assigned_to").eq("tenant_id", tenant_id).execute().data
    conv_ids = [c["id"] for c in convs]
    assigned = {c["id"]: c.get("assigned_to") for c in convs}
    
    ai_msgs = []
    if conv_ids:
        for i in range(0, len(conv_ids), 200):
            chunk = conv_ids[i:i+200]
            r = supabase.table("messages").select("conversation_id,created_at,ai_suggestion").in_("conversation_id", chunk).gte("created_at", since).not_.is_("ai_suggestion", "null").execute().data
            ai_msgs.extend(r)
    
    # Per agent
    users = supabase.table("users").select("id,name").eq("tenant_id", tenant_id).execute().data
    user_names = {u["id"]: u["name"] for u in users}
    by_agent = {}
    by_day = {}
    
    for m in ai_msgs:
        aid = assigned.get(m["conversation_id"], "unknown")
        name = user_names.get(aid, "Sem atribuição")
        by_agent[name] = by_agent.get(name, 0) + 1
        try:
            day = datetime.fromisoformat(m["created_at"].replace("Z","").replace("+00:00","")).strftime("%Y-%m-%d")
            by_day[day] = by_day.get(day, 0) + 1
        except: pass
    
    tenant_data = supabase.table("tenants").select("ai_credits,ai_credits_reset_at,plan").eq("id", tenant_id).single().execute().data
    credits_remaining, plan, limit = get_tenant_credits(tenant_id)
    
    return {
        "total_used": len(ai_msgs),
        "credits_remaining": credits_remaining,
        "credits_limit": limit,
        "plan": plan,
        "cost_estimate": round(len(ai_msgs) * 0.0004, 2),  # GPT-4o Mini pricing
        "by_agent": [{"name": k, "used": v} for k,v in sorted(by_agent.items(), key=lambda x: x[1], reverse=True)],
        "by_day": [{"date": k, "count": v} for k,v in sorted(by_day.items())],
        "period_days": days
    }

@app.get("/reports/financial-forecast")
async def report_financial_forecast(tenant_id: str = Depends(enforce_tenant)):
    """Previsão financeira — próximo mês"""
    # Require super admin
    if tenant_id != os.environ.get("SUPER_ADMIN_TENANT", "98c38c97-2796-471f-bfc9-f093ff3ae6e9"):
        raise HTTPException(403, "Acesso restrito")
    
    all_tenants = supabase.table("tenants").select("id,name,plan,is_blocked,created_at").execute().data
    plan_prices = {"trial": 0, "starter": 99, "pro": 149, "business": 299, "enterprise": 999}
    
    forecast = []
    mrr_total = 0
    for t in all_tenants:
        if t.get("is_blocked"): continue
        price = plan_prices.get(t.get("plan","starter"), 0)
        if price == 0: continue
        mrr_total += price
        # Estimate renewal date (monthly from created_at)
        try:
            created = datetime.fromisoformat(t["created_at"].replace("Z","").replace("+00:00",""))
            now = datetime.utcnow()
            months_since = (now.year - created.year)*12 + now.month - created.month
            renewal = created.replace(year=created.year + (created.month + months_since) // 12,
                                      month=(created.month + months_since) % 12 or 12)
            days_until = (renewal - now).days % 30
        except: days_until = 15
        forecast.append({"name": t["name"], "plan": t.get("plan"), "mrr": price, "days_until_renewal": days_until})
    
    forecast.sort(key=lambda x: x["days_until_renewal"])
    
    return {
        "mrr_forecast": mrr_total,
        "arr_forecast": mrr_total * 12,
        "active_tenants": len(forecast),
        "renewals_this_week": [f for f in forecast if f["days_until_renewal"] <= 7],
        "renewals_this_month": forecast,
        "plan_breakdown": {p: sum(1 for f in forecast if f["plan"]==p) for p in plan_prices}
    }


# ─── MISSING ENDPOINTS — added by audit ─────────────────────────────────────

# ── /admin/tenants — SuperAdmin tenant management via main app ────────────────
@app.get("/admin/tenants", dependencies=[Depends(verify_key)])
async def admin_list_tenants():
    """List all tenants with user count and MRR info (superadmin use)."""
    try:
        rows = supabase.table("tenants").select("id,name,plan,is_blocked,created_at,activated_at,trial_ends_at").order("created_at", desc=True).execute().data or []
        plan_prices = {"trial": 0, "starter": 149, "pro": 299, "business": 599, "enterprise": 1200}
        total_mrr = 0
        result = []
        for t in rows:
            users_count = supabase.table("users").select("id", count="exact").eq("tenant_id", t["id"]).execute().count or 0
            price = plan_prices.get(t.get("plan", "starter"), 0)
            if not t.get("is_blocked"):
                total_mrr += price
            result.append({**t, "user_count": users_count, "mrr": price})
        return {"tenants": result, "total_mrr": total_mrr}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/tenants", dependencies=[Depends(verify_key)])
async def admin_create_tenant(body: dict):
    """Create a new tenant and admin user (superadmin use)."""
    import secrets, string
    name = (body.get("name") or "").strip()
    email = (body.get("email") or "").strip().lower()
    plan = body.get("plan", "starter")
    phone = (body.get("phone") or "").replace("+", "").replace(" ", "")

    if not name or not email:
        raise HTTPException(status_code=400, detail="name and email are required")

    # Check duplicate
    existing = supabase.table("tenants").select("id").eq("name", name).execute().data
    if existing:
        raise HTTPException(status_code=400, detail=f"Tenant '{name}' already exists")

    tid = str(uuid.uuid4())
    try:
        supabase.table("tenants").insert({"id": tid, "name": name, "plan": plan,
            "ai_credits": 0, "ai_credits_limit": 0, "created_at": datetime.utcnow().isoformat()}).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create tenant: {e}")

    # Create admin user
    alphabet = string.ascii_letters + string.digits
    temp_pw = "".join(secrets.choice(alphabet) for _ in range(10))
    uid_val = str(uuid.uuid4())
    pw_hash = bcrypt.hashpw(temp_pw.encode(), bcrypt.gensalt()).decode()
    supabase.table("users").insert({"id": uid_val, "tenant_id": tid, "name": name,
        "email": email, "password_hash": pw_hash, "role": "admin",
        "created_at": datetime.utcnow().isoformat()}).execute()

    # Generate invite code
    invite_code = "".join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(8))
    supabase.table("invite_codes").insert({"code": invite_code, "tenant_id": tid,
        "created_by": uid_val, "expires_at": (datetime.utcnow() + timedelta(days=7)).isoformat()}).execute()

    invite_url = f"https://7zap-inbox-frontend.vercel.app/?invite={invite_code}"

    # Try to send WhatsApp invite
    whatsapp_sent = False
    if phone and WAHA_URL:
        try:
            msg = (f"Olá! Sua conta no 7zap Inbox foi criada. 🎉\n\n"
                   f"*Empresa:* {name}\n*Email:* {email}\n*Senha temp:* {temp_pw}\n\n"
                   f"Acesse: {invite_url}\n\nAlterNe sua senha no primeiro acesso.")
            async with httpx.AsyncClient(timeout=5) as _hc:
                r2 = await _hc.post(f"{WAHA_URL}/api/sendText",
                    headers={"x-api-key": WAHA_KEY, "Content-Type": "application/json"},
                    json={"session": "default", "chatId": f"{phone}@c.us", "text": msg})
            whatsapp_sent = r2.status_code == 201
        except Exception:
            pass

    return {"ok": True, "tenant_id": tid, "invite_url": invite_url,
            "temp_password": temp_pw, "whatsapp_sent": whatsapp_sent}


@app.put("/admin/tenants/{tenant_id}/plan", dependencies=[Depends(verify_key)])
async def admin_update_tenant_plan(tenant_id: str, body: dict):
    plan = body.get("plan", "starter")
    supabase.table("tenants").update({"plan": plan}).eq("id", tenant_id).execute()
    return {"ok": True}


@app.put("/admin/tenants/{tenant_id}/block", dependencies=[Depends(verify_key)])
async def admin_block_tenant(tenant_id: str, body: dict):
    blocked = bool(body.get("blocked", True))
    supabase.table("tenants").update({"is_blocked": blocked}).eq("id", tenant_id).execute()
    return {"ok": True}


@app.post("/admin/tenants/{tenant_id}/resend-invite", dependencies=[Depends(verify_key)])
async def admin_resend_invite(tenant_id: str, body: dict):
    """Generate fresh invite link and optionally send via WhatsApp."""
    import secrets, string
    phone = (body.get("phone") or "").replace("+", "").replace(" ", "")

    admin = supabase.table("users").select("id,name").eq("tenant_id", tenant_id).eq("role", "admin").limit(1).execute().data
    if not admin:
        raise HTTPException(status_code=404, detail="Tenant admin not found")
    admin_user = admin[0]

    invite_code = "".join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(8))
    supabase.table("invite_codes").insert({"code": invite_code, "tenant_id": tenant_id,
        "created_by": admin_user["id"], "expires_at": (datetime.utcnow() + timedelta(days=7)).isoformat()}).execute()
    invite_url = f"https://7zap-inbox-frontend.vercel.app/?invite={invite_code}"

    whatsapp_sent = False
    if phone and WAHA_URL:
        try:
            msg = f"Seu link de acesso ao 7zap Inbox:\n{invite_url}"
            async with httpx.AsyncClient(timeout=5) as _hc:
                r2 = await _hc.post(f"{WAHA_URL}/api/sendText",
                    headers={"x-api-key": WAHA_KEY, "Content-Type": "application/json"},
                    json={"session": "default", "chatId": f"{phone}@c.us", "text": msg})
            whatsapp_sent = r2.status_code == 201
        except Exception:
            pass

    return {"ok": True, "invite_url": invite_url, "whatsapp_sent": whatsapp_sent}


# ── /contacts/sync-profile-pictures ───────────────────────────────────────────
@app.post("/contacts/sync-profile-pictures", dependencies=[Depends(verify_key)])
async def sync_profile_pictures(tenant_id: str):
    """Batch-sync profile pictures for all contacts of a tenant that haven't been synced yet."""
    try:
        contacts = supabase.table("contacts").select("id,phone").eq("tenant_id", tenant_id)\
            .is_("profile_picture_url", "null").limit(50).execute().data or []

        # Get active instance for tenant
        inst_row = supabase.table("gateway_instances").select("instance_name").eq("tenant_id", tenant_id)\
            .eq("status", "connected").limit(1).execute().data
        if not inst_row:
            return {"ok": True, "synced": 0, "message": "No connected instance"}
        instance_name = inst_row[0]["instance_name"]

        synced = 0
        for c in contacts:
            phone = c["phone"]
            clean = phone.replace("+", "").replace(" ", "").replace("-", "")
            if not clean.startswith("55"):
                clean = "55" + clean
            try:
                async with httpx.AsyncClient(timeout=3) as _hc:
                    r = await _hc.get(
                        f"{WAHA_URL}/api/contacts/profile-picture",
                        params={"session": instance_name, "contactId": f"{clean}@c.us"},
                        headers={"x-api-key": WAHA_KEY}
                    )
                if r.status_code == 200:
                    url = r.json().get("url", "")
                    if url:
                        supabase.table("contacts").update(
                            {"profile_picture_url": url, "last_sync_at": datetime.utcnow().isoformat()}
                        ).eq("id", c["id"]).execute()
                        synced += 1
            except Exception:
                pass
        return {"ok": True, "synced": synced, "total": len(contacts)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── /conversations/{id}/auto-mode ─────────────────────────────────────────────
@app.put("/conversations/{conv_id}/auto-mode", dependencies=[Depends(verify_key)])
async def set_conv_auto_mode(conv_id: str, body: dict):
    """Enable/disable per-conversation copilot auto-mode.
    enabled=True também remove a pausa temporária do Redis.
    """
    enabled = bool(body.get("enabled", False))
    try:
        supabase.table("conversations").update(
            {"copilot_auto_mode": enabled, "updated_at": datetime.utcnow().isoformat()}
        ).eq("id", conv_id).execute()
        # Se atendente reativou manualmente → remove pausa temporária
        if enabled:
            autopilot_resume(conv_id)
        return {"ok": True, "copilot_auto_mode": enabled, "paused": False if enabled else autopilot_is_paused(conv_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/conversations/{conv_id}/auto-mode", dependencies=[Depends(verify_key)])
async def get_conv_auto_mode(conv_id: str):
    """Retorna status do auto-pilot para uma conversa: modo DB + pausa temporária Redis."""
    conv = supabase.table("conversations").select("copilot_auto_mode,instance_name").eq("id", conv_id).single().execute().data
    paused = autopilot_is_paused(conv_id)
    r = _get_redis()
    ttl = 0
    if paused and r:
        try: ttl = r.ttl(f"7crm:ap_pause:{conv_id}")
        except: pass
    return {
        "copilot_auto_mode": conv.get("copilot_auto_mode"),
        "paused_by_agent": paused,
        "pause_ttl_secs": ttl,
        "pause_ttl_mins": round(ttl / 60) if ttl > 0 else 0,
    }


# ── /tenant/kanban-columns — shared kanban column config ─────────────────────
@app.put("/tenant/kanban-columns", dependencies=[Depends(verify_key)])
async def save_kanban_columns(body: dict):
    """Save kanban columns shared for entire tenant (all users see the same columns)."""
    tenant_id = body.get("tenant_id")
    columns = body.get("columns")
    if not tenant_id or not isinstance(columns, list):
        raise HTTPException(status_code=400, detail="tenant_id and columns required")
    # Validate columns structure
    validated = []
    for col in columns:
        if isinstance(col, dict) and col.get("id") and col.get("label"):
            validated.append({"id": col["id"], "label": col["label"], "color": col.get("color", "#00a884")})
    if not validated:
        raise HTTPException(status_code=400, detail="At least one valid column required")
    try:
        supabase.table("tenants").update({"kanban_columns": validated}).eq("id", tenant_id).execute()
        return {"ok": True, "columns": validated}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
