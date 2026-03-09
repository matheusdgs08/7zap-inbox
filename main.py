from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks
import concurrent.futures
_thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)

async def run_sync(fn):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_thread_pool, fn)

# ── CACHE SIMPLES (stale-while-revalidate) ────────────────
_cache: dict = {}
def cache_get(key, stale_ok=False):
    v = _cache.get(key)
    if not v: return None
    age = datetime.utcnow().timestamp() - v["ts"]
    if age < v["ttl"]: return v["data"]
    if stale_ok and age < v["ttl"] * 10: return v["data"]  # serve stale até 10x o TTL
    return None
def cache_set(key, data, ttl=30):
    _cache[key] = {"data": data, "ts": datetime.utcnow().timestamp(), "ttl": ttl}
def cache_del(key):
    _cache.pop(key, None)
def cache_is_stale(key):
    v = _cache.get(key)
    if not v: return True
    return (datetime.utcnow().timestamp() - v["ts"]) >= v["ttl"]
# ── PLAN CREDIT CONSTANTS ────────────────────────────────
PLAN_CREDITS = {
    "trial":      200,   # suficiente pra testar, não pra depender
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
import httpx, os, jwt, bcrypt, asyncio, random, base64
from datetime import datetime, timedelta

app = FastAPI(title="7CRM API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SUPABASE_URL      = os.getenv("SUPABASE_URL")
SUPABASE_KEY      = os.getenv("SUPABASE_SERVICE_KEY")
WAHA_URL          = os.getenv("WAHA_URL", "http://localhost:3000")
WAHA_KEY          = os.getenv("WAHA_KEY", "pulsekey")
INBOX_API_KEY     = os.getenv("INBOX_API_KEY", "7zap_inbox_secret")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
OPENAI_API_KEY    = os.getenv("OPENAI_API_KEY", "")

# ── AI HELPER — usa OpenAI GPT-4o Mini (mais barato) com fallback pro Claude ──
async def call_ai(system: str, user: str, max_tokens: int = 300, prefer_openai: bool = True) -> str:
    """Chama GPT-4o Mini se disponível, senão fallback pro Claude Haiku."""
    if prefer_openai and OPENAI_API_KEY:
        async with httpx.AsyncClient(timeout=30) as client:
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
JWT_SECRET        = os.getenv("JWT_SECRET", "7crm_super_secret_change_in_prod")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
security = HTTPBearer(auto_error=False)

@app.on_event("startup")
async def start_keepalive():
    asyncio.create_task(keepalive_loop())

async def keepalive_loop():
    await asyncio.sleep(30)
    while True:
        try:
            supabase.table("tenants").select("id").limit(1).execute()
        except:
            pass
        await asyncio.sleep(240)

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

# ── WAHA HEADERS (X-Api-Key — formato correto WAHA) ──────
def waha_headers():
    return {"X-Api-Key": WAHA_KEY, "Content-Type": "application/json"}

# ── Schemas ──────────────────────────────────────────────
class LoginRequest(BaseModel):
    email: str
    password: str

class CreateUser(BaseModel):
    tenant_id: str; name: str; email: str; password: str; role: str = "agent"; avatar_color: Optional[str] = "#00c853"; permissions: str = "read_write"; allowed_instances: Optional[List[str]] = []

class UpdateUser(BaseModel):
    name: Optional[str] = None; email: Optional[str] = None; password: Optional[str] = None
    role: Optional[str] = None; is_active: Optional[bool] = None; avatar_color: Optional[str] = None; permissions: Optional[str] = None; allowed_instances: Optional[List[str]] = None

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
    tenant_id: str; copilot_prompt: str
    copilot_auto_mode: str = "off"
    copilot_schedule_start: str = "18:00"
    copilot_schedule_end: str = "09:00"

class CreateBroadcast(BaseModel):
    tenant_id: str; name: str; message: str; interval_min: int = 60; interval_max: int = 120
    scheduled_at: Optional[str] = None; recipients: List[dict]

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
    user = supabase.table("users").insert({"tenant_id": body.tenant_id, "name": body.name, "email": body.email.lower().strip(), "role": body.role, "password_hash": pw_hash, "is_active": True, "avatar_color": body.avatar_color, "permissions": body.permissions, "allowed_instances": body.allowed_instances or []}).execute().data[0]
    user.pop("password_hash", None)
    return {"user": user}

@app.put("/admin/users/{user_id}")
async def update_user_admin(user_id: str, body: UpdateUser, admin=Depends(require_admin)):
    updates = {k: v for k, v in {"name": body.name, "email": body.email, "role": body.role, "is_active": body.is_active, "avatar_color": body.avatar_color, "permissions": body.permissions, "allowed_instances": body.allowed_instances}.items() if v is not None}
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
@app.post("/webhook/inbox")
@app.post("/webhook/message")
async def receive_message(payload: dict):
    """Aceita formato WAHA: {event, session, payload: {from, fromMe, body, type}}"""
    # Suporte a ambos formatos: WAHA e Evolution API legado
    event = payload.get("event", "")

    # Formato WAHA
    if "payload" in payload and isinstance(payload["payload"], dict):
        data = payload["payload"]
        if data.get("fromMe"): return {"ok": True}
        phone = data.get("from", "").replace("@c.us", "").replace("@s.whatsapp.net", "").replace("@g.us", "")
        if not phone or "@g" in data.get("from", ""): return {"ok": True}  # ignora grupos
        content = data.get("body") or data.get("text") or ""
        msg_type = data.get("type", "chat")
        if not content:
            if "image" in msg_type:    content = "[Imagem]"
            elif "audio" in msg_type:  content = "[Áudio]"
            elif "video" in msg_type:  content = "[Vídeo]"
            elif "document" in msg_type: content = "[Documento]"
            elif "sticker" in msg_type: content = "[Sticker]"
            else: return {"ok": True}
        waha_id = data.get("id", "")
    # Formato Evolution API legado
    else:
        data = payload.get("data", {})
        key = data.get("key", {})
        if key.get("fromMe"): return {"ok": True}
        phone = key.get("remoteJid", "").replace("@s.whatsapp.net", "").replace("@g.us", "")
        if not phone: return {"ok": True}
        msg = data.get("message", {})
        content = msg.get("conversation") or (msg.get("extendedTextMessage") or {}).get("text") or (
            "[Imagem]" if msg.get("imageMessage") else "[Áudio]" if msg.get("audioMessage") else
            "[Documento]" if msg.get("documentMessage") else "")
        if not content: return {"ok": True}
        waha_id = ""

    # Captura session/instance_name do webhook
    instance_name = payload.get("session") or payload.get("instance") or payload.get("instanceName") or ""

    try:
        tenants = supabase.table("tenants").select("id").execute().data
        for tenant in tenants:
            tid = tenant["id"]
            contacts = supabase.table("contacts").select("id").eq("tenant_id", tid).eq("phone", phone).execute().data
            contact_id = contacts[0]["id"] if contacts else supabase.table("contacts").insert({"tenant_id": tid, "phone": phone, "name": phone}).execute().data[0]["id"]
            # Get most recent open conversation for this contact (avoid duplicates)
            convs = supabase.table("conversations").select("id,unread_count").eq("contact_id", contact_id).neq("status", "resolved").order("created_at", desc=True).limit(1).execute().data
            if convs:
                conv_id = convs[0]["id"]; uc = (convs[0].get("unread_count") or 0) + 1
            else:
                insert_data = {"contact_id": contact_id, "tenant_id": tid, "status": "open", "kanban_stage": "new", "unread_count": 0}
                if instance_name:
                    insert_data["instance_name"] = instance_name
                try:
                    conv = supabase.table("conversations").insert(insert_data).execute().data[0]
                except Exception:
                    conv = supabase.table("conversations").insert({"contact_id": contact_id, "tenant_id": tid, "status": "open"}).execute().data[0]
                conv_id = conv["id"]; uc = 1

            # Evita duplicata por waha_id
            if waha_id:
                try:
                    dup = supabase.table("messages").select("id").eq("waha_id", waha_id).execute().data
                    if dup: continue
                except Exception:
                    pass  # waha_id column may not exist yet, continue saving

            # Salva mensagem — tenta com waha_id primeiro, fallback sem ele
            try:
                supabase.table("messages").insert({"conversation_id": conv_id, "direction": "inbound", "content": content, "type": "text", "waha_id": waha_id or None}).execute()
            except Exception:
                # Fallback: insert sem waha_id caso coluna não exista
                supabase.table("messages").insert({"conversation_id": conv_id, "direction": "inbound", "content": content, "type": "text"}).execute()

            supabase.table("conversations").update({"last_message_at": datetime.utcnow().isoformat(), "unread_count": uc}).eq("id", conv_id).execute()
            # Invalida cache para forçar reload no frontend
            cache_del(f"msgs:{conv_id}")
            cache_del(f"convs:{tid}:open")
            cache_del(f"convs:{tid}:None")
            cache_del(f"convs:{tid}:all")
        return {"ok": True}
    except Exception as e:
        import traceback
        print(f"WEBHOOK ERROR: {e}\n{traceback.format_exc()}")
        return {"ok": False, "error": str(e)}

# ── CONVERSATIONS ────────────────────────────────────────
@app.get("/conversations", dependencies=[Depends(verify_key)])
async def list_conversations(tenant_id: str, status: Optional[str] = None, user_id: Optional[str] = None):
    cache_key = f"convs:{tenant_id}:{status}:{user_id}"
    # Serve stale cache instantly while refreshing in background
    stale = cache_get(cache_key, stale_ok=True)
    if stale and not cache_is_stale(cache_key):
        return {"conversations": stale}  # fresh cache, return immediately
    if stale:
        # Has stale data — return it now, refresh in background
        asyncio.create_task(_refresh_conversations(cache_key, tenant_id, status, user_id))
        return {"conversations": stale}

    def _query():
        # Get user's allowed_instances if user_id provided
        allowed = None
        if user_id:
            u = supabase.table("users").select("allowed_instances,role").eq("id", user_id).maybe_single().execute().data
            if u and u.get("role") != "admin" and u.get("allowed_instances"):
                allowed = u["allowed_instances"]  # list of instance IDs

        q = supabase.table("conversations").select("*, contacts(id,name,phone,tags), users!assigned_to(id,name,avatar_color)").eq("tenant_id", tenant_id)
        if status and status != "all": q = q.eq("status", status)
        convs = q.order("last_message_at", desc=True).limit(100).execute().data
        if not convs:
            return convs

        # Filter by allowed instances if applicable
        if allowed:
            # Get instance names for allowed IDs
            inst_rows = supabase.table("gateway_instances").select("id,instance_name").in_("id", allowed).execute().data
            allowed_names = {r["instance_name"] for r in inst_rows}
            convs = [c for c in convs if c.get("instance_name") in allowed_names]

        # Busca todos os labels de uma vez (sem N+1)
        conv_ids = [c["id"] for c in convs]
        all_cl = supabase.table("conversation_labels").select("conversation_id,label_id").in_("conversation_id", conv_ids).execute().data if conv_ids else []
        # Agrupa label_ids por conversa
        cl_map: dict = {}
        for row in all_cl:
            cl_map.setdefault(row["conversation_id"], []).append(row["label_id"])
        # Busca detalhes dos labels únicos
        all_label_ids = list({r["label_id"] for r in all_cl if r.get("label_id")})
        label_details = {}
        if all_label_ids:
            for lb in supabase.table("labels").select("id,name,color").in_("id", all_label_ids).execute().data:
                label_details[lb["id"]] = lb
        # Monta labels por conversa
        for c in convs:
            c["labels"] = [label_details[lid] for lid in cl_map.get(c["id"], []) if lid in label_details]
        return convs

    convs = await run_sync(_query)
    cache_set(cache_key, convs, ttl=30)
    return {"conversations": convs}

async def _refresh_conversations(cache_key, tenant_id, status, user_id):
    """Background task: silently refresh stale conversations cache."""
    try:
        result = await list_conversations(tenant_id, status, user_id)
        # result already sets cache internally
    except Exception:
        pass


@app.delete("/conversations/{conv_id}", dependencies=[Depends(verify_key)])
async def delete_conversation(conv_id: str):
    """Delete a conversation and all its messages/tasks."""
    try:
        supabase.table("tasks").delete().eq("conversation_id", conv_id).execute()
    except: pass
    try:
        supabase.table("conversation_labels").delete().eq("conversation_id", conv_id).execute()
    except: pass
    try:
        supabase.table("messages").delete().eq("conversation_id", conv_id).execute()
    except: pass
    supabase.table("conversations").delete().eq("id", conv_id).execute()
    return {"ok": True}

@app.get("/conversations/{conv_id}/messages", dependencies=[Depends(verify_key)])
async def get_messages(conv_id: str, before: str = None, limit: int = 50):
    limit = min(limit, 100)
    cache_key = f"msgs:{conv_id}:{before or 'latest'}"
    cached = cache_get(cache_key)
    if cached:
        await run_sync(lambda: supabase.table("conversations").update({"unread_count": 0}).eq("id", conv_id).execute())
        return {"messages": cached}
    q = supabase.table("messages").select("*").eq("conversation_id", conv_id)
    if before:
        q = q.lt("created_at", before)
    msgs = await run_sync(lambda: q.order("created_at", desc=True).limit(limit).execute().data)
    msgs = list(reversed(msgs))
    await run_sync(lambda: supabase.table("conversations").update({"unread_count": 0}).eq("id", conv_id).execute())
    cache_set(cache_key, msgs, ttl=30)
    return {"messages": msgs, "has_more": len(msgs) == limit}

@app.post("/conversations/{conv_id}/messages", dependencies=[Depends(verify_key)])
async def send_message(conv_id: str, body: SendMessage, bg: BackgroundTasks):
    conv = supabase.table("conversations").select("*, contacts(phone)").eq("id", conv_id).single().execute().data
    msg = supabase.table("messages").insert({"conversation_id": conv_id, "direction": "outbound", "content": body.text, "type": "text", "sent_by": body.sent_by, "is_internal_note": body.is_internal_note}).execute().data[0]
    if not body.is_internal_note:
        supabase.table("conversations").update({"last_message_at": datetime.utcnow().isoformat()}).eq("id", conv_id).execute()
        bg.add_task(waha_send_msg, conv["contacts"]["phone"], body.text)
    return {"message": msg}

async def waha_send_msg(phone: str, text: str):
    """Envia mensagem via WAHA — formato correto"""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await client.post(
                f"{WAHA_URL}/api/sendText",
                headers=waha_headers(),
                json={"session": "default", "chatId": f"{phone}@c.us", "text": text}
            )
    except:
        pass

@app.put("/conversations/{conv_id}/assign", dependencies=[Depends(verify_key)])
async def assign_conversation(conv_id: str, body: AssignConversation):
    return supabase.table("conversations").update({"assigned_to": body.user_id, "updated_at": datetime.utcnow().isoformat()}).eq("id", conv_id).execute().data[0]

@app.put("/conversations/{conv_id}/kanban", dependencies=[Depends(verify_key)])
async def update_kanban(conv_id: str, body: UpdateKanban):
    return supabase.table("conversations").update({"kanban_stage": body.stage, "updated_at": datetime.utcnow().isoformat()}).eq("id", conv_id).execute().data[0]

@app.put("/conversations/{conv_id}/resolve", dependencies=[Depends(verify_key)])
async def resolve_conversation(conv_id: str):
    return supabase.table("conversations").update({"status": "resolved", "kanban_stage": "resolved", "updated_at": datetime.utcnow().isoformat()}).eq("id", conv_id).execute().data[0]

@app.put("/conversations/{conv_id}/reopen", dependencies=[Depends(verify_key)])
async def reopen_conversation(conv_id: str):
    return supabase.table("conversations").update({"status": "open", "kanban_stage": "new", "updated_at": datetime.utcnow().isoformat()}).eq("id", conv_id).execute().data[0]

@app.put("/conversations/{conv_id}/pending", dependencies=[Depends(verify_key)])
async def pending_conversation(conv_id: str):
    return supabase.table("conversations").update({"status": "pending", "updated_at": datetime.utcnow().isoformat()}).eq("id", conv_id).execute().data[0]

@app.put("/conversations/{conv_id}/labels", dependencies=[Depends(verify_key)])
async def update_conversation_labels(conv_id: str, body: UpdateLabels):
    supabase.table("conversation_labels").delete().eq("conversation_id", conv_id).execute()
    if body.label_ids:
        supabase.table("conversation_labels").insert([{"conversation_id": conv_id, "label_id": lid} for lid in body.label_ids]).execute()
    return {"ok": True}

# ── CONTACTS ─────────────────────────────────────────────
@app.get("/contacts", dependencies=[Depends(verify_key)])
async def list_contacts(tenant_id: str):
    return {"contacts": supabase.table("contacts").select("*").eq("tenant_id", tenant_id).order("name").execute().data}

# ── TASKS ────────────────────────────────────────────────
@app.get("/tasks/completed", dependencies=[Depends(verify_key)])
async def list_completed_tasks(tenant_id: str, days: int = 7):
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    res = supabase.table("tasks").select("*, conversations(id,tenant_id,contacts(name,phone)), users!assigned_to(name)").eq("done", True).gte("done_at", since).order("done_at", desc=True).execute()
    return {"tasks": [t for t in (res.data or []) if (t.get("conversations") or {}).get("tenant_id") == tenant_id]}

@app.get("/tasks", dependencies=[Depends(verify_key)])
async def list_tasks(tenant_id: str, conversation_id: Optional[str] = None):
    q = supabase.table("tasks").select("*, conversations(id,tenant_id), users!assigned_to(name)").eq("done", False)
    if conversation_id: q = q.eq("conversation_id", conversation_id)
    res = q.order("created_at", desc=True).execute()
    return {"tasks": [t for t in (res.data or []) if (t.get("conversations") or {}).get("tenant_id") == tenant_id]}

@app.post("/tasks", dependencies=[Depends(verify_key)])
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
@app.get("/users", dependencies=[Depends(verify_key)])
async def list_users(tenant_id: str):
    return {"users": supabase.table("users").select("id,name,email,role,avatar_color").eq("tenant_id", tenant_id).eq("is_active", True).execute().data}

# ── QUICK REPLIES ────────────────────────────────────────
@app.get("/quick-replies", dependencies=[Depends(verify_key)])
async def list_quick_replies(tenant_id: str):
    return {"quick_replies": supabase.table("quick_replies").select("*").eq("tenant_id", tenant_id).execute().data}

@app.post("/quick-replies", dependencies=[Depends(verify_key)])
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

    conv = supabase.table("conversations")         .select("ai_summary, tenant_id, tenants(copilot_prompt, name)")         .eq("id", conv_id).single().execute().data

    # Divide em recentes (últimas 15) e antigas
    history_str, recent_msgs, old_msgs = trim_context(all_msgs, max_msgs=15)

    # Resumo das mensagens antigas (gerado 1x, reutilizado sempre)
    existing_summary = conv.get("ai_summary")
    summary = await get_or_generate_summary(conv_id, old_msgs, existing_summary)

    # Monta system prompt
    tenant_data   = conv.get("tenants") or {}
    company_prompt = tenant_data.get("copilot_prompt") or         f"Você é atendente da empresa {tenant_data.get('name', '')}. Seja simpático e objetivo."

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
@app.get("/tenant", dependencies=[Depends(verify_key)])
async def get_tenant(tenant_id: str):
    tenant = supabase.table("tenants").select("id,name,plan,copilot_prompt_summary,copilot_auto_mode,copilot_schedule_start,copilot_schedule_end,ai_credits,ai_credits_reset_at,trial_ends_at").eq("id", tenant_id).single().execute().data
    if tenant:
        credits, plan, limit = get_tenant_credits(tenant_id)
        tenant["ai_credits"] = credits
        tenant["ai_credits_limit"] = limit
        tenant["ai_credits_pct"] = round((credits / limit * 100) if limit > 0 else 100)
    return tenant

@app.get("/credits", dependencies=[Depends(verify_key)])
async def get_credits(tenant_id: str):
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
    # Only update mode/schedule — real prompt is protected and only set via onboarding
    res = supabase.table("tenants").update({"copilot_auto_mode": body.copilot_auto_mode, "copilot_schedule_start": body.copilot_schedule_start, "copilot_schedule_end": body.copilot_schedule_end, "updated_at": datetime.utcnow().isoformat()}).eq("id", body.tenant_id).execute()
    return {"ok": True, "tenant": res.data[0]}

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
                if status == "STOPPED":
                    await client.post(f"{WAHA_URL}/api/sessions/{instance}/start", headers=waha_headers())
                    await asyncio.sleep(4)
                elif status == "FAILED":
                    await client.post(f"{WAHA_URL}/api/sessions/{instance}/restart", headers=waha_headers())
                    await asyncio.sleep(4)

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

@app.get("/whatsapp/tenant-instances", dependencies=[Depends(verify_key)])
async def whatsapp_tenant_instances(tenant_id: str):
    """Lista instâncias do tenant com status real-time"""
    tenant = supabase.table("tenants").select("plan").eq("id", tenant_id).single().execute().data
    plan = tenant.get("plan", "starter")
    max_numbers = PLAN_FEATURES.get(plan, PLAN_FEATURES["starter"])["numbers"]
    db_instances = supabase.table("gateway_instances").select("*").eq("tenant_id", tenant_id).order("created_at").execute().data
    result = []
    for inst in db_instances:
        inst_name = inst.get("instance_name") or inst["id"]
        live_status = {"connected": False, "phone": ""}
        if WAHA_URL:
            try:
                async with httpx.AsyncClient(timeout=5) as client:
                    r = await client.get(f"{WAHA_URL}/api/sessions/{inst_name}", headers=waha_headers())
                    if r.status_code == 200:
                        data = r.json()
                        connected = data.get("status") == "WORKING"
                        me = data.get("me") or {}
                        phone = me.get("id","").replace("@c.us","").replace("@s.whatsapp.net","")
                        live_status = {"connected": connected, "phone": phone}
            except: pass
        result.append({**inst, **live_status})
    return {"instances": result, "max_numbers": max_numbers, "plan": plan}

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
                await client.post(f"{WAHA_URL}/api/sessions", headers=waha_headers(),
                    json={"name": inst_name, "config": {"webhooks": [{"url": f"{os.environ.get('BACKEND_URL','')}/webhook/relay", "events": ["message"]}]}})
        except: pass
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
                try: supabase.table("conversation_labels").delete().in_("conversation_id", conv_ids).execute()
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
async def reset_tenant(body: dict):
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
                try: supabase.table("conversation_labels").delete().in_("conversation_id", chunk).execute()
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

@app.post("/broadcasts", dependencies=[Depends(verify_key)])
async def create_broadcast(body: CreateBroadcast, bg: BackgroundTasks):
    bcast = supabase.table("broadcasts").insert({"tenant_id": body.tenant_id, "name": body.name, "message": body.message, "status": "scheduled" if body.scheduled_at else "pending", "scheduled_at": body.scheduled_at, "interval_min": max(body.interval_min, 60), "interval_max": max(body.interval_max, 90), "total_recipients": len(body.recipients), "sent_count": 0, "failed_count": 0}).execute().data[0]
    if body.recipients:
        supabase.table("broadcast_recipients").insert([{"broadcast_id": bcast["id"], "phone": r["phone"], "name": r.get("name"), "contact_id": r.get("contact_id"), "status": "pending"} for r in body.recipients]).execute()
    if not body.scheduled_at:
        bg.add_task(run_broadcast, bcast["id"], body.interval_min, body.interval_max)
    return {"broadcast": bcast}

@app.get("/broadcasts", dependencies=[Depends(verify_key)])
async def list_broadcasts(tenant_id: str):
    return {"broadcasts": supabase.table("broadcasts").select("*").eq("tenant_id", tenant_id).order("created_at", desc=True).execute().data}

@app.put("/broadcasts/{broadcast_id}/cancel", dependencies=[Depends(verify_key)])
async def cancel_broadcast(broadcast_id: str):
    supabase.table("broadcasts").update({"status": "cancelled"}).eq("id", broadcast_id).execute()
    return {"ok": True}

async def run_broadcast(broadcast_id: str, interval_min: int, interval_max: int):
    supabase.table("broadcasts").update({"status": "sending", "started_at": datetime.utcnow().isoformat()}).eq("id", broadcast_id).execute()
    bcast = supabase.table("broadcasts").select("*").eq("id", broadcast_id).single().execute().data
    recipients = supabase.table("broadcast_recipients").select("*").eq("broadcast_id", broadcast_id).eq("status", "pending").execute().data
    sent = 0; failed = 0
    for rec in recipients:
        if supabase.table("broadcasts").select("status").eq("id", broadcast_id).single().execute().data["status"] == "cancelled": break
        msg = bcast["message"].replace("{nome}", rec.get("name") or "").replace("{telefone}", rec.get("phone") or "")
        ok = False
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post(f"{WAHA_URL}/api/sendText", headers=waha_headers(), json={"session": "default", "chatId": f"{rec['phone']}@c.us", "text": msg})
                ok = r.status_code in [200, 201]
        except: pass
        supabase.table("broadcast_recipients").update({"status": "sent" if ok else "failed", "sent_at": datetime.utcnow().isoformat() if ok else None}).eq("id", rec["id"]).execute()
        if ok: sent += 1
        else: failed += 1
        supabase.table("broadcasts").update({"sent_count": sent, "failed_count": failed}).eq("id", broadcast_id).execute()
        await asyncio.sleep(random.randint(interval_min, interval_max))
    supabase.table("broadcasts").update({"status": "done", "finished_at": datetime.utcnow().isoformat()}).eq("id", broadcast_id).execute()

# ── SCHEDULED MESSAGES ───────────────────────────────────
@app.post("/scheduled-messages", dependencies=[Depends(verify_key)])
async def create_scheduled_message(body: ScheduledMessageCreate):
    return {"scheduled_message": supabase.table("scheduled_messages").insert({"tenant_id": body.tenant_id, "conversation_id": body.conversation_id, "contact_name": body.contact_name, "contact_phone": body.contact_phone, "message": body.message, "scheduled_at": body.scheduled_at, "recurrence": body.recurrence, "status": "pending"}).execute().data[0]}

@app.get("/scheduled-messages", dependencies=[Depends(verify_key)])
async def list_scheduled_messages(tenant_id: str, status: Optional[str] = None):
    q = supabase.table("scheduled_messages").select("*").eq("tenant_id", tenant_id)
    if status: q = q.eq("status", status)
    return {"scheduled_messages": q.order("scheduled_at").execute().data}

@app.delete("/scheduled-messages/{msg_id}", dependencies=[Depends(verify_key)])
async def delete_scheduled_message(msg_id: str):
    supabase.table("scheduled_messages").delete().eq("id", msg_id).execute()
    return {"ok": True}

# ── TRIAL & BILLING ───────────────────────────────────────
@app.get("/tenant/trial-status", dependencies=[Depends(verify_key)])
async def trial_status(tenant_id: str):
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
async def activate_plan(body: dict):
    tenant_id = body.get("tenant_id")
    plan = body.get("plan")
    if plan not in ["starter", "pro", "business"]:
        raise HTTPException(status_code=400, detail="Plano inválido")
    supabase.table("tenants").update({"plan": plan, "is_blocked": False, "activated_at": datetime.utcnow().isoformat()}).eq("id", tenant_id).execute()
    return {"ok": True, "plan": plan}

# ── ONBOARDING INTELIGENTE ────────────────────────────────
@app.post("/onboarding/analyze", dependencies=[Depends(verify_key)])
async def onboarding_analyze(body: dict):
    tenant_id = body.get("tenant_id")
    days = body.get("days", 90)
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="Anthropic API key não configurada")
    tenant = supabase.table("tenants").select("id, name, plan, copilot_prompt, onboarding_last_run").eq("id", tenant_id).single().execute().data
    plan = tenant.get("plan")
    plan_limits = {"pro": 200, "business": 500}
    if plan not in plan_limits:
        raise HTTPException(status_code=403, detail="Onboarding Inteligente disponível apenas nos planos Pro e Business")
    conv_limit = plan_limits[plan]
    # Check and consume 1000 credits — no monthly limit, use as many times as you have credits
    ok, remaining, err = consume_credit(tenant_id, 1000)
    if not ok: raise HTTPException(status_code=402, detail=f"São necessários 1.000 créditos para esta análise. {err}")
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    conversations = supabase.table("conversations").select("id, contacts(name, phone)").eq("tenant_id", tenant_id).gte("created_at", since).limit(conv_limit).execute().data
    if not conversations:
        raise HTTPException(status_code=404, detail="Nenhuma conversa encontrada no período.")
    supabase.table("tenants").update({"onboarding_last_run": datetime.utcnow().isoformat()}).eq("id", tenant_id).execute()
    all_samples = []
    for conv in conversations[:conv_limit]:
        msgs = supabase.table("messages").select("direction, content").eq("conversation_id", conv["id"]).eq("is_internal_note", False).order("created_at").limit(20).execute().data
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
    if len(combined) > 150000: combined = combined[:150000]
    total_convs = len(all_samples)
    analysis_prompt = f"""Você é um especialista em atendimento ao cliente e CRM.\n\nAnalise as conversas abaixo da empresa "{tenant['name']}" e gere um prompt de sistema detalhado para um Co-pilot de IA.\n\nO prompt deve incluir:\n1. Tom de voz\n2. Produtos/serviços\n3. Perguntas frequentes\n4. Fluxo de vendas\n5. Regras importantes\n6. Instruções para o Co-pilot\n\nCONVERSAS ({total_convs} conversas, últimos {days} dias):\n\n{combined}\n\nPROMPT GERADO:"""
    async with httpx.AsyncClient(timeout=120) as client:
        # Onboarding usa GPT-4o para máxima qualidade na geração do prompt
        if OPENAI_API_KEY:
            r = await client.post("https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={"model": "gpt-4o", "max_tokens": 2000,
                      "messages": [{"role": "user", "content": analysis_prompt}]})
            raw_result = r.json()["choices"][0]["message"]["content"]
        else:
            r = await client.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={"model": "claude-sonnet-4-20250514", "max_tokens": 2000, "messages": [{"role": "user", "content": analysis_prompt}]})
            raw_result = None  # handled below
        generated_prompt = r.json()["content"][0]["text"]

    # Save real prompt to DB (never sent to frontend)
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
async def onboarding_save_prompt(body: dict):
    tenant_id = body.get("tenant_id")
    prompt = body.get("prompt")
    supabase.table("tenants").update({"copilot_prompt": prompt, "onboarding_done": True, "updated_at": datetime.utcnow().isoformat()}).eq("id", tenant_id).execute()
    return {"ok": True}

# ── PLAN FEATURES ────────────────────────────────────────
PLAN_FEATURES = {
    "trial":     {"agents": 3,   "numbers": 1,   "ai_credits": 200,   "disparos": True,  "copilot": True,  "onboarding": False, "white_label": False},
    "starter":   {"agents": 3,   "numbers": 1,   "ai_credits": 0,     "disparos": True,  "copilot": False, "onboarding": False, "white_label": False},
    "pro":       {"agents": 8,   "numbers": 2,   "ai_credits": 300,   "disparos": True,  "copilot": True,  "onboarding": True,  "white_label": False},
    "business":  {"agents": 20,  "numbers": 5,   "ai_credits": 1000,  "disparos": True,  "copilot": True,  "onboarding": True,  "white_label": False},
    "enterprise":{"agents": 999, "numbers": 999, "ai_credits": 99999, "disparos": True,  "copilot": True,  "onboarding": True,  "white_label": True},
}

@app.get("/plan/features", dependencies=[Depends(verify_key)])
async def get_plan_features(tenant_id: str):
    tenant = supabase.table("tenants").select("plan").eq("id", tenant_id).single().execute().data
    plan = tenant.get("plan", "trial")
    return {"plan": plan, "features": PLAN_FEATURES.get(plan, PLAN_FEATURES["starter"])}

# ── SYNC HISTÓRICO ────────────────────────────────────────
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
        # ── 1. Fetch chats list ──────────────────────────────────
        chats = []
        for route in [f"{WAHA_URL}/api/chats?session={instance}", f"{WAHA_URL}/api/{instance}/chats"]:
            try:
                r = await client.get(route, headers=waha_headers(), timeout=20)
                if r.status_code == 200:
                    data = r.json()
                    chats = data if isinstance(data, list) else data.get("chats", data.get("data", []))
                    if chats: break
            except: continue
        if not chats:
            raise HTTPException(status_code=502, detail="Não foi possível buscar chats do WAHA.")
        # Ordena por timestamp desc e limita aos 60 mais recentes para não travar
        chats = sorted(chats, key=lambda c: c.get("timestamp", 0), reverse=True)[:60]
        stats["chats"] = len(chats)

        # ── 2. For each chat: upsert contact + conversation + messages ──
        for chat in chats:
            try:
                raw_id = chat.get("id", "")
                if isinstance(raw_id, dict):
                    phone_raw = raw_id.get("user", ""); is_group = raw_id.get("server", "") == "g.us"
                else:
                    phone_raw = str(raw_id); is_group = "@g.us" in phone_raw
                if is_group or chat.get("isGroup"): continue
                phone = "".join(c for c in phone_raw if c.isdigit())
                if not phone or len(phone) < 8: stats["skipped"] += 1; continue
                chat_id = f"{phone}@c.us"
                name = chat.get("name") or phone

                # Upsert contact — always update name if it came from WhatsApp
                existing = supabase.table("contacts").select("id,name").eq("tenant_id", tenant_id).eq("phone", phone).execute().data
                if existing:
                    contact_id = existing[0]["id"]
                    # Update name if currently blank or is just the phone number
                    existing_name = existing[0].get("name","")
                    if name and name != phone and (not existing_name or existing_name == phone):
                        supabase.table("contacts").update({"name": name}).eq("id", contact_id).execute()
                else:
                    contact_id = supabase.table("contacts").insert({"tenant_id": tenant_id, "phone": phone, "name": name}).execute().data[0]["id"]
                    stats["contacts_created"] += 1

                # Upsert conversation
                existing_conv = supabase.table("conversations").select("id,instance_name").eq("tenant_id", tenant_id).eq("contact_id", contact_id).execute().data
                if existing_conv:
                    conv_id = existing_conv[0]["id"]
                    if not existing_conv[0].get("instance_name"):
                        supabase.table("conversations").update({"instance_name": instance}).eq("id", conv_id).execute()
                else:
                    conv_id = supabase.table("conversations").insert({"tenant_id": tenant_id, "contact_id": contact_id, "status": "open", "kanban_stage": "new", "unread_count": chat.get("unreadCount", 0), "instance_name": instance}).execute().data[0]["id"]
                    stats["conversations_created"] += 1
                last_ts = chat.get("timestamp", 0)
                if last_ts:
                    supabase.table("conversations").update({"last_message_at": datetime.utcfromtimestamp(last_ts).isoformat()}).eq("id", conv_id).execute()

                # ── 3. Fetch messages for this chat ──────────────
                msgs_data = []
                for msg_route in [
                    f"{WAHA_URL}/api/messages?session={instance}&chatId={chat_id}&limit=100&downloadMedia=false",
                    f"{WAHA_URL}/api/{instance}/messages?chatId={chat_id}&limit=100",
                    f"{WAHA_URL}/api/chats/{chat_id}/messages?session={instance}&limit=100",
                ]:
                    try:
                        mr = await client.get(msg_route, headers=waha_headers(), timeout=15)
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
                        created_at = datetime.utcfromtimestamp(msg_ts).isoformat() if msg_ts else datetime.utcnow().isoformat()

                        row = {
                            "conversation_id": conv_id,
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

@app.get("/whatsapp/sync-status", dependencies=[Depends(verify_key)])
async def sync_status(tenant_id: str):
    convs = supabase.table("conversations").select("id", count="exact").eq("tenant_id", tenant_id).execute()
    msgs = supabase.table("messages").select("id", count="exact").execute()
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
        for route in [f"{WAHA_URL}/api/chats?session={instance}", f"{WAHA_URL}/api/{instance}/chats"]:
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
            if is_group or chat.get("isGroup"): continue
            phone = "".join(c for c in phone_raw if c.isdigit())
            if not phone or len(phone) < 8: continue
            # Find contact then conversation
            contact = supabase.table("contacts").select("id").eq("tenant_id", tenant_id).eq("phone", phone).maybe_single().execute().data
            if not contact: continue
            conv = supabase.table("conversations").select("id,instance_name").eq("tenant_id", tenant_id).eq("contact_id", contact["id"]).maybe_single().execute().data
            if not conv: continue
            if not conv.get("instance_name"):
                supabase.table("conversations").update({"instance_name": instance}).eq("id", conv["id"]).execute()
                updated += 1
        except: continue
    return {"ok": True, "instance": instance, "chats_checked": len(chats), "updated": updated}

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

@app.get("/auth/invites", dependencies=[Depends(verify_key)])
async def list_invites(admin=Depends(require_admin)):
    invites = supabase.table("invite_codes").select("*, users!created_by(name)").eq("tenant_id", admin["tenant_id"]).order("created_at", desc=True).limit(20).execute().data
    return {"invites": invites}


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
    token = jwt.encode(
        {"user_id": user["id"], "tenant_id": user["tenant_id"], "role": user["role"],
         "exp": datetime.utcnow() + timedelta(days=30)},
        JWT_SECRET, algorithm="HS256"
    )
    return {"token": token, "user": user, "tenant_id": user["tenant_id"]}

# ── RELATÓRIOS / ANALYTICS ────────────────────────────────

@app.get("/reports/messages", dependencies=[Depends(verify_key)])
async def report_messages(tenant_id: str, days: int = 30):
    """Mensagens por dia e por hora — heatmap"""
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    # Get conversations for this tenant
    convs = supabase.table("conversations").select("id").eq("tenant_id", tenant_id).execute().data
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

@app.get("/reports/agents", dependencies=[Depends(verify_key)])
async def report_agents(tenant_id: str, days: int = 30):
    """Performance por atendente"""
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    users = supabase.table("users").select("id,name,email,role").eq("tenant_id", tenant_id).execute().data
    convs = supabase.table("conversations").select("id,assigned_to,status,created_at,last_message_at").eq("tenant_id", tenant_id).gte("created_at", since).execute().data
    
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

@app.get("/reports/broadcasts", dependencies=[Depends(verify_key)])
async def report_broadcasts(tenant_id: str):
    """Relatório de disparos — enviados, entregues e ROI (conversas geradas)"""
    broadcasts = supabase.table("broadcasts").select("*").eq("tenant_id", tenant_id).order("created_at", desc=True).limit(20).execute().data
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

@app.get("/reports/credits", dependencies=[Depends(verify_key)])
async def report_credits(tenant_id: str, days: int = 30):
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

@app.get("/reports/financial-forecast", dependencies=[Depends(verify_key)])
async def report_financial_forecast(tenant_id: str):
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
