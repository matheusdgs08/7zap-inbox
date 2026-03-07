from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks
import concurrent.futures
_thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)

async def run_sync(fn):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_thread_pool, fn)

# ── CACHE SIMPLES ─────────────────────────────────────────
_cache: dict = {}
def cache_get(key):
    v = _cache.get(key)
    if v and (datetime.utcnow().timestamp() - v["ts"]) < v["ttl"]:
        return v["data"]
    return None
def cache_set(key, data, ttl=20):
    _cache[key] = {"data": data, "ts": datetime.utcnow().timestamp(), "ttl": ttl}
def cache_del(key):
    _cache.pop(key, None)
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
    tenant_id: str; name: str; email: str; password: str; role: str = "agent"; avatar_color: Optional[str] = "#00c853"; permissions: str = "read_write"

class UpdateUser(BaseModel):
    name: Optional[str] = None; email: Optional[str] = None; password: Optional[str] = None
    role: Optional[str] = None; is_active: Optional[bool] = None; avatar_color: Optional[str] = None; permissions: Optional[str] = None

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
    return {"token": create_jwt(user, session_id), "user": {"id": user["id"], "name": user["name"], "email": user["email"], "role": user["role"], "tenant_id": user["tenant_id"], "avatar_color": user.get("avatar_color", "#00c853")}}

@app.get("/auth/me")
async def me(user=Depends(get_current_user)):
    return supabase.table("users").select("id,name,email,role,tenant_id,avatar_color,last_login").eq("id", user["sub"]).single().execute().data

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
    return {"users": supabase.table("users").select("id,name,email,role,is_active,avatar_color,last_login,tenant_id,permissions").eq("tenant_id", admin["tenant_id"]).order("created_at").execute().data}

@app.post("/admin/users")
async def create_user_admin(body: CreateUser, admin=Depends(require_admin)):
    if supabase.table("users").select("id").eq("email", body.email.lower()).execute().data:
        raise HTTPException(status_code=400, detail="Email já cadastrado")
    pw_hash = bcrypt.hashpw(body.password.encode(), bcrypt.gensalt()).decode()
    user = supabase.table("users").insert({"tenant_id": body.tenant_id, "name": body.name, "email": body.email.lower().strip(), "role": body.role, "password_hash": pw_hash, "is_active": True, "avatar_color": body.avatar_color, "permissions": body.permissions}).execute().data[0]
    user.pop("password_hash", None)
    return {"user": user}

@app.put("/admin/users/{user_id}")
async def update_user_admin(user_id: str, body: UpdateUser, admin=Depends(require_admin)):
    updates = {k: v for k, v in {"name": body.name, "email": body.email, "role": body.role, "is_active": body.is_active, "avatar_color": body.avatar_color, "permissions": body.permissions}.items() if v is not None}
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

    tenants = supabase.table("tenants").select("id").execute().data
    for tenant in tenants:
        tid = tenant["id"]
        contacts = supabase.table("contacts").select("id").eq("tenant_id", tid).eq("phone", phone).execute().data
        contact_id = contacts[0]["id"] if contacts else supabase.table("contacts").insert({"tenant_id": tid, "phone": phone, "name": phone}).execute().data[0]["id"]
        convs = supabase.table("conversations").select("id,unread_count").eq("contact_id", contact_id).neq("status", "resolved").execute().data
        if convs:
            conv_id = convs[0]["id"]; uc = (convs[0].get("unread_count") or 0) + 1
        else:
            conv = supabase.table("conversations").insert({"contact_id": contact_id, "tenant_id": tid, "status": "open", "kanban_stage": "new", "unread_count": 0}).execute().data[0]
            conv_id = conv["id"]; uc = 1

        # Evita duplicata por waha_id
        if waha_id:
            dup = supabase.table("messages").select("id").eq("waha_id", waha_id).execute().data
            if dup: continue

        supabase.table("messages").insert({"conversation_id": conv_id, "direction": "inbound", "content": content, "type": "text", "waha_id": waha_id or None}).execute()
        supabase.table("conversations").update({"last_message_at": datetime.utcnow().isoformat(), "unread_count": uc}).eq("id", conv_id).execute()
        # Invalida cache para forçar reload no frontend
        cache_del(f"msgs:{conv_id}")
        cache_del(f"convs:{tid}:open")
        cache_del(f"convs:{tid}:None")
        cache_del(f"convs:{tid}:all")
    return {"ok": True}

# ── CONVERSATIONS ────────────────────────────────────────
@app.get("/conversations", dependencies=[Depends(verify_key)])
async def list_conversations(tenant_id: str, status: Optional[str] = None):
    cache_key = f"convs:{tenant_id}:{status}"
    cached = cache_get(cache_key)
    if cached:
        return {"conversations": cached}

    def _query():
        q = supabase.table("conversations").select("*, contacts(id,name,phone,tags), users!assigned_to(id,name,avatar_color)").eq("tenant_id", tenant_id)
        if status and status != "all": q = q.eq("status", status)
        convs = q.order("last_message_at", desc=True).limit(200).execute().data
        if not convs:
            return convs
        # Busca todos os labels de uma vez (sem N+1)
        conv_ids = [c["id"] for c in convs]
        all_cl = supabase.table("conversation_labels").select("conversation_id,label_id").in_("conversation_id", conv_ids).execute().data
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
    cache_set(cache_key, convs, ttl=20)
    return {"conversations": convs}

@app.get("/conversations/{conv_id}/messages", dependencies=[Depends(verify_key)])
async def get_messages(conv_id: str):
    cache_key = f"msgs:{conv_id}"
    cached = cache_get(cache_key)
    if cached:
        await run_sync(lambda: supabase.table("conversations").update({"unread_count": 0}).eq("id", conv_id).execute())
        return {"messages": cached}
    msgs = await run_sync(lambda: supabase.table("messages").select("*").eq("conversation_id", conv_id).order("created_at").execute().data)
    await run_sync(lambda: supabase.table("conversations").update({"unread_count": 0}).eq("id", conv_id).execute())
    cache_set(cache_key, msgs, ttl=10)
    return {"messages": msgs}

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
    if not ANTHROPIC_API_KEY: raise HTTPException(status_code=503, detail="Anthropic API key não configurada")
    # Get tenant_id from conversation if not provided
    if not tenant_id:
        conv_data = supabase.table("conversations").select("tenant_id").eq("id", conv_id).single().execute().data
        tenant_id = conv_data.get("tenant_id") if conv_data else None
    if tenant_id:
        ok, remaining, err = consume_credit(tenant_id, 1)
        if not ok: raise HTTPException(status_code=402, detail=err)
    msgs = supabase.table("messages").select("direction,content,is_internal_note").eq("conversation_id", conv_id).order("created_at", desc=True).limit(10).execute().data
    msgs = [m for m in reversed(msgs) if not m.get("is_internal_note")]
    conv = supabase.table("conversations").select("*, tenants(copilot_prompt,name)").eq("id", conv_id).single().execute().data
    system_prompt = conv["tenants"].get("copilot_prompt") or f"Você é atendente da empresa {conv['tenants']['name']}. Seja simpático e objetivo."
    history = "\n".join([f"{'Cliente' if m['direction']=='inbound' else 'Atendente'}: {m['content']}" for m in msgs])
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 300, "system": system_prompt + "\n\nSugira apenas a próxima resposta, sem explicações. Breve e natural.",
                  "messages": [{"role": "user", "content": f"Histórico:\n{history}\n\nSugira a próxima resposta do atendente:"}]})
        return {"suggestion": r.json()["content"][0]["text"]}

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

@app.get("/whatsapp/qrcode", dependencies=[Depends(verify_key)])
async def whatsapp_qrcode(instance: str = "default"):
    """Retorna QR Code para conexão via WAHA screenshot"""
    if not WAHA_URL:
        raise HTTPException(status_code=503, detail="WAHA não configurada")
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            # Verifica se já está conectado
            r = await client.get(f"{WAHA_URL}/api/sessions/{instance}", headers=waha_headers())
            if r.status_code == 200:
                data = r.json()
                status = data.get("status", "")
                if status == "WORKING":
                    me = data.get("me") or {}
                    phone = me.get("id", "").replace("@c.us", "")
                    return {"qr_code": "", "state": "open", "connected": True, "phone": phone}
                # Se FAILED, reinicia
                if status == "FAILED":
                    await client.post(f"{WAHA_URL}/api/sessions/{instance}/restart", headers=waha_headers())
                    await asyncio.sleep(5)

            # Pega screenshot com QR Code
            for _ in range(5):
                screenshot = await client.get(f"{WAHA_URL}/api/screenshot?session={instance}", headers=waha_headers())
                if screenshot.status_code == 200 and screenshot.content:
                    b64 = base64.b64encode(screenshot.content).decode()
                    return {"qr_code": f"data:image/png;base64,{b64}", "state": "SCAN_QR_CODE"}
                await asyncio.sleep(3)

            return {"qr_code": "", "state": "timeout", "error": "QR Code não disponível"}
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

# ── BROADCASTS ───────────────────────────────────────────
@app.post("/broadcasts/suggest-message", dependencies=[Depends(verify_key)])
async def suggest_broadcast_message(body: dict):
    if not ANTHROPIC_API_KEY: raise HTTPException(status_code=503, detail="Anthropic API key não configurada")
    tenant = supabase.table("tenants").select("copilot_prompt,name").eq("id", body.get("tenant_id")).single().execute().data
    system = (tenant.get("copilot_prompt") or f"Você é atendente da empresa {tenant['name']}.") + "\n\nCrie mensagens de WhatsApp curtas e naturais. Use {nome} para personalizar."
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 400, "system": system,
                  "messages": [{"role": "user", "content": f"Crie uma mensagem de disparo para WhatsApp com objetivo: {body.get('objective', '')}\n\nRetorne apenas a mensagem."}]})
        return {"suggestion": r.json()["content"][0]["text"]}

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
        r = await client.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 2000, "messages": [{"role": "user", "content": analysis_prompt}]})
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

    async with httpx.AsyncClient(timeout=30) as client2:
        r2 = await client2.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 300,
                  "messages": [{"role": "user", "content": summary_prompt}]})
        summary = r2.json()["content"][0]["text"]

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
    "trial":    {"agents": 15, "numbers": 3,  "ai_credits": 1000, "disparos": True,  "copilot": True,  "onboarding": False, "white_label": False},
    "starter":  {"agents": 5,  "numbers": 1,  "ai_credits": 0,    "disparos": True,  "copilot": False, "onboarding": False, "white_label": False},
    "pro":      {"agents": 15, "numbers": 3,  "ai_credits": 1000, "disparos": True,  "copilot": True,  "onboarding": True,  "white_label": False},
    "business": {"agents": 30, "numbers": 8,  "ai_credits": 3000, "disparos": True,  "copilot": True,  "onboarding": True,  "white_label": True},
    "enterprise":{"agents":999,"numbers": 999,"ai_credits":99999,  "disparos": True,  "copilot": True,  "onboarding": True,  "white_label": True},
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
    plan_days = {"pro": 90, "business": 180, "trial": 90, "enterprise": 365}
    if plan == "starter":
        raise HTTPException(status_code=403, detail="Sincronização disponível nos planos Pro e Business.")
    days_limit = plan_days.get(plan, 90)
    since_ts = int((datetime.utcnow() - timedelta(days=days_limit)).timestamp() * 1000)
    stats = {"chats": 0, "contacts_created": 0, "conversations_created": 0, "messages_saved": 0, "skipped": 0}
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
        stats["chats"] = len(chats)
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
                name = chat.get("name") or phone
                existing = supabase.table("contacts").select("id").eq("tenant_id", tenant_id).eq("phone", phone).execute().data
                contact_id = existing[0]["id"] if existing else supabase.table("contacts").insert({"tenant_id": tenant_id, "phone": phone, "name": name}).execute().data[0]["id"]
                if not existing: stats["contacts_created"] += 1
                existing_conv = supabase.table("conversations").select("id").eq("tenant_id", tenant_id).eq("contact_id", contact_id).execute().data
                conv_id = existing_conv[0]["id"] if existing_conv else supabase.table("conversations").insert({"tenant_id": tenant_id, "contact_id": contact_id, "status": "open", "kanban_stage": "new", "unread_count": chat.get("unreadCount", 0)}).execute().data[0]["id"]
                if not existing_conv: stats["conversations_created"] += 1
                last_ts = chat.get("timestamp", 0)
                if last_ts:
                    supabase.table("conversations").update({"last_message_at": datetime.utcfromtimestamp(last_ts).isoformat()}).eq("id", conv_id).execute()
            except: stats["skipped"] += 1; continue
    return {"ok": True, "stats": stats}

@app.get("/whatsapp/sync-status", dependencies=[Depends(verify_key)])
async def sync_status(tenant_id: str):
    convs = supabase.table("conversations").select("id", count="exact").eq("tenant_id", tenant_id).execute()
    msgs = supabase.table("messages").select("id", count="exact").execute()
    return {"conversations": convs.count, "messages": msgs.count}

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
    """Relatório de disparos — enviados vs respondidos"""
    broadcasts = supabase.table("broadcasts").select("*").eq("tenant_id", tenant_id).order("created_at", desc=True).limit(20).execute().data
    result = []
    for b in broadcasts:
        sent = b.get("recipients_count", 0)
        delivered = b.get("delivered_count", 0)
        # Count replies: messages inbound after broadcast creation from broadcast contacts
        result.append({
            "id": b["id"], "message": (b.get("message","") or "")[:80],
            "created_at": b.get("created_at"), "status": b.get("status"),
            "sent": sent, "delivered": delivered,
            "reply_rate": round(delivered/sent*100) if sent > 0 else 0
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
        "cost_estimate": round(len(ai_msgs) * 0.0007, 2),
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
