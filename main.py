from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional, List
from supabase import create_client, Client
import httpx, os, jwt, bcrypt, asyncio, random
from datetime import datetime, timedelta

app = FastAPI(title="7CRM API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SUPABASE_URL      = os.getenv("SUPABASE_URL")
SUPABASE_KEY      = os.getenv("SUPABASE_SERVICE_KEY")
WAHA_URL          = os.getenv("WAHA_URL", "http://localhost:3000")
WAHA_KEY          = os.getenv("WAHA_KEY", "pulsekey")
INBOX_API_KEY     = os.getenv("INBOX_API_KEY", "7zap_inbox_secret")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
JWT_SECRET        = os.getenv("JWT_SECRET", "7crm_super_secret_change_in_prod")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
security = HTTPBearer(auto_error=False)

def create_jwt(user):
    payload = {"sub": user["id"], "email": user["email"], "role": user["role"], "tenant_id": user["tenant_id"], "name": user["name"], "exp": datetime.utcnow() + timedelta(hours=168)}
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def decode_jwt(token):
    try: return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except: raise HTTPException(status_code=401, detail="Token inválido ou expirado")

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not credentials: raise HTTPException(status_code=401, detail="Token necessário")
    return decode_jwt(credentials.credentials)

async def require_admin(user=Depends(get_current_user)):
    if user.get("role") != "admin": raise HTTPException(status_code=403, detail="Apenas admins")
    return user

def verify_key(x_api_key: str = Header(...)):
    if x_api_key != INBOX_API_KEY: raise HTTPException(status_code=401, detail="Unauthorized")

# ── Schemas ──────────────────────────────────────────────
class LoginRequest(BaseModel):
    email: str
    password: str

class CreateUser(BaseModel):
    tenant_id: str; name: str; email: str; password: str; role: str = "agent"; avatar_color: Optional[str] = "#00c853"

class UpdateUser(BaseModel):
    name: Optional[str] = None; email: Optional[str] = None; password: Optional[str] = None
    role: Optional[str] = None; is_active: Optional[bool] = None; avatar_color: Optional[str] = None

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
    if not bcrypt.checkpw(body.password.encode(), user["password_hash"].encode()): raise HTTPException(status_code=401, detail="Email ou senha incorretos")
    supabase.table("users").update({"last_login": datetime.utcnow().isoformat()}).eq("id", user["id"]).execute()
    return {"token": create_jwt(user), "user": {"id": user["id"], "name": user["name"], "email": user["email"], "role": user["role"], "tenant_id": user["tenant_id"], "avatar_color": user.get("avatar_color", "#00c853")}}

@app.get("/auth/me")
async def me(user=Depends(get_current_user)):
    return supabase.table("users").select("id,name,email,role,tenant_id,avatar_color,last_login").eq("id", user["sub"]).single().execute().data

@app.post("/auth/change-password")
async def change_password(body: dict, user=Depends(get_current_user)):
    new_pw = body.get("new_password", "")
    if len(new_pw) < 6: raise HTTPException(status_code=400, detail="Mínimo 6 caracteres")
    db = supabase.table("users").select("password_hash").eq("id", user["sub"]).single().execute().data
    if not bcrypt.checkpw(body.get("current_password", "").encode(), db["password_hash"].encode()): raise HTTPException(status_code=401, detail="Senha atual incorreta")
    supabase.table("users").update({"password_hash": bcrypt.hashpw(new_pw.encode(), bcrypt.gensalt()).decode()}).eq("id", user["sub"]).execute()
    return {"ok": True}

# ── ADMIN ────────────────────────────────────────────────
@app.get("/admin/users")
async def list_users_admin(admin=Depends(require_admin)):
    return {"users": supabase.table("users").select("id,name,email,role,is_active,avatar_color,last_login,tenant_id").eq("tenant_id", admin["tenant_id"]).order("created_at").execute().data}

@app.post("/admin/users")
async def create_user_admin(body: CreateUser, admin=Depends(require_admin)):
    if supabase.table("users").select("id").eq("email", body.email.lower()).execute().data:
        raise HTTPException(status_code=400, detail="Email já cadastrado")
    pw_hash = bcrypt.hashpw(body.password.encode(), bcrypt.gensalt()).decode()
    user = supabase.table("users").insert({"tenant_id": body.tenant_id, "name": body.name, "email": body.email.lower().strip(), "role": body.role, "password_hash": pw_hash, "is_active": True, "avatar_color": body.avatar_color}).execute().data[0]
    user.pop("password_hash", None)
    return {"user": user}

@app.put("/admin/users/{user_id}")
async def update_user_admin(user_id: str, body: UpdateUser, admin=Depends(require_admin)):
    updates = {k: v for k, v in {"name": body.name, "email": body.email, "role": body.role, "is_active": body.is_active, "avatar_color": body.avatar_color}.items() if v is not None}
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

# ── WEBHOOK ──────────────────────────────────────────────
@app.post("/webhook/message")
async def receive_message(payload: dict):
    data = payload.get("data", {})
    key = data.get("key", {})
    if key.get("fromMe"): return {"ok": True}
    phone = key.get("remoteJid", "").replace("@s.whatsapp.net", "").replace("@g.us", "")
    if not phone: return {"ok": True}
    msg = data.get("message", {})
    content = msg.get("conversation") or (msg.get("extendedTextMessage") or {}).get("text") or ("[Imagem]" if msg.get("imageMessage") else "[Áudio]" if msg.get("audioMessage") else "[Documento]" if msg.get("documentMessage") else "")
    if not content: return {"ok": True}
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
        supabase.table("messages").insert({"conversation_id": conv_id, "direction": "inbound", "content": content, "type": "text"}).execute()
        supabase.table("conversations").update({"last_message_at": datetime.utcnow().isoformat(), "unread_count": uc}).eq("id", conv_id).execute()
    return {"ok": True}

# ── CONVERSATIONS ────────────────────────────────────────
@app.get("/conversations", dependencies=[Depends(verify_key)])
async def list_conversations(tenant_id: str, status: Optional[str] = None):
    q = supabase.table("conversations").select("*, contacts(id,name,phone,tags), users!assigned_to(id,name,avatar_color)").eq("tenant_id", tenant_id)
    if status and status != "all": q = q.eq("status", status)
    convs = q.order("last_message_at", desc=True).limit(200).execute().data
    for c in convs:
        labels_res = supabase.table("conversation_labels").select("labels(id,name,color)").eq("conversation_id", c["id"]).execute().data
        c["labels"] = [r["labels"] for r in labels_res if r.get("labels")]
    return {"conversations": convs}

@app.get("/conversations/{conv_id}/messages", dependencies=[Depends(verify_key)])
async def get_messages(conv_id: str):
    msgs = supabase.table("messages").select("*").eq("conversation_id", conv_id).order("created_at").execute().data
    supabase.table("conversations").update({"unread_count": 0}).eq("id", conv_id).execute()
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
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await client.post(f"{WAHA_URL}/api/sendText", headers={"X-Api-Key": WAHA_KEY}, json={"session": "default", "chatId": f"{phone}@s.whatsapp.net", "text": text})
    except: pass

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
async def ai_suggest(conv_id: str):
    if not ANTHROPIC_API_KEY: raise HTTPException(status_code=503, detail="Anthropic API key não configurada")
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
    return supabase.table("tenants").select("id,name,plan,copilot_prompt").eq("id", tenant_id).single().execute().data

@app.put("/tenant/copilot-prompt", dependencies=[Depends(verify_key)])
async def update_copilot_prompt(body: UpdateCopilotPrompt):
    res = supabase.table("tenants").update({"copilot_prompt": body.copilot_prompt, "updated_at": datetime.utcnow().isoformat()}).eq("id", body.tenant_id).execute()
    return {"ok": True, "tenant": res.data[0]}

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
                r = await client.post(f"{WAHA_URL}/api/sendText", headers={"X-Api-Key": WAHA_KEY}, json={"session": "default", "chatId": f"{rec['phone']}@s.whatsapp.net", "text": msg})
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

# ── WHATSAPP CONNECTION (WAHA) ───────────────────────────
def waha_headers():
    return {"X-Api-Key": WAHA_KEY, "Content-Type": "application/json"}

@app.get("/whatsapp/status", dependencies=[Depends(verify_key)])
async def whatsapp_status(instance: str = "default"):
    if not WAHA_URL:
        raise HTTPException(status_code=503, detail="WAHA não configurada")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{WAHA_URL}/api/sessions/{instance}", headers=waha_headers())
            data = r.json()
            state = data.get("status", "STOPPED")
            connected = state == "WORKING"
            phone = ""
            me = data.get("me", {})
            if me:
                phone = me.get("id", "").replace("@c.us", "").replace("@s.whatsapp.net", "")
            return {"state": state, "connected": connected, "instance": instance, "phone": phone}
    except Exception as e:
        return {"state": "error", "connected": False, "error": str(e)}

@app.get("/whatsapp/qrcode", dependencies=[Depends(verify_key)])
async def whatsapp_qrcode(instance: str = "default"):
    if not WAHA_URL:
        raise HTTPException(status_code=503, detail="WAHA não configurada")
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            # Inicia sessão se não existir
            await client.post(f"{WAHA_URL}/api/sessions/start", headers=waha_headers(), json={"name": instance})
            # Busca QR Code
            r = await client.get(f"{WAHA_URL}/api/{instance}/auth/qr", headers=waha_headers(), params={"format": "image"})
            if r.status_code == 200 and r.headers.get("content-type", "").startswith("image"):
                import base64
                b64 = base64.b64encode(r.content).decode()
                return {"qr_code": f"data:image/png;base64,{b64}", "state": "SCAN_QR_CODE"}
            # Tenta JSON
            r2 = await client.get(f"{WAHA_URL}/api/{instance}/auth/qr", headers=waha_headers())
            data = r2.json()
            return {"qr_code": data.get("value", ""), "state": "SCAN_QR_CODE"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/whatsapp/disconnect", dependencies=[Depends(verify_key)])
async def whatsapp_disconnect(body: dict):
    instance = body.get("instance", "default")
    if not WAHA_URL:
        raise HTTPException(status_code=503, detail="WAHA não configurada")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(f"{WAHA_URL}/api/sessions/stop", headers=waha_headers(), json={"name": instance, "logout": True})
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/whatsapp/instances", dependencies=[Depends(verify_key)])
async def whatsapp_instances():
    if not WAHA_URL:
        raise HTTPException(status_code=503, detail="WAHA não configurada")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{WAHA_URL}/api/sessions", headers=waha_headers())
            return {"instances": r.json()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
        # First access — start trial now
        ends = now + timedelta(days=7)
        supabase.table("tenants").update({
            "trial_ends_at": ends.isoformat(),
            "trial_used": True,
            "plan": "trial"
        }).eq("id", tenant_id).execute()
        return {"status": "trial", "plan": "trial", "is_blocked": False, "days_left": 7, "trial_ends_at": ends.isoformat()}

    ends_dt = datetime.fromisoformat(trial_ends_at.replace("Z", ""))
    days_left = max(0, (ends_dt - now).days)
    expired = now > ends_dt

    if expired and not is_blocked:
        supabase.table("tenants").update({"is_blocked": True}).eq("id", tenant_id).execute()

    return {
        "status": "expired" if expired else "trial",
        "plan": "trial",
        "is_blocked": expired,
        "days_left": days_left,
        "trial_ends_at": trial_ends_at
    }

@app.post("/tenant/activate-plan", dependencies=[Depends(verify_key)])
async def activate_plan(body: dict):
    tenant_id = body.get("tenant_id")
    plan = body.get("plan")  # starter | pro | business
    if plan not in ["starter", "pro", "business"]:
        raise HTTPException(status_code=400, detail="Plano inválido")
    supabase.table("tenants").update({
        "plan": plan,
        "is_blocked": False,
        "activated_at": datetime.utcnow().isoformat()
    }).eq("id", tenant_id).execute()
    return {"ok": True, "plan": plan}

# ── ONBOARDING INTELIGENTE ────────────────────────────────
@app.post("/onboarding/analyze", dependencies=[Depends(verify_key)])
async def onboarding_analyze(body: dict):
    """
    Lê o histórico de conversas do tenant e gera um Co-pilot prompt
    personalizado baseado no tom de voz, produtos e padrões da empresa.
    """
    tenant_id = body.get("tenant_id")
    days = body.get("days", 90)

    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="Anthropic API key não configurada")

    # Busca tenant
    tenant = supabase.table("tenants").select("id, name, plan, copilot_prompt, onboarding_last_run").eq("id", tenant_id).single().execute().data
    plan = tenant.get("plan")

    # Verifica plano
    # Limites por plano: Pro=200 convs, Business=500 convs
    plan_limits = {"pro": 200, "business": 500}
    if plan not in plan_limits:
        raise HTTPException(status_code=403, detail="Onboarding Inteligente disponível apenas nos planos Pro e Business")

    conv_limit = plan_limits[plan]

    # Verifica se já usou no mês atual
    last_run = tenant.get("onboarding_last_run")
    if last_run:
        last_run_dt = datetime.fromisoformat(last_run.replace("Z", ""))
        now = datetime.utcnow()
        if last_run_dt.year == now.year and last_run_dt.month == now.month:
            next_month = (now.replace(day=1) + timedelta(days=32)).replace(day=1)
            days_until = (next_month - now).days
            raise HTTPException(status_code=429, detail=f"Você já usou o Onboarding este mês. Disponível novamente em {days_until} dia(s).")

    # Busca conversas dos últimos X dias
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    conversations = supabase.table("conversations").select("id, contacts(name, phone)").eq("tenant_id", tenant_id).gte("created_at", since).limit(conv_limit).execute().data

    if not conversations:
        raise HTTPException(status_code=404, detail="Nenhuma conversa encontrada no período. Conecte seu WhatsApp e aguarde mensagens chegarem.")

    # Registra uso do mês
    supabase.table("tenants").update({"onboarding_last_run": datetime.utcnow().isoformat()}).eq("id", tenant_id).execute()

    # Busca mensagens de cada conversa (amostra representativa)
    all_samples = []
    for conv in conversations[:conv_limit]:  # limite por plano
        msgs = supabase.table("messages").select("direction, content").eq("conversation_id", conv["id"]).eq("is_internal_note", False).order("created_at").limit(20).execute().data
        if len(msgs) < 2:
            continue
        contact_name = (conv.get("contacts") or {}).get("name", "Cliente")
        sample = f"[Conversa com {contact_name}]\n"
        for m in msgs:
            role = "Atendente" if m["direction"] == "outbound" else "Cliente"
            sample += f"{role}: {m['content']}\n"
        all_samples.append(sample)

    if not all_samples:
        raise HTTPException(status_code=404, detail="Nenhuma conversa com conteúdo suficiente encontrada.")

    # Limita tokens — pega as primeiras N conversas que cabem
    combined = "\n---\n".join(all_samples[:conv_limit])
    if len(combined) > 150000:
        combined = combined[:150000]

    total_convs = len(all_samples)

    # Gera o prompt com Claude
    analysis_prompt = f"""Você é um especialista em atendimento ao cliente e CRM.

Analise as conversas abaixo da empresa "{tenant['name']}" e gere um prompt de sistema detalhado para um Co-pilot de IA que vai ajudar os atendentes dessa empresa.

O prompt deve incluir:
1. **Tom de voz** — como a empresa se comunica (formal/informal, use exemplos reais)
2. **Produtos/serviços** — o que a empresa oferece, preços se mencionados
3. **Perguntas frequentes** — as dúvidas mais comuns dos clientes e como responder
4. **Fluxo de vendas** — como a empresa conduz o processo de venda
5. **Regras importantes** — horários, políticas, informações críticas
6. **Instruções para o Co-pilot** — como ele deve se comportar, o que evitar

Escreva o prompt em primeira pessoa, como se fosse instrução direta para a IA atendente.
Seja específico com exemplos reais das conversas. Não invente informações que não estejam nas conversas.

CONVERSAS DA EMPRESA ({total_convs} conversas analisadas, últimos {days} dias):

{combined}

PROMPT GERADO (escreva apenas o prompt, sem explicações adicionais):"""

    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 2000,
                "messages": [{"role": "user", "content": analysis_prompt}]
            }
        )
        generated_prompt = r.json()["content"][0]["text"]

    return {
        "generated_prompt": generated_prompt,
        "conversations_analyzed": total_convs,
        "days_analyzed": days,
        "tenant_name": tenant["name"]
    }

@app.post("/onboarding/save-prompt", dependencies=[Depends(verify_key)])
async def onboarding_save_prompt(body: dict):
    tenant_id = body.get("tenant_id")
    prompt = body.get("prompt")
    supabase.table("tenants").update({
        "copilot_prompt": prompt,
        "onboarding_done": True,
        "updated_at": datetime.utcnow().isoformat()
    }).eq("id", tenant_id).execute()
    return {"ok": True}

# ── PLAN INFO ────────────────────────────────────────────
PLAN_FEATURES = {
    "trial":    {"agents": 15, "numbers": 3,  "ai_credits": 1000, "onboarding_convs": 200, "disparos": True,  "copilot": True,  "onboarding": False, "white_label": False},
    "starter":  {"agents": 5,  "numbers": 1,  "ai_credits": 0,    "onboarding_convs": 0,   "disparos": True,  "copilot": False, "onboarding": False, "white_label": False},
    "pro":      {"agents": 15, "numbers": 3,  "ai_credits": 1000, "onboarding_convs": 200, "disparos": True,  "copilot": True,  "onboarding": True,  "white_label": False},
    "business": {"agents": 30, "numbers": 8,  "ai_credits": 3000, "onboarding_convs": 500, "disparos": True,  "copilot": True,  "onboarding": True,  "white_label": True},
    "enterprise":{"agents": 999,"numbers": 999,"ai_credits": 99999,"onboarding_convs": 999,"disparos": True,  "copilot": True,  "onboarding": True,  "white_label": True},
}

@app.get("/plan/features", dependencies=[Depends(verify_key)])
async def get_plan_features(tenant_id: str):
    tenant = supabase.table("tenants").select("plan").eq("id", tenant_id).single().execute().data
    plan = tenant.get("plan", "trial")
    features = PLAN_FEATURES.get(plan, PLAN_FEATURES["starter"])
    return {"plan": plan, "features": features}
