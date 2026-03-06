from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional, List
from supabase import create_client, Client
import httpx, os, jwt, bcrypt
from datetime import datetime, timedelta

app = FastAPI(title="7zap Inbox API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

SUPABASE_URL      = os.getenv("SUPABASE_URL")
SUPABASE_KEY      = os.getenv("SUPABASE_SERVICE_KEY")
WAHA_URL          = os.getenv("WAHA_URL", "http://localhost:3000")
WAHA_KEY          = os.getenv("WAHA_KEY", "pulsekey")
INBOX_API_KEY     = os.getenv("INBOX_API_KEY", "7zap_inbox_secret")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
JWT_SECRET        = os.getenv("JWT_SECRET", "7crm_super_secret_change_in_prod")
JWT_EXPIRE_HOURS  = 24 * 7  # 7 days

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
security = HTTPBearer(auto_error=False)

def create_jwt(user: dict) -> str:
    payload = {
        "sub": user["id"],
        "email": user["email"],
        "role": user["role"],
        "tenant_id": user["tenant_id"],
        "name": user["name"],
        "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRE_HOURS)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def decode_jwt(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except Exception:
        raise HTTPException(status_code=401, detail="Token inválido ou expirado")

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not credentials:
        raise HTTPException(status_code=401, detail="Token necessário")
    return decode_jwt(credentials.credentials)

async def require_admin(user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Acesso restrito a administradores")
    return user


# ── Schemas ─────────────────────────────────────────────
class LoginRequest(BaseModel):
    email: str
    password: str


class CreateUser(BaseModel):
    tenant_id: str
    name: str
    email: str
    password: str
    role: str = "agent"
    avatar_color: Optional[str] = "#00c853"


class UpdateUser(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    password: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None
    avatar_color: Optional[str] = None

# ── Schemas ─────────────────────────────────────────────

class SendMessage(BaseModel):
    conversation_id: str
    text: str
    sent_by: Optional[str] = None
    is_internal_note: Optional[bool] = False


class AssignConversation(BaseModel):
    user_id: Optional[str] = None


class UpdateKanban(BaseModel):
    stage: str


class UpdateLabels(BaseModel):
    labels: List[dict]


class CreateTask(BaseModel):
    title: str
    description: Optional[str] = None
    assigned_to: Optional[str] = None
    due_at: Optional[str] = None


class CreateTaskUpdate(BaseModel):
    content: str
    created_by: Optional[str] = None


class CreateQuickReply(BaseModel):
    title: str
    content: str


class UpdateCopilotPrompt(BaseModel):
    tenant_id: str
    copilot_prompt: str

# ── Health ───────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "service": "7zap-inbox"}

# ── WEBHOOK ──────────────────────────────────────────────
@app.post("/webhook/inbox")
async def webhook_inbox(payload: dict, background_tasks: BackgroundTasks):
    event = payload.get("event", "")
    if event in ("message", "message.any"):
        background_tasks.add_task(process_message, payload)
    return {"received": True}

async def process_message(payload: dict):
    try:
        session = payload.get("session", "default")
        msg = payload.get("payload", {})
        if msg.get("fromMe"):
            return
        from_id = msg.get("from", "")
        phone = from_id.replace("@c.us", "").replace("@lid", "")
        push_name = msg.get("_data", {}).get("notifyName", "") or msg.get("pushName", "")
        body = msg.get("body", "")
        waha_msg_id = msg.get("id", {}).get("_serialized", "") if isinstance(msg.get("id"), dict) else str(msg.get("id", ""))
        msg_type = msg.get("type", "chat")
        if not phone or not body:
            return
        tenant_res = supabase.table("tenants").select("*").eq("waha_session", session).single().execute()
        if not tenant_res.data:
            return
        tenant = tenant_res.data
        tenant_id = tenant["id"]
        contact_res = supabase.table("contacts").upsert({
            "tenant_id": tenant_id, "phone": phone,
            "push_name": push_name or phone, "name": push_name or phone,
            "updated_at": datetime.utcnow().isoformat(),
        }, on_conflict="tenant_id,phone").execute()
        contact_id = contact_res.data[0]["id"]
        conv_res = supabase.table("conversations").select("*").eq("tenant_id", tenant_id).eq("contact_id", contact_id).in_("status", ["open", "pending"]).order("created_at", desc=True).limit(1).execute()
        if conv_res.data:
            conversation = conv_res.data[0]
            conv_id = conversation["id"]
            supabase.table("conversations").update({
                "last_message_at": datetime.utcnow().isoformat(),
                "unread_count": conversation["unread_count"] + 1,
                "updated_at": datetime.utcnow().isoformat(),
            }).eq("id", conv_id).execute()
        else:
            conv_id = supabase.table("conversations").insert({
                "tenant_id": tenant_id, "contact_id": contact_id,
                "status": "open", "kanban_stage": "new",
                "last_message_at": datetime.utcnow().isoformat(), "unread_count": 1,
            }).execute().data[0]["id"]
        existing = supabase.table("messages").select("id").eq("waha_message_id", waha_msg_id).execute()
        if not existing.data:
            supabase.table("messages").insert({
                "conversation_id": conv_id, "tenant_id": tenant_id,
                "waha_message_id": waha_msg_id, "direction": "inbound",
                "content": body, "type": msg_type,
            }).execute()
        print(f"[INBOX] ✅ {tenant['name']} | {phone} | {body[:50]}")
    except Exception as e:
        print(f"[INBOX] ❌ {e}")

# ── CONVERSATIONS ────────────────────────────────────────
@app.get("/conversations", dependencies=[Depends(verify_key)])
async def list_conversations(tenant_id: str, status: Optional[str] = None, stage: Optional[str] = None):
    query = supabase.table("conversations").select("*, contacts(name, phone, push_name), users!assigned_to(name, email)").eq("tenant_id", tenant_id).order("last_message_at", desc=True)
    if status: query = query.eq("status", status)
    if stage: query = query.eq("kanban_stage", stage)
    res = query.execute()
    convs = []
    for c in (res.data or []):
        c["assigned_agent"] = (c.get("users") or {}).get("name")
        if not isinstance(c.get("labels"), list): c["labels"] = []
        convs.append(c)
    return {"conversations": convs}

@app.get("/conversations/{conv_id}", dependencies=[Depends(verify_key)])
async def get_conversation(conv_id: str):
    return supabase.table("conversations").select("*, contacts(*), users!assigned_to(name, email)").eq("id", conv_id).single().execute().data

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
async def update_labels(conv_id: str, body: UpdateLabels):
    return supabase.table("conversations").update({"labels": body.labels, "updated_at": datetime.utcnow().isoformat()}).eq("id", conv_id).execute().data[0]

# ── MESSAGES ─────────────────────────────────────────────
@app.get("/conversations/{conv_id}/messages", dependencies=[Depends(verify_key)])
async def get_messages(conv_id: str):
    supabase.table("conversations").update({"unread_count": 0}).eq("id", conv_id).execute()
    return {"messages": supabase.table("messages").select("*, users!sent_by(name)").eq("conversation_id", conv_id).order("created_at").execute().data}

@app.post("/conversations/{conv_id}/messages", dependencies=[Depends(verify_key)])
async def send_message(conv_id: str, body: SendMessage):
    conv = supabase.table("conversations").select("*, contacts(phone), tenants(waha_session)").eq("id", conv_id).single().execute().data
    if not body.is_internal_note:
        await waha_send(conv["tenants"]["waha_session"], f"{conv['contacts']['phone']}@c.us", body.text)
    res = supabase.table("messages").insert({
        "conversation_id": conv_id, "tenant_id": conv["tenant_id"],
        "direction": "outbound", "content": body.text, "type": "chat",
        "sent_by": body.sent_by, "is_internal_note": body.is_internal_note or False,
    }).execute()
    supabase.table("conversations").update({"last_message_at": datetime.utcnow().isoformat(), "updated_at": datetime.utcnow().isoformat()}).eq("id", conv_id).execute()
    return res.data[0]

# ── TASKS ─────────────────────────────────────────────────
@app.get("/tasks", dependencies=[Depends(verify_key)])
async def list_tasks(tenant_id: str, assigned_to: Optional[str] = None):
    query = supabase.table("tasks").select("*, conversations(id, tenant_id), users!assigned_to(name)").eq("done", False)
    if assigned_to: query = query.eq("assigned_to", assigned_to)
    res = query.order("due_at").execute()
    return {"tasks": [t for t in (res.data or []) if (t.get("conversations") or {}).get("tenant_id") == tenant_id]}

@app.post("/conversations/{conv_id}/tasks", dependencies=[Depends(verify_key)])
async def create_task(conv_id: str, body: CreateTask, tenant_id: str):
    res = supabase.table("tasks").insert({
        "conversation_id": conv_id, "title": body.title,
        "description": body.description or None,
        "assigned_to": body.assigned_to or None,
        "due_at": body.due_at or None, "done": False,
    }).execute()
    return {"task": res.data[0]}

@app.put("/tasks/{task_id}/done", dependencies=[Depends(verify_key)])
async def complete_task(task_id: str):
    supabase.table("tasks").update({"done": True, "done_at": datetime.utcnow().isoformat()}).eq("id", task_id).execute()
    return {"ok": True}

@app.get("/tasks/completed", dependencies=[Depends(verify_key)])
async def list_completed_tasks(tenant_id: str, days: int = 7):
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    res = supabase.table("tasks").select("*, conversations(id, tenant_id, contacts(name, phone)), users!assigned_to(name)").eq("done", True).gte("done_at", since).order("done_at", desc=True).execute()
    return {"tasks": [t for t in (res.data or []) if (t.get("conversations") or {}).get("tenant_id") == tenant_id]}

# ── TASK UPDATES ──────────────────────────────────────────
@app.get("/tasks/{task_id}/updates", dependencies=[Depends(verify_key)])
async def get_task_updates(task_id: str):
    return {"updates": supabase.table("task_updates").select("*").eq("task_id", task_id).order("created_at").execute().data or []}

@app.post("/tasks/{task_id}/updates", dependencies=[Depends(verify_key)])
async def add_task_update(task_id: str, body: CreateTaskUpdate):
    res = supabase.table("task_updates").insert({
        "task_id": task_id, "content": body.content,
        "created_by": body.created_by or "Atendente",
    }).execute()
    return {"update": res.data[0]}

# ── USERS ─────────────────────────────────────────────────
@app.get("/users", dependencies=[Depends(verify_key)])
async def list_users(tenant_id: str):
    return {"users": supabase.table("users").select("id, name, email, role").eq("tenant_id", tenant_id).execute().data}

# ── QUICK REPLIES ─────────────────────────────────────────
@app.get("/quick-replies", dependencies=[Depends(verify_key)])
async def list_quick_replies(tenant_id: str):
    return {"quick_replies": supabase.table("quick_replies").select("*").eq("tenant_id", tenant_id).execute().data}

@app.post("/quick-replies", dependencies=[Depends(verify_key)])
async def create_quick_reply(body: CreateQuickReply, tenant_id: str):
    return supabase.table("quick_replies").insert({"tenant_id": tenant_id, "title": body.title, "content": body.content}).execute().data[0]

# ── CO-PILOT IA ───────────────────────────────────────────
@app.get("/conversations/{conv_id}/suggest", dependencies=[Depends(verify_key)])
async def ai_suggest(conv_id: str):
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="Anthropic API key não configurada")
    msgs = supabase.table("messages").select("direction, content, is_internal_note").eq("conversation_id", conv_id).order("created_at", desc=True).limit(10).execute().data
    msgs = [m for m in reversed(msgs) if not m.get("is_internal_note")]
    conv = supabase.table("conversations").select("*, tenants(copilot_prompt, name)").eq("id", conv_id).single().execute().data
    system_prompt = conv["tenants"].get("copilot_prompt") or f"Você é um atendente da empresa {conv['tenants']['name']}. Seja simpático e objetivo."
    history = "\n".join([f"{'Cliente' if m['direction']=='inbound' else 'Atendente'}: {m['content']}" for m in msgs])
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 300,
                  "system": system_prompt + "\n\nSugira apenas a próxima resposta do atendente, sem explicações. Seja breve e natural.",
                  "messages": [{"role": "user", "content": f"Histórico:\n{history}\n\nSugira a próxima resposta do atendente:"}]},
        )
        suggestion = r.json()["content"][0]["text"]
    supabase.table("messages").update({"ai_suggestion": suggestion}).eq("conversation_id", conv_id).eq("direction", "inbound").execute()
    return {"suggestion": suggestion}

# ── TENANT CONFIG ─────────────────────────────────────────
@app.get("/tenant", dependencies=[Depends(verify_key)])
async def get_tenant(tenant_id: str):
    return supabase.table("tenants").select("id, name, plan, copilot_prompt").eq("id", tenant_id).single().execute().data

@app.put("/tenant/copilot-prompt", dependencies=[Depends(verify_key)])
async def update_copilot_prompt(body: UpdateCopilotPrompt):
    res = supabase.table("tenants").update({"copilot_prompt": body.copilot_prompt, "updated_at": datetime.utcnow().isoformat()}).eq("id", body.tenant_id).execute()
    return {"ok": True, "tenant": res.data[0]}

# ── BROADCASTS ────────────────────────────────────────────
import asyncio, random


class CreateBroadcast(BaseModel):
    tenant_id: str
    name: str
    message: str
    interval_min: int = 60
    interval_max: int = 120
    scheduled_at: Optional[str] = None
    recipients: List[dict]  # [{phone, name, contact_id?}]


class ScheduledMessageCreate(BaseModel):
    tenant_id: str
    conversation_id: Optional[str] = None
    contact_name: Optional[str] = None
    contact_phone: str
    message: str
    scheduled_at: str
    recurrence: Optional[str] = None  # daily, weekly, monthly

@app.post("/broadcasts", dependencies=[Depends(verify_key)])
async def create_broadcast(body: CreateBroadcast, bg: BackgroundTasks):
    bcast = supabase.table("broadcasts").insert({
        "tenant_id": body.tenant_id,
        "name": body.name,
        "message": body.message,
        "status": "scheduled" if body.scheduled_at else "pending",
        "scheduled_at": body.scheduled_at,
        "interval_min": max(body.interval_min, 60),
        "interval_max": max(body.interval_max, 90),
        "total_recipients": len(body.recipients),
        "sent_count": 0,
        "failed_count": 0,
    }).execute().data[0]

    rows = [{"broadcast_id": bcast["id"], "phone": r["phone"], "name": r.get("name"), "contact_id": r.get("contact_id"), "status": "pending"} for r in body.recipients]
    if rows:
        supabase.table("broadcast_recipients").insert(rows).execute()

    # If no scheduled time, start immediately
    if not body.scheduled_at:
        bg.add_task(run_broadcast, bcast["id"], body.interval_min, body.interval_max)

    return {"broadcast": bcast}

@app.get("/broadcasts", dependencies=[Depends(verify_key)])
async def list_broadcasts(tenant_id: str):
    return {"broadcasts": supabase.table("broadcasts").select("*").eq("tenant_id", tenant_id).order("created_at", desc=True).execute().data}

@app.get("/broadcasts/{broadcast_id}/recipients", dependencies=[Depends(verify_key)])
async def list_recipients(broadcast_id: str):
    return {"recipients": supabase.table("broadcast_recipients").select("*").eq("broadcast_id", broadcast_id).execute().data}

@app.put("/broadcasts/{broadcast_id}/cancel", dependencies=[Depends(verify_key)])
async def cancel_broadcast(broadcast_id: str):
    supabase.table("broadcasts").update({"status": "cancelled"}).eq("id", broadcast_id).execute()
    return {"ok": True}

async def run_broadcast(broadcast_id: str, interval_min: int, interval_max: int):
    """Background task: sends messages with random delay between each"""
    supabase.table("broadcasts").update({"status": "sending", "started_at": datetime.utcnow().isoformat()}).eq("id", broadcast_id).execute()
    recipients = supabase.table("broadcast_recipients").select("*").eq("broadcast_id", broadcast_id).eq("status", "pending").execute().data
    bcast = supabase.table("broadcasts").select("*").eq("id", broadcast_id).single().execute().data

    for rec in recipients:
        # Check if cancelled
        current = supabase.table("broadcasts").select("status").eq("id", broadcast_id).single().execute().data
        if current["status"] == "cancelled":
            break

        message = bcast["message"].replace("{nome}", rec.get("name") or "").replace("{telefone}", rec.get("phone") or "")
        success = False
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post(
                    f"{WAHA_URL}/message/sendText",
                    headers={"apikey": WAHA_KEY},
                    json={"session": "default", "chatId": f"{rec['phone']}@s.whatsapp.net", "text": message}
                )
                success = r.status_code == 201
        except Exception as e:
            pass

        supabase.table("broadcast_recipients").update({
            "status": "sent" if success else "failed",
            "sent_at": datetime.utcnow().isoformat() if success else None,
            "error": None if success else "Falha no envio"
        }).eq("id", rec["id"]).execute()

        if success:
            supabase.table("broadcasts").update({"sent_count": bcast["sent_count"] + 1}).eq("id", broadcast_id).execute()
            bcast["sent_count"] += 1
        else:
            supabase.table("broadcasts").update({"failed_count": bcast["failed_count"] + 1}).eq("id", broadcast_id).execute()
            bcast["failed_count"] += 1

        # Random delay between messages (seconds)
        delay = random.randint(interval_min, interval_max)
        await asyncio.sleep(delay)

    supabase.table("broadcasts").update({"status": "done", "finished_at": datetime.utcnow().isoformat()}).eq("id", broadcast_id).execute()

# ── SCHEDULED MESSAGES ────────────────────────────────────
@app.post("/scheduled-messages", dependencies=[Depends(verify_key)])
async def create_scheduled_message(body: ScheduledMessageCreate):
    row = supabase.table("scheduled_messages").insert({
        "tenant_id": body.tenant_id,
        "conversation_id": body.conversation_id,
        "contact_name": body.contact_name,
        "contact_phone": body.contact_phone,
        "message": body.message,
        "scheduled_at": body.scheduled_at,
        "recurrence": body.recurrence,
        "status": "pending",
    }).execute().data[0]
    return {"scheduled_message": row}

@app.get("/scheduled-messages", dependencies=[Depends(verify_key)])
async def list_scheduled_messages(tenant_id: str, status: Optional[str] = None):
    q = supabase.table("scheduled_messages").select("*").eq("tenant_id", tenant_id)
    if status:
        q = q.eq("status", status)
    return {"scheduled_messages": q.order("scheduled_at").execute().data}

@app.delete("/scheduled-messages/{msg_id}", dependencies=[Depends(verify_key)])
async def delete_scheduled_message(msg_id: str):
    supabase.table("scheduled_messages").delete().eq("id", msg_id).execute()
    return {"ok": True}

# ── AI MESSAGE SUGGESTION FOR BROADCAST ──────────────────
@app.post("/broadcasts/suggest-message", dependencies=[Depends(verify_key)])
async def suggest_broadcast_message(body: dict):
    tenant_id = body.get("tenant_id")
    objective = body.get("objective", "")
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="Anthropic API key não configurada")
    tenant = supabase.table("tenants").select("copilot_prompt, name").eq("id", tenant_id).single().execute().data
    system = tenant.get("copilot_prompt") or f"Você é um atendente da empresa {tenant['name']}."
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 400,
                  "system": system + "\n\nCrie mensagens de WhatsApp curtas, naturais e sem parecer spam. Você pode usar {nome} para personalizar.",
                  "messages": [{"role": "user", "content": f"Crie uma mensagem de disparo em massa para WhatsApp com o objetivo: {objective}\n\nRetorne apenas a mensagem, sem explicações."}]},
        )
        return {"suggestion": r.json()["content"][0]["text"]}


def verify_key(x_api_key: str = Header(...)):
    if x_api_key != INBOX_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# ── AUTH ENDPOINTS ───────────────────────────────────────
@app.post("/auth/login")
async def login(body: LoginRequest):
    users = supabase.table("users").select("*").eq("email", body.email.lower().strip()).eq("is_active", True).execute().data
    if not users:
        raise HTTPException(status_code=401, detail="Email ou senha incorretos")
    user = users[0]
    if not user.get("password_hash"):
        raise HTTPException(status_code=401, detail="Usuário sem senha definida")
    if not bcrypt.checkpw(body.password.encode(), user["password_hash"].encode()):
        raise HTTPException(status_code=401, detail="Email ou senha incorretos")
    supabase.table("users").update({"last_login": datetime.utcnow().isoformat()}).eq("id", user["id"]).execute()
    token = create_jwt(user)
    return {
        "token": token,
        "user": {"id": user["id"], "name": user["name"], "email": user["email"], "role": user["role"], "tenant_id": user["tenant_id"], "avatar_color": user.get("avatar_color", "#00c853")}
    }

@app.get("/auth/me")
async def me(user=Depends(get_current_user)):
    db_user = supabase.table("users").select("id, name, email, role, tenant_id, avatar_color, last_login").eq("id", user["sub"]).single().execute().data
    return db_user

@app.post("/auth/change-password")
async def change_password(body: dict, user=Depends(get_current_user)):
    current = body.get("current_password", "")
    new_pw = body.get("new_password", "")
    if len(new_pw) < 6:
        raise HTTPException(status_code=400, detail="Senha deve ter pelo menos 6 caracteres")
    db_user = supabase.table("users").select("password_hash").eq("id", user["sub"]).single().execute().data
    if not bcrypt.checkpw(current.encode(), db_user["password_hash"].encode()):
        raise HTTPException(status_code=401, detail="Senha atual incorreta")
    new_hash = bcrypt.hashpw(new_pw.encode(), bcrypt.gensalt()).decode()
    supabase.table("users").update({"password_hash": new_hash}).eq("id", user["sub"]).execute()
    return {"ok": True}

# ── USER MANAGEMENT (admin only) ─────────────────────────
@app.get("/admin/users")
async def list_users_admin(admin=Depends(require_admin)):
    return {"users": supabase.table("users").select("id, name, email, role, is_active, avatar_color, last_login, tenant_id").eq("tenant_id", admin["tenant_id"]).order("created_at").execute().data}

@app.post("/admin/users")
async def create_user_admin(body: CreateUser, admin=Depends(require_admin)):
    existing = supabase.table("users").select("id").eq("email", body.email.lower()).execute().data
    if existing:
        raise HTTPException(status_code=400, detail="Email já cadastrado")
    pw_hash = bcrypt.hashpw(body.password.encode(), bcrypt.gensalt()).decode()
    user = supabase.table("users").insert({
        "tenant_id": body.tenant_id,
        "name": body.name,
        "email": body.email.lower().strip(),
        "role": body.role,
        "password_hash": pw_hash,
        "is_active": True,
        "avatar_color": body.avatar_color,
    }).execute().data[0]
    user.pop("password_hash", None)
    return {"user": user}

@app.put("/admin/users/{user_id}")
async def update_user_admin(user_id: str, body: UpdateUser, admin=Depends(require_admin)):
    updates = {}
    if body.name is not None: updates["name"] = body.name
    if body.email is not None: updates["email"] = body.email.lower().strip()
    if body.role is not None: updates["role"] = body.role
    if body.is_active is not None: updates["is_active"] = body.is_active
    if body.avatar_color is not None: updates["avatar_color"] = body.avatar_color
    if body.password is not None:
        if len(body.password) < 6:
            raise HTTPException(status_code=400, detail="Senha deve ter pelo menos 6 caracteres")
        updates["password_hash"] = bcrypt.hashpw(body.password.encode(), bcrypt.gensalt()).decode()
    supabase.table("users").update(updates).eq("id", user_id).execute()
    return {"ok": True}

@app.delete("/admin/users/{user_id}")
async def delete_user_admin(user_id: str, admin=Depends(require_admin)):
    if user_id == admin["sub"]:
        raise HTTPException(status_code=400, detail="Você não pode excluir sua própria conta")
    supabase.table("users").update({"is_active": False}).eq("id", user_id).execute()
    return {"ok": True}

async def waha_send(session: str, chat_id: str, text: str):
    async with httpx.AsyncClient(timeout=15) as client:
        await client.post(
            f"{WAHA_URL}/api/sendText",
            headers={"X-Api-Key": WAHA_KEY},
            json={"session": session, "chatId": chat_id, "text": text},
        )

# ── All Schemas ──────────────────────────────────────────
