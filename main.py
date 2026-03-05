from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from supabase import create_client, Client
import httpx, os, json, uuid
from datetime import datetime

app = FastAPI(title="7zap Inbox API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Config ──────────────────────────────────────────────
SUPABASE_URL      = os.getenv("SUPABASE_URL")
SUPABASE_KEY      = os.getenv("SUPABASE_SERVICE_KEY")
WAHA_URL          = os.getenv("WAHA_URL", "http://localhost:3000")
WAHA_KEY          = os.getenv("WAHA_KEY", "pulsekey")
INBOX_API_KEY     = os.getenv("INBOX_API_KEY", "7zap_inbox_secret")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def verify_key(x_api_key: str = Header(...)):
    if x_api_key != INBOX_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

async def waha_send(session: str, chat_id: str, text: str):
    async with httpx.AsyncClient(timeout=15) as client:
        await client.post(
            f"{WAHA_URL}/api/sendText",
            headers={"X-Api-Key": WAHA_KEY},
            json={"session": session, "chatId": chat_id, "text": text},
        )

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
    assigned_to: Optional[str] = None
    due_at: Optional[str] = None

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
            print(f"[INBOX] Tenant não encontrado para sessão: {session}")
            return

        tenant = tenant_res.data
        tenant_id = tenant["id"]

        contact_res = supabase.table("contacts").upsert({
            "tenant_id": tenant_id,
            "phone": phone,
            "push_name": push_name or phone,
            "name": push_name or phone,
            "updated_at": datetime.utcnow().isoformat(),
        }, on_conflict="tenant_id,phone").execute()
        contact = contact_res.data[0]
        contact_id = contact["id"]

        conv_res = supabase.table("conversations")\
            .select("*")\
            .eq("tenant_id", tenant_id)\
            .eq("contact_id", contact_id)\
            .in_("status", ["open", "pending"])\
            .order("created_at", desc=True)\
            .limit(1)\
            .execute()

        if conv_res.data:
            conversation = conv_res.data[0]
            conv_id = conversation["id"]
            supabase.table("conversations").update({
                "last_message_at": datetime.utcnow().isoformat(),
                "unread_count": conversation["unread_count"] + 1,
                "updated_at": datetime.utcnow().isoformat(),
            }).eq("id", conv_id).execute()
        else:
            new_conv = supabase.table("conversations").insert({
                "tenant_id": tenant_id,
                "contact_id": contact_id,
                "status": "open",
                "kanban_stage": "new",
                "last_message_at": datetime.utcnow().isoformat(),
                "unread_count": 1,
            }).execute()
            conv_id = new_conv.data[0]["id"]

        existing = supabase.table("messages").select("id").eq("waha_message_id", waha_msg_id).execute()
        if not existing.data:
            supabase.table("messages").insert({
                "conversation_id": conv_id,
                "tenant_id": tenant_id,
                "waha_message_id": waha_msg_id,
                "direction": "inbound",
                "content": body,
                "type": msg_type,
            }).execute()

        print(f"[INBOX] ✅ Mensagem salva — tenant={tenant['name']} phone={phone} msg={body[:50]}")

    except Exception as e:
        print(f"[INBOX] ❌ Erro ao processar mensagem: {e}")

# ── CONVERSATIONS ────────────────────────────────────────
@app.get("/conversations", dependencies=[Depends(verify_key)])
async def list_conversations(tenant_id: str, status: Optional[str] = None, stage: Optional[str] = None):
    query = supabase.table("conversations")\
        .select("*, contacts(name, phone, push_name), users!assigned_to(name, email)")\
        .eq("tenant_id", tenant_id)\
        .order("last_message_at", desc=True)
    if status:
        query = query.eq("status", status)
    if stage:
        query = query.eq("kanban_stage", stage)
    res = query.execute()

    convs = []
    for c in (res.data or []):
        c["assigned_agent"] = (c.get("users") or {}).get("name")
        if not isinstance(c.get("labels"), list):
            c["labels"] = []
        convs.append(c)
    return {"conversations": convs}

@app.get("/conversations/{conv_id}", dependencies=[Depends(verify_key)])
async def get_conversation(conv_id: str):
    res = supabase.table("conversations")\
        .select("*, contacts(*), users!assigned_to(name, email)")\
        .eq("id", conv_id).single().execute()
    return res.data

@app.put("/conversations/{conv_id}/assign", dependencies=[Depends(verify_key)])
async def assign_conversation(conv_id: str, body: AssignConversation):
    res = supabase.table("conversations").update({
        "assigned_to": body.user_id,
        "updated_at": datetime.utcnow().isoformat(),
    }).eq("id", conv_id).execute()
    return res.data[0]

@app.put("/conversations/{conv_id}/kanban", dependencies=[Depends(verify_key)])
async def update_kanban(conv_id: str, body: UpdateKanban):
    res = supabase.table("conversations").update({
        "kanban_stage": body.stage,
        "updated_at": datetime.utcnow().isoformat(),
    }).eq("id", conv_id).execute()
    return res.data[0]

@app.put("/conversations/{conv_id}/resolve", dependencies=[Depends(verify_key)])
async def resolve_conversation(conv_id: str):
    res = supabase.table("conversations").update({
        "status": "resolved",
        "kanban_stage": "resolved",
        "updated_at": datetime.utcnow().isoformat(),
    }).eq("id", conv_id).execute()
    return res.data[0]

@app.put("/conversations/{conv_id}/labels", dependencies=[Depends(verify_key)])
async def update_labels(conv_id: str, body: UpdateLabels):
    res = supabase.table("conversations").update({
        "labels": body.labels,
        "updated_at": datetime.utcnow().isoformat(),
    }).eq("id", conv_id).execute()
    return res.data[0]

# ── MESSAGES ─────────────────────────────────────────────
@app.get("/conversations/{conv_id}/messages", dependencies=[Depends(verify_key)])
async def get_messages(conv_id: str):
    supabase.table("conversations").update({"unread_count": 0}).eq("id", conv_id).execute()
    res = supabase.table("messages")\
        .select("*, users!sent_by(name)")\
        .eq("conversation_id", conv_id)\
        .order("created_at").execute()
    return {"messages": res.data}

@app.post("/conversations/{conv_id}/messages", dependencies=[Depends(verify_key)])
async def send_message(conv_id: str, body: SendMessage):
    conv = supabase.table("conversations")\
        .select("*, contacts(phone), tenants(waha_session)")\
        .eq("id", conv_id).single().execute().data

    # Notas internas NÃO são enviadas pelo WhatsApp
    if not body.is_internal_note:
        phone = conv["contacts"]["phone"]
        session = conv["tenants"]["waha_session"]
        chat_id = f"{phone}@c.us"
        await waha_send(session, chat_id, body.text)

    res = supabase.table("messages").insert({
        "conversation_id": conv_id,
        "tenant_id": conv["tenant_id"],
        "direction": "outbound",
        "content": body.text,
        "type": "chat",
        "sent_by": body.sent_by,
        "is_internal_note": body.is_internal_note or False,
    }).execute()

    supabase.table("conversations").update({
        "last_message_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    }).eq("id", conv_id).execute()

    return res.data[0]

# ── TASKS ─────────────────────────────────────────────────
@app.get("/tasks", dependencies=[Depends(verify_key)])
async def list_tasks(tenant_id: str, assigned_to: Optional[str] = None):
    query = supabase.table("tasks")\
        .select("*, conversations(id, tenant_id), users!assigned_to(name)")\
        .eq("done", False)
    if assigned_to:
        query = query.eq("assigned_to", assigned_to)
    res = query.order("due_at").execute()
    tasks = [t for t in (res.data or []) if (t.get("conversations") or {}).get("tenant_id") == tenant_id]
    return {"tasks": tasks}

@app.post("/conversations/{conv_id}/tasks", dependencies=[Depends(verify_key)])
async def create_task(conv_id: str, body: CreateTask, tenant_id: str):
    res = supabase.table("tasks").insert({
        "conversation_id": conv_id,
        "title": body.title,
        "assigned_to": body.assigned_to or None,
        "due_at": body.due_at or None,
        "done": False,
    }).execute()
    return {"task": res.data[0]}

@app.put("/tasks/{task_id}/done", dependencies=[Depends(verify_key)])
async def complete_task(task_id: str):
    supabase.table("tasks").update({"done": True}).eq("id", task_id).execute()
    return {"ok": True}

# ── USERS ─────────────────────────────────────────────────
@app.get("/users", dependencies=[Depends(verify_key)])
async def list_users(tenant_id: str):
    res = supabase.table("users").select("id, name, email, role").eq("tenant_id", tenant_id).execute()
    return {"users": res.data}

# ── QUICK REPLIES ─────────────────────────────────────────
@app.get("/quick-replies", dependencies=[Depends(verify_key)])
async def list_quick_replies(tenant_id: str):
    res = supabase.table("quick_replies").select("*").eq("tenant_id", tenant_id).execute()
    return {"quick_replies": res.data}

@app.post("/quick-replies", dependencies=[Depends(verify_key)])
async def create_quick_reply(body: CreateQuickReply, tenant_id: str):
    res = supabase.table("quick_replies").insert({
        "tenant_id": tenant_id,
        "title": body.title,
        "content": body.content,
    }).execute()
    return res.data[0]

# ── CO-PILOT IA ───────────────────────────────────────────
@app.get("/conversations/{conv_id}/suggest", dependencies=[Depends(verify_key)])
async def ai_suggest(conv_id: str):
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="Anthropic API key não configurada")

    msgs = supabase.table("messages")\
        .select("direction, content, created_at, is_internal_note")\
        .eq("conversation_id", conv_id)\
        .order("created_at", desc=True)\
        .limit(10)\
        .execute().data
    msgs.reverse()

    # Ignora notas internas no contexto do co-pilot
    msgs = [m for m in msgs if not m.get("is_internal_note")]

    conv = supabase.table("conversations")\
        .select("*, tenants(copilot_prompt, name)")\
        .eq("id", conv_id).single().execute().data

    system_prompt = conv["tenants"].get("copilot_prompt") or \
        f"Você é um atendente da empresa {conv['tenants']['name']}. Seja simpático e objetivo."

    history = "\n".join([
        f"{'Cliente' if m['direction']=='inbound' else 'Atendente'}: {m['content']}"
        for m in msgs
    ])

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 300,
                "system": system_prompt + "\n\nSugira apenas a próxima resposta do atendente, sem explicações. Seja breve e natural.",
                "messages": [{"role": "user", "content": f"Histórico:\n{history}\n\nSugira a próxima resposta do atendente:"}],
            }
        )
        data = r.json()
        suggestion = data["content"][0]["text"]

    supabase.table("messages")\
        .update({"ai_suggestion": suggestion})\
        .eq("conversation_id", conv_id)\
        .eq("direction", "inbound")\
        .execute()

    return {"suggestion": suggestion}

# ── TENANT CONFIG ─────────────────────────────────────────
@app.get("/tenant", dependencies=[Depends(verify_key)])
async def get_tenant(tenant_id: str):
    res = supabase.table("tenants")\
        .select("id, name, plan, copilot_prompt")\
        .eq("id", tenant_id).single().execute()
    return res.data

@app.put("/tenant/copilot-prompt", dependencies=[Depends(verify_key)])
async def update_copilot_prompt(body: UpdateCopilotPrompt):
    res = supabase.table("tenants").update({
        "copilot_prompt": body.copilot_prompt,
        "updated_at": datetime.utcnow().isoformat(),
    }).eq("id", body.tenant_id).execute()
    return {"ok": True, "tenant": res.data[0]}
