# ─── ADICIONAR NO main.py ────────────────────────────────────────────────────
# Cole esses 2 endpoints junto com os outros endpoints do arquivo


# ── Pydantic model (adicionar junto dos outros models) ──
class CreateTask(BaseModel):
    title: str
    assigned_to: Optional[str] = None
    due_at: Optional[str] = None


# ── Endpoint 1: Listar tarefas do tenant ──
@app.get("/tasks")
async def get_tasks(tenant_id: str, api_key: str = Depends(verify_api_key)):
    try:
        result = (
            supabase.table("tasks")
            .select("*, users(name), conversations(tenant_id)")
            .eq("conversations.tenant_id", tenant_id)
            .eq("done", False)
            .order("due_at", desc=False)
            .execute()
        )
        # Filtra só as tarefas que pertencem ao tenant
        tasks = [t for t in (result.data or []) if t.get("conversations")]
        return {"tasks": tasks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Endpoint 2: Criar tarefa vinculada a uma conversa ──
@app.post("/conversations/{conversation_id}/tasks")
async def create_task(
    conversation_id: str,
    body: CreateTask,
    tenant_id: str,
    api_key: str = Depends(verify_api_key)
):
    try:
        task = {
            "id": str(uuid.uuid4()),
            "conversation_id": conversation_id,
            "title": body.title,
            "assigned_to": body.assigned_to,
            "due_at": body.due_at,
            "done": False,
        }
        result = supabase.table("tasks").insert(task).execute()
        return {"task": result.data[0] if result.data else task}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Endpoint 3: Marcar tarefa como concluída ──
@app.put("/tasks/{task_id}/done")
async def complete_task(task_id: str, api_key: str = Depends(verify_api_key)):
    try:
        supabase.table("tasks").update({"done": True}).eq("id", task_id).execute()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
