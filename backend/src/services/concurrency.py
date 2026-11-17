"""Real PostgreSQL row-lock experiment. Events coordinate two database connections; no timing simulation.

Cancellation protocol: the API flips status to 'cancelling'; a watcher thread notices,
pg_cancel_backend interrupts B's lock wait, and A rolls back its uncommitted receipt and
balance update instead of committing. Terminal writes are fenced by run_token so an
expired executor can never overwrite a final state.
"""
import threading,time,hashlib,json,uuid
from sqlalchemy import text,select,update
from src.config.database import engine,Session
from src.models.entities import Trial,Transfer,TrialBalance,Receipt,Sample

class Cancelled(Exception): pass

def _checksum(payload): return hashlib.sha256(json.dumps(payload,sort_keys=True,ensure_ascii=False).encode()).hexdigest()

def execute(trial_id):
    token=uuid.uuid4().hex
    with Session() as db:
        claimed=db.execute(update(Trial).where(Trial.id==trial_id,Trial.status=='queued').values(status='running',run_token=token)).rowcount
        db.commit()
        if claimed!=1: return None  # 排队时已取消（或他处领取），不再执行
        trial=db.get(Trial,trial_id);transfer=db.get(Transfer,trial.transfer_id);sample=db.get(Sample,transfer.sample_id)
        timeout=trial.timeout_ms;qty=transfer.quantity;initial=sample.quantity
    acquired=threading.Event();contender_done=threading.Event();cancel_event=threading.Event();stop_watch=threading.Event()
    traces=[];errors=[];pids={};outcome={'committed':False}
    def watcher():
        while not stop_watch.wait(0.1):
            try:
                with engine.connect() as c:
                    st=c.execute(text('SELECT status FROM trials WHERE id=:id'),{'id':trial_id}).scalar()
                    if st!='cancelling': continue
                    cancel_event.set()
                    # 先终止 B 的锁等待，A 再回滚，B 绝无机会拿到锁
                    while not contender_done.wait(0.05):
                        pid=pids.get('B')
                        if pid: c.execute(text('SELECT pg_cancel_backend(:pid)'),{'pid':pid})
                    return
            except Exception: pass
    def holder():
        try:
            with engine.connect() as c:
                tx=c.begin()
                try:
                    pid=c.execute(text('SELECT pg_backend_pid()')).scalar();pids['A']=pid
                    c.execute(text('SELECT remaining FROM trial_balances WHERE trial_id=:id FOR UPDATE'),{'id':trial_id}).one()
                    traces.append({'actor':'A','pid':pid,'event':'row_lock_acquired'})
                    # A receipt made before B starts is uncommitted, and B cannot read it.
                    c.execute(text("INSERT INTO receipts(trial_id,actor,quantity) VALUES (:id,'A',:qty)"),{'id':trial_id,'qty':qty})
                    acquired.set()
                    deadline=time.monotonic()+12
                    while not contender_done.wait(0.05):
                        if time.monotonic()>deadline: raise RuntimeError('竞争事务未在保护期限内结束')
                    if cancel_event.is_set(): raise Cancelled()
                    changed=c.execute(text('UPDATE trial_balances SET remaining=remaining-:qty,received=received+:qty WHERE trial_id=:id AND remaining>=:qty'),{'id':trial_id,'qty':qty})
                    if changed.rowcount!=1: raise RuntimeError('可用量校验失败')
                    if cancel_event.is_set(): raise Cancelled()
                    tx.commit();outcome['committed']=True;traces.append({'actor':'A','pid':pid,'event':'committed','quantity':qty})
                except Cancelled:
                    tx.rollback()  # 未提交回执与演练账在同一事务内一起回滚，原样本量不变
                    traces.append({'actor':'A','pid':pids.get('A'),'event':'cancelled_rollback'})
        except Exception as e: errors.append(str(e));acquired.set();contender_done.set()
    def contender():
        try:
            if not acquired.wait(5): raise RuntimeError('持锁事务未就绪')
            with engine.connect() as c:
                tx=c.begin();pid=c.execute(text('SELECT pg_backend_pid()')).scalar();pids['B']=pid
                c.execute(text(f"SET LOCAL lock_timeout = '{int(timeout)}ms'"))
                c.execute(text("INSERT INTO receipts(trial_id,actor,quantity) VALUES (:id,'B',:qty)"),{'id':trial_id,'qty':qty})
                t=time.perf_counter()
                try:
                    c.execute(text('SELECT remaining FROM trial_balances WHERE trial_id=:id FOR UPDATE'),{'id':trial_id}).one()
                    raise RuntimeError('竞争连接意外获得锁')
                except Exception as e:
                    state=getattr(getattr(e,'orig',None),'sqlstate',None)
                    wait_ms=round((time.perf_counter()-t)*1000,2)
                    tx.rollback()
                    if state=='55P03':
                        traces.append({'actor':'B','pid':pid,'event':'rolled_back','sqlstate':state,'wait_ms':wait_ms,'receipt_insert_rolled_back':True})
                    elif state=='57014' and cancel_event.is_set():
                        traces.append({'actor':'B','pid':pid,'event':'cancelled_rollback','sqlstate':state,'wait_ms':wait_ms,'receipt_insert_rolled_back':True})
                    else: raise
        except Exception as e: errors.append(str(e))
        finally: contender_done.set()
    w=threading.Thread(target=watcher,daemon=True);a=threading.Thread(target=holder);b=threading.Thread(target=contender)
    w.start();a.start();b.start();a.join(15);b.join(15);stop_watch.set()
    try:
        if a.is_alive() or b.is_alive(): raise RuntimeError('并发试验未及时结束')
        if errors: raise RuntimeError(';'.join(errors))
        with Session() as db:
            balance=db.get(TrialBalance,trial_id)
            receipts=list(db.scalars(select(Receipt).where(Receipt.trial_id==trial_id)))
            base={'mode':'真实 PostgreSQL / 隔离样本镜像账本','sample':sample.code,'initial':initial,'transfer_quantity':qty,'remaining':balance.remaining,'received':balance.received,'receipt_count':len(receipts),'traces':traces,'sql':['SELECT ... FOR UPDATE','SET LOCAL lock_timeout','INSERT INTO receipts','UPDATE trial_balances','ROLLBACK / COMMIT']}
            if outcome['committed']:
                invariant=balance.remaining+balance.received==initial and balance.remaining>=0 and len(receipts)==1 and receipts[0].actor=='A'
                result=base|{'invariant_passed':invariant,'formula':'remaining + received = initial；A 回执 1 条，B 回执回滚为 0 条'}
                status='completed' if invariant else 'failed';error='' if invariant else '不变量校验未通过'
            else:
                rolled_back=len(receipts)==0 and balance.received==0 and balance.remaining==initial
                reason=db.get(Trial,trial_id).cancel_reason
                result=base|{'cancelled':True,'reason':reason,'invariant_passed':rolled_back,'formula':'取消后 remaining = initial、received = 0、回执 0 条；未提交回执与演练账一并回滚，原样本量不变'}
                status='cancelled' if rolled_back else 'failed';error='' if rolled_back else '取消后回执或演练账未完全回滚'
            result['checksum']=_checksum(result)
            # 围栏：仅当前执行者、且仍处于运行/取消中才允许写终态
            res=db.execute(update(Trial).where(Trial.id==trial_id,Trial.run_token==token,Trial.status.in_(['running','cancelling'])).values(status=status,result=result,error=error))
            db.commit()
            return result if res.rowcount==1 else None  # 过期执行者不得改写终态
    except Exception as e:
        with Session() as db:
            db.execute(update(Trial).where(Trial.id==trial_id,Trial.run_token==token,Trial.status.in_(['running','cancelling'])).values(status='failed',error=str(e)))
            db.commit()
        return None

def recover_interrupted():
    """执行进程中断后的启动恢复：cancelling 续办取消，running 标记失败。条件更新，不覆盖终态。"""
    recovered=[]
    with Session() as db:
        stale=[(t.id,t.status,t.run_token,t.transfer_id) for t in db.scalars(select(Trial).where(Trial.status.in_(['cancelling','running'])))]
    if not stale: return recovered
    try:
        with engine.connect() as c:
            # 残留连接防御性清理；进程死亡通常已使连接断开并回滚其未提交事务
            for pid in list(c.execute(text("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND state<>'idle' AND query LIKE '%trial_balances%'")).scalars()):
                c.execute(text('SELECT pg_terminate_backend(:pid)'),{'pid':pid})
    except Exception: pass
    for ident,status,token,transfer_id in stale:
        with Session() as db:
            balance=db.get(TrialBalance,ident)
            receipts=list(db.scalars(select(Receipt).where(Receipt.trial_id==ident)))
            transfer=db.get(Transfer,transfer_id);sample=db.get(Sample,transfer.sample_id)
            clean=balance is not None and len(receipts)==0 and balance.received==0 and balance.remaining==sample.quantity
            if status=='cancelling':
                trial=db.get(Trial,ident)
                result={'mode':'真实 PostgreSQL / 隔离样本镜像账本','cancelled':True,'recovered':True,'reason':trial.cancel_reason,'sample':sample.code,'initial':sample.quantity,'transfer_quantity':transfer.quantity,'remaining':balance.remaining if balance else None,'received':balance.received if balance else None,'receipt_count':len(receipts),'traces':[],'sql':['SELECT ... FOR UPDATE','SET LOCAL lock_timeout','INSERT INTO receipts','ROLLBACK'],'invariant_passed':clean,'formula':'取消后 remaining = initial、received = 0、回执 0 条；执行进程中断后由恢复流程确认回滚'}
                result['checksum']=_checksum(result)
                values={'status':'cancelled' if clean else 'failed','result':result if clean else None,'error':'' if clean else '恢复时回执或演练账未完全回滚','run_token':''}
            else:
                values={'status':'failed','error':'执行进程中断，未提交事务已随连接断开回滚，请重新发起演练','run_token':''}
            res=db.execute(update(Trial).where(Trial.id==ident,Trial.status==status,Trial.run_token==token).values(**values))
            db.commit()
            if res.rowcount==1: recovered.append({'id':ident,'from':status,'to':values['status']})
    return recovered
