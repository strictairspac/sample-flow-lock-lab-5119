"""取消流程端到端验证：可确认取消、持锁期间取消、提交竞争、排队不领取、中断恢复、围栏与审计幂等。

用法：backend/worker/db/redis 均已运行后，在 backend 目录执行
    python -m scripts.verify_cancel
环境变量：API_BASE（默认 http://localhost:19242，后端裸端口，勿经 nginx /api 前缀）、
          WORKER_RESTART_CMD（默认 'python3 -m src.worker'，用于中断恢复用例重启 worker）。
"""
import os,sys,json,time,shlex,subprocess,urllib.request,urllib.error

API=os.getenv('API_BASE','http://localhost:19242')
BACKEND_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PASSED=[];FAILED=[]

def check(name,cond,detail=''):
    (PASSED if cond else FAILED).append(name)
    print(('  ✓ ' if cond else '  ✗ ')+name+((' — '+str(detail)) if (detail and not cond) else ''))

def call(method,path,token=None,body=None):
    req=urllib.request.Request(API+path,method=method,data=json.dumps(body).encode() if body is not None else None,
        headers={'Content-Type':'application/json',**({'Authorization':'Bearer '+token} if token else {})})
    try:
        with urllib.request.urlopen(req,timeout=15) as r: return r.status,json.loads(r.read())
    except urllib.error.HTTPError as e:
        try: return e.code,json.loads(e.read())
        except Exception: return e.code,{}

def wait_status(tid,token,want,timeout=20):
    deadline=time.time()+timeout
    while time.time()<deadline:
        _,t=call('GET',f'/trials/{tid}',token)
        if t.get('status') in want: return t
        time.sleep(0.1)
    return t

def db_check(tid):
    from src.config.database import Session
    from src.models.entities import Trial,TrialBalance,Receipt,Transfer,Sample,Audit
    from sqlalchemy import select,func
    with Session() as db:
        trial=db.get(Trial,tid);balance=db.get(TrialBalance,tid)
        receipts=list(db.scalars(select(Receipt).where(Receipt.trial_id==tid)))
        transfer=db.get(Transfer,trial.transfer_id);sample=db.get(Sample,transfer.sample_id)
        cancels=db.scalar(select(func.count()).select_from(Audit).where(Audit.action.like('%取消%演练%'+trial.name+'%')))
        return {'status':trial.status,'reason':trial.cancel_reason,'token':trial.run_token,'result':trial.result,
                'remaining':balance.remaining,'received':balance.received,'initial':sample.quantity,
                'receipts':[(r.actor,r.quantity) for r in receipts],'cancel_audits':cancels}

def stale_token_write(tid,token_value):
    """模拟过期执行者写终态：必须 0 行命中。"""
    from src.config.database import Session
    from src.models.entities import Trial
    from sqlalchemy import update
    with Session() as db:
        res=db.execute(update(Trial).where(Trial.id==tid,Trial.run_token==token_value,Trial.status.in_(['running','cancelling'])).values(status='completed'))
        db.commit();return res.rowcount

def worker_pids():
    pids=[]
    for pid in filter(str.isdigit,os.listdir('/proc')):
        try:
            if 'python' not in open(f'/proc/{pid}/comm').read(): continue
            cmd=open(f'/proc/{pid}/cmdline','rb').read().decode().replace('\x00',' ')
            if 'src.worker' in cmd and int(pid)!=os.getpid(): pids.append(int(pid))
        except Exception: pass
    return pids

def main():
    print(f'API={API}')
    _,login=call('POST','/auth/login',body={'email':'operator@qingyu.test','password':'Demo2026!'})
    token=login['token']
    _,vlogin=call('POST','/auth/login',body={'email':'viewer@qingyu.test','password':'Demo2026!'})
    vtoken=vlogin['token']
    _,transfers=call('GET','/transfers',token)
    transfer_id=transfers[0]['id']
    seq=int(time.time())%100000

    print('\n[1] 可确认流程与权限')
    _,busy=call('POST','/trials',token,{'name':f'vfy-blocker-{seq}','transfer_id':transfer_id,'timeout_ms':2000})
    wait_status(busy['id'],token,{'running'})  # 占用 worker，确保目标演练停留在排队状态
    _,t=call('POST','/trials',token,{'name':f'vfy-confirm-{seq}','transfer_id':transfer_id,'timeout_ms':50})
    code,_=call('POST',f"/trials/{t['id']}/cancel",token,{'confirm':False,'reason':'x'})
    check('未确认的取消被拒绝(400)',code==400,code)
    code,_=call('POST',f"/trials/{t['id']}/cancel",vtoken,{'confirm':True,'reason':'x'})
    check('观察员取消被拒绝(403)',code==403,code)
    code,r=call('POST',f"/trials/{t['id']}/cancel",token,{'confirm':True,'reason':'验证：排队取消'})
    check('排队演练取消返回已取消',code==200 and r['status']=='cancelled',r.get('status'))
    s=db_check(t['id'])
    check('排队取消后不再领取（无结果、无回执、账本未动）',s['result'] is None and s['receipts']==[] and s['remaining']==s['initial'] and s['received']==0,s)
    check('取消原因已记录',s['reason']=='验证：排队取消',s['reason'])
    wait_status(busy['id'],token,{'completed','cancelled','failed'},25)
    time.sleep(3)  # 留给 worker 一次领取机会，确认不会被误执行
    s=db_check(t['id'])
    check('worker 跳过已取消的排队任务',s['status']=='cancelled' and s['result'] is None,s['status'])

    print('\n[2] 持锁期间取消（A 持锁、B 等待中）')
    _,t=call('POST','/trials',token,{'name':f'vfy-hold-{seq}','transfer_id':transfer_id,'timeout_ms':2000})
    tid=t['id'];wait_status(tid,token,{'running'})
    time.sleep(0.4)  # A 已持锁并写入未提交回执，B 正在锁等待
    code,r=call('POST',f'/trials/{tid}/cancel',token,{'confirm':True,'reason':'验证：持锁期间取消'})
    check('运行中取消立即返回取消中（不提前报成功）',code==200 and r['status']=='cancelling',r.get('status'))
    final=wait_status(tid,token,{'cancelled','completed','failed'},20)
    check('回滚确认后进入已取消',final['status']=='cancelled',final['status'])
    s=db_check(tid)
    check('回执与演练账一起回滚（回执0、remaining=initial、received=0）',s['receipts']==[] and s['remaining']==s['initial'] and s['received']==0,f"receipts={s['receipts']} remaining={s['remaining']}/{s['initial']} received={s['received']}")
    check('取消证据含不变量与原因',s['result'] and s['result'].get('invariant_passed') and s['result'].get('cancelled') and s['result'].get('reason')=='验证：持锁期间取消',s['result'] and s['result'].get('invariant_passed'))
    check('取消审计恰好一条',s['cancel_audits']==1,s['cancel_audits'])
    code,r=call('POST',f'/trials/{tid}/cancel',token,{'confirm':True,'reason':'重复取消'})
    s2=db_check(tid)
    check('重试取消幂等且不新增审计',r['status']=='cancelled' and s2['cancel_audits']==1,(r['status'],s2['cancel_audits']))
    check('过期执行者不得改写终态（围栏0行）',stale_token_write(tid,s['token'])==0 and db_check(tid)['status']=='cancelled')

    print('\n[3] 取消与提交并发竞争')
    outcomes=set()
    for i in range(6):
        timeout=50 if i<3 else 800  # 前半提交通常获胜，后半取消通常获胜；两种终态都必须自洽
        _,t=call('POST','/trials',token,{'name':f'vfy-race-{seq}-{i}','transfer_id':transfer_id,'timeout_ms':timeout})
        tid=t['id'];wait_status(tid,token,{'running'},10)
        time.sleep(0.02*(i%3))
        call('POST',f'/trials/{tid}/cancel',token,{'confirm':True,'reason':'验证：提交竞争'})
        final=wait_status(tid,token,{'cancelled','completed','failed'},20)
        s=db_check(tid)
        outcomes.add(final['status'])
        if final['status']=='completed':
            ok=len(s['receipts'])==1 and s['receipts'][0][0]=='A' and s['remaining']+s['received']==s['initial']
        elif final['status']=='cancelled':
            ok=s['receipts']==[] and s['remaining']==s['initial'] and s['received']==0
        else: ok=False
        check(f'竞争#{i} 终态自洽（{final["status"]}）',ok,f"receipts={s['receipts']} remaining={s['remaining']} received={s['received']} initial={s['initial']}")
    print(f'  竞争终态分布: {sorted(outcomes)}')
    check('竞争同时出现已完成与已取消',outcomes=={'completed','cancelled'},sorted(outcomes))
    _,t=call('POST','/trials',token,{'name':f'vfy-done-{seq}','transfer_id':transfer_id,'timeout_ms':50})
    final=wait_status(t['id'],token,{'completed','failed'},20)
    check('演练可正常完成（原锁等待演练保留）',final['status']=='completed',final['status'])
    audits_before=db_check(t['id'])['cancel_audits']
    code,r=call('POST',f"/trials/{t['id']}/cancel",token,{'confirm':True,'reason':'迟到取消'})
    check('已提交的演练取消返回已完成且不新增审计',r['status']=='completed' and db_check(t['id'])['cancel_audits']==audits_before,(r['status'],))

    print('\n[4] 执行进程中断后恢复取消')
    _,t=call('POST','/trials',token,{'name':f'vfy-crash-{seq}','transfer_id':transfer_id,'timeout_ms':2000})
    tid=t['id'];wait_status(tid,token,{'running'})
    time.sleep(0.3)
    call('POST',f'/trials/{tid}/cancel',token,{'confirm':True,'reason':'验证：中断恢复'})
    mid=wait_status(tid,token,{'cancelling','cancelled'},10)
    pids=worker_pids()
    check('找到 worker 进程',len(pids)>=1,pids)
    for pid in pids: os.kill(pid,9)
    time.sleep(1.5)
    s=db_check(tid)
    check('worker 中断后无人提前终结（仍为取消中/已取消）',s['status'] in ('cancelling','cancelled'),s['status'])
    old_token=s['token']
    cmd=os.getenv('WORKER_RESTART_CMD','python3 -m src.worker')
    subprocess.Popen(shlex.split(cmd),cwd=BACKEND_DIR,stdout=open('/tmp/worker-restart.log','ab'),stderr=subprocess.STDOUT,start_new_session=True)
    final=wait_status(tid,token,{'cancelled','failed','completed'},30)
    check('重启后恢复取消为已取消',final['status']=='cancelled',final['status'])
    s=db_check(tid)
    check('恢复后回执0且账本守恒',s['receipts']==[] and s['remaining']==s['initial'] and s['received']==0,f"receipts={s['receipts']}")
    check('恢复证据标记 recovered',bool(s['result'] and s['result'].get('recovered')),s['result'] and s['result'].get('recovered'))
    check('旧执行者令牌已失效（围栏0行）',stale_token_write(tid,old_token)==0 and db_check(tid)['status']=='cancelled')

    print('\n[5] 排队演练取消后不再领取（worker 忙碌时）')
    _,t1=call('POST','/trials',token,{'name':f'vfy-busy-a-{seq}','transfer_id':transfer_id,'timeout_ms':2000})
    wait_status(t1['id'],token,{'running'})
    _,t2=call('POST','/trials',token,{'name':f'vfy-busy-b-{seq}','transfer_id':transfer_id,'timeout_ms':100})
    code,r=call('POST',f"/trials/{t2['id']}/cancel",token,{'confirm':True,'reason':'验证：排队不领取'})
    check('忙碌期间排队取消成功',code==200 and r['status']=='cancelled',r.get('status'))
    wait_status(t1['id'],token,{'completed','cancelled','failed'},25)
    time.sleep(3)
    s=db_check(t2['id'])
    check('排队任务未被 worker 领取执行',s['status']=='cancelled' and s['result'] is None and s['receipts']==[],(s['status'],s['receipts']))

    print(f'\n结果：{len(PASSED)} 通过，{len(FAILED)} 失败')
    if FAILED: print('失败项：'+'; '.join(FAILED));sys.exit(1)

if __name__=='__main__': main()
