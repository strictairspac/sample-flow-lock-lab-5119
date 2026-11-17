import json
from sqlalchemy import select,update
from redis import Redis
from fastapi import HTTPException
from src.config.settings import REDIS_URL
from src.models.entities import Trial,Transfer,Sample,TrialBalance,Audit
from src.utils.serialization import record
from .common import save
from src.constants.enums import RUN_STATES
queue=Redis.from_url(REDIS_URL,decode_responses=True)
def listing(db): return [record(x) for x in db.scalars(select(Trial).order_by(Trial.id.desc()))]
def detail(db,ident):
    obj=db.get(Trial,ident)
    if not obj: raise HTTPException(404,'演练不存在')
    return record(obj)
def create(db,data,user):
    transfer=db.get(Transfer,data.transfer_id)
    if not transfer: raise HTTPException(404,'流转申请不存在')
    sample=db.get(Sample,transfer.sample_id)
    if transfer.quantity>sample.quantity: raise HTTPException(422,'当前样本量不足')
    obj=Trial(**data.model_dump(),status=RUN_STATES[0])
    result=save(db,obj,user,'并发演练入队 '+data.name)
    db.add(TrialBalance(trial_id=obj.id,remaining=sample.quantity,received=0));db.commit()
    try: queue.rpush('locklab:queue',str(obj.id))
    except Exception:
        obj.status='failed';obj.error='队列不可用，请稍后重新创建';db.commit();raise HTTPException(503,obj.error)
    return result
def cancel(db,ident,data,user):
    if not data.confirm: raise HTTPException(400,'取消需要确认：请确认后重新提交')
    trial=db.get(Trial,ident)
    if not trial: raise HTTPException(404,'演练不存在')
    reason=(data.reason or '').strip() or '误发演练，人工取消'
    if trial.status=='queued':
        # 原子跃迁：worker 领取与取消只会有一个成功；取消后不再被领取
        res=db.execute(update(Trial).where(Trial.id==ident,Trial.status=='queued').values(status='cancelled',cancel_reason=reason))
        if res.rowcount==1:
            db.add(Audit(actor=user.email,action=f'取消排队演练 {trial.name}：{reason}'));db.commit()
            try: queue.lrem('locklab:queue',0,str(ident))
            except Exception: pass
            return record(db.get(Trial,ident))
        db.rollback()
    if trial.status=='running':
        # 仅标记取消中；worker 确认回滚后才进入已取消，不提前报成功
        res=db.execute(update(Trial).where(Trial.id==ident,Trial.status=='running').values(status='cancelling',cancel_reason=reason))
        if res.rowcount==1:
            db.add(Audit(actor=user.email,action=f'取消运行演练 {trial.name}：{reason}'));db.commit()
            return record(db.get(Trial,ident))
        db.rollback()
    db.refresh(trial)
    return record(trial)
