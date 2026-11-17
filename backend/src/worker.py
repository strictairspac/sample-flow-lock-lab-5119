import os,time,traceback,json
from pathlib import Path
from src.services.trials import queue
from src.services.concurrency import execute,recover_interrupted
ART=Path(os.getenv('ARTIFACTS_DIR','/artifacts'))
recovered=recover_interrupted()
if recovered: print('启动恢复完成:',recovered,flush=True)
while True:
    Path('/tmp/worker-heartbeat').touch()
    item=queue.blpop('locklab:queue',timeout=2)
    if not item: continue
    ident=int(item[1])
    try:
        result=execute(ident)
        if result is not None:
            ART.mkdir(parents=True,exist_ok=True)
            (ART/f'trial-{ident}.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
    except Exception:
        traceback.print_exc()
