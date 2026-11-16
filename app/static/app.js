const state={busy:false};
document.querySelectorAll("[data-endpoint]").forEach(btn=>btn.addEventListener("click",async()=>{
  if(state.busy)return; state.busy=true; const out=btn.parentElement.querySelector(".result"); out.textContent="运行中";
  try{const res=await fetch(btn.dataset.endpoint,{method:btn.dataset.endpoint.includes("export")?"GET":"POST",headers:{"content-type":"application/json"},body:btn.dataset.endpoint.includes("export")?null:"{}"});out.textContent=await res.text()}catch(e){out.textContent="请求失败，请重试"}finally{state.busy=false}
}));
document.querySelector("#refresh")?.addEventListener("click",()=>location.reload());

/* M1 分段采集控制台：仅在日志采信页生效，不改变其他模块行为 */
(function(){
  const root=document.querySelector("[data-intake]");
  if(!root)return;
  const panel=root.closest(".intake");
  const out=document.querySelector("#intake-result");
  const field=name=>panel.querySelector(`[name="${name}"]`);
  const num=name=>{const v=field(name).value.trim();return v===""?null:Number(v)};
  const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  const LABELS={RECEIVING:"接收中",PENDING_TAIL:"待补齐",COMPLETE:"完成"};
  let lastSnapshot=null;

  function suggestOffset(){
    const src=field("source").value.trim(), gen=num("generation");
    const found=lastSnapshot?.sources?.find(s=>s.source===src);
    const g=found?.generations?.find(x=>x.generation===gen);
    if(g&&field("expected_offset").value.trim()==="")field("expected_offset").value=g.received_offset;
  }
  field("generation").addEventListener("change",suggestOffset);

  function render(snapshot){
    lastSnapshot=snapshot;
    const bodies=document.querySelectorAll(".ledger tbody");
    const ledger=bodies[0], evidence=bodies[1];
    const t=snapshot.totals;
    document.querySelector(".panel--primary .panel__meta").textContent=
      `M1 · 分段采信 · 来源 ${t.source_count} / 代次 ${t.generation_count}`;
    const stateMeta=panel.closest(".stack").querySelector(".panel .panel__meta");
    if(stateMeta)stateMeta.textContent=`接收中 ${t.receiving} · 待补齐 ${t.pending_tail} · 完成 ${t.complete}`;
    let rows="";
    snapshot.sources.forEach(src=>src.generations.forEach(g=>{
      rows+=`<tr><td>${esc(g.source)}</td><td class="num">g${g.generation}</td><td>${esc(g.batch_id)}</td>`+
        `<td><span class="sev state--${g.state}">${LABELS[g.state]}</span></td>`+
        `<td class="num">${g.received_offset}</td>`+
        `<td class="tail">${g.pending_tail?`<code>${esc(g.pending_tail)}</code>`:"—"}</td>`+
        `<td class="num">${g.line_count}</td><td class="num">${g.valid_count}</td>`+
        `<td class="num">${g.quarantine_count}</td>`+
        `<td class="buckets">${g.hour_buckets.length?g.hour_buckets.map(b=>`<i>${esc(b)}</i>`).join(" "):"—"}</td>`+
        `<td class="num">${esc(g.ended_at||"—")}</td></tr>`;
    }));
    ledger.innerHTML=rows||`<tr><td colspan="11" class="empty">尚未登记来源，先在右侧「采集操作」开立来源</td></tr>`;
    let ev="";
    snapshot.sources.forEach(src=>src.evidence.forEach(e=>{
      ev+=`<tr class="evidence--${e.kind.toLowerCase()}"><td>${esc(e.source)}</td><td class="num">g${e.generation}</td>`+
        `<td>${esc(e.batch_id)}</td><td class="num">${e.source_line}</td>`+
        `<td>${esc(e.request_id||"—")}</td><td><i>${esc(e.hour_bucket)}</i></td>`+
        `<td>${e.reason?`<span class="sev">${esc(e.reason)}</span>`:"—"}</td>`+
        `<td class="rawcell" title="${esc(e.raw)}">${esc(e.raw)}</td></tr>`;
    }));
    evidence.innerHTML=ev||`<tr><td colspan="8" class="empty">暂无证据：开立来源并上传分片后展示</td></tr>`;
  }

  async function call(payload){
    const res=await fetch("/api/logs/intake",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(payload)});
    const data=await res.json();
    if(!res.ok)throw Object.assign(new Error(data.message||"请求失败"),{data});
    return data;
  }

  panel.querySelectorAll("[data-intake]").forEach(btn=>btn.addEventListener("click",async()=>{
    const kind=btn.dataset.intake;
    if(kind==="reload"){location.reload();return}
    if(state.busy)return; state.busy=true; out.textContent="运行中";
    const source=field("source").value.trim(), generation=num("generation"), seq=num("seq");
    const expected_offset=num("expected_offset"), batch_id=field("batch_id").value.trim()||null;
    const payload={action:kind,source};
    try{
      let data;
      if(kind==="open"){if(batch_id)payload.batch_id=batch_id;data=await call(payload)}
      else if(kind==="upload"){Object.assign(payload,{generation,seq:seq??0,text:field("text").value,expected_offset});data=await call(payload)}
      else if(kind==="rotate"){Object.assign(payload,{generation,batch_id});data=await call(payload)}
      else{Object.assign(payload,{generation});data=await call(payload)}
      out.textContent=JSON.stringify(data,null,2);
      render(data.snapshot);
      if(kind==="rotate")field("generation").value=generation+1;
      suggestOffset();
    }catch(e){out.textContent=e.data?JSON.stringify(e.data,null,2):("请求失败，请重试："+e.message)}
    finally{state.busy=false}
  }));

  fetch("/api/logs/intake").then(r=>r.json()).then(render).catch(()=>{});
})();
