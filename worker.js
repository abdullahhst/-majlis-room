/** مجلس - Room Server (Cloudflare Worker + Durable Objects)
 * Supports room creation, joining, live player list, and synchronized imported-question quiz rounds.
 */
const json = (data, status = 200) => new Response(JSON.stringify(data), {
  status, headers: { 'content-type': 'application/json; charset=utf-8', 'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET,POST,OPTIONS', 'access-control-allow-headers': 'Content-Type' }
});
const code4 = () => String(Math.floor(1000 + Math.random() * 9000));
const token = () => crypto.randomUUID().replaceAll('-', '') + crypto.randomUUID().replaceAll('-', '');

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (request.method === 'OPTIONS') return new Response(null, { headers: {
      'access-control-allow-origin': '*', 'access-control-allow-methods': 'GET,POST,OPTIONS',
      'access-control-allow-headers': 'Content-Type' } });

    if (url.pathname === '/api/create' && request.method === 'POST') {
      let body; try { body = await request.json(); } catch { return json({error:'بيانات غير صالحة'},400); }
      const name = String(body.name || '').trim().slice(0,18);
      if (!name) return json({error:'اكتب اسم المستضيف'},400);
      let code, id, stub;
      for (let i=0;i<8;i++) {
        code = code4(); id = env.ROOMS.idFromName(code); stub = env.ROOMS.get(id);
        const check = await stub.fetch('https://room.internal/exists');
        if (!check.ok || (await check.json()).exists === false) break;
        code = null;
      }
      if (!code) return json({error:'تعذر إنشاء رمز غرفة، حاول مرة أخرى'},503);
      const secret = token();
      const init = await stub.fetch('https://room.internal/init', {method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({code,secret,name,hostMode:body.hostMode==='display'?'display':'player'})});
      if (!init.ok) return json({error:'تعذر إنشاء الغرفة'},500);
      return json({code, token:secret, playerId:'host', wsUrl:`wss://${url.host}/ws`});
    }

    if (url.pathname === '/api/join' && request.method === 'POST') {
      let body; try { body = await request.json(); } catch { return json({error:'بيانات غير صالحة'},400); }
      const code = String(body.code || '').trim(); const name = String(body.name || '').trim().slice(0,18);
      if (!/^\d{4}$/.test(code) || !name) return json({error:'تحقق من رمز الغرفة واسمك'},400);
      const id = env.ROOMS.idFromName(code); const stub = env.ROOMS.get(id);
      const r = await stub.fetch('https://room.internal/join', {method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({code,name})});
      const data = await r.json(); if (!r.ok) return json(data,r.status);
      return json({...data,wsUrl:`wss://${url.host}/ws`});
    }

    if (url.pathname === '/ws') {
      const code = url.searchParams.get('code') || '';
      if (!/^\d{4}$/.test(code)) return json({error:'رمز الغرفة غير صالح'},400);
      const id = env.ROOMS.idFromName(code); const stub = env.ROOMS.get(id);
      return stub.fetch(new Request(`https://room.internal/ws${url.search}`, request));
    }
    return json({service:'Majlis Rooms',status:'ok',endpoints:['POST /api/create','POST /api/join','WebSocket /ws']});
  }
};

export class Room {
  constructor(state, env) { this.state = state; this.env = env; this.sockets = new Map(); }
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/exists') { const room=await this.state.storage.get('room'); return json({exists:!!room}); }
    if (url.pathname === '/init' && request.method === 'POST') {
      if (await this.state.storage.get('room')) return json({error:'الغرفة موجودة'},409);
      const b=await request.json();
      const hostRole=b.hostMode==='display'?'display':'host';
      await this.state.storage.put('room',{code:b.code,secret:b.secret,hostMode:b.hostMode,status:'بانتظار انضمام اللاعبين',players:[{id:'host',name:b.name,role:hostRole==='display'?'display':'player'}],quiz:null,answers:{},createdAt:Date.now()});
      return json({ok:true});
    }
    if (url.pathname === '/join' && request.method === 'POST') {
      const room=await this.state.storage.get('room'); if(!room) return json({error:'الغرفة غير موجودة أو انتهت'},404);
      if(room.code!==String((await request.clone().json()).code)) return json({error:'رمز الغرفة غير صحيح'},404);
      const b=await request.json(); if(room.players.length>=20) return json({error:'الغرفة ممتلئة (20 لاعبًا كحد أقصى)'},409);
      const playerId=crypto.randomUUID(); room.players.push({id:playerId,name:String(b.name||'لاعب').slice(0,18),role:'player'});
      await this.state.storage.put('room',room); this.broadcast({type:'state',players:room.players,status:room.status});
      return json({playerId});
    }
    if (url.pathname === '/ws') {
      if (request.headers.get('Upgrade') !== 'websocket') return new Response('WebSocket required',{status:426});
      const room=await this.state.storage.get('room'); if(!room) return json({error:'الغرفة غير موجودة'},404);
      const code=url.searchParams.get('code'), playerId=url.searchParams.get('playerId'), name=url.searchParams.get('name')||'لاعب', role=url.searchParams.get('role')||'player', secret=url.searchParams.get('token')||'';
      if(code!==room.code) return json({error:'رمز الغرفة غير صحيح'},403);
      if(role==='host' && secret!==room.secret) return json({error:'صلاحية المستضيف غير صحيحة'},403);
      if(role!=='host' && !room.players.some(p=>p.id===playerId)) return json({error:'اللاعب غير مسجل في الغرفة'},403);
      const pair=new WebSocketPair(); const client=pair[0], server=pair[1]; server.accept();
      this.sockets.set(server,{playerId,role});
      server.send(JSON.stringify({type:'state',players:room.players,status:room.status}));
      server.addEventListener('message',async event=>{
        let msg;try{msg=JSON.parse(event.data)}catch{return;}
        const info=this.sockets.get(server); if(!info) return;
        const current=await this.state.storage.get('room'); if(!current)return;
        const isHost=info.role==='host';
        if(msg.type==='quiz_start' && isHost) {
          const questions=Array.isArray(msg.questions)?msg.questions.filter(q=>q&&typeof q.question==='string'&&typeof q.answer==='string').slice(0,500):[];
          if(!questions.length){server.send(JSON.stringify({type:'notice',message:'لا يوجد بنك أسئلة صالح.'}));return;}
          current.quiz={questions,index:0,revealed:false,answered:[]};current.answers={};current.status='بدأ تحدي الأسئلة';await this.state.storage.put('room',current);
          this.broadcast({type:'notice',event:'quiz',data:current.quiz,message:current.status});
        }
        if(msg.type==='quiz_answer' && !isHost && current.quiz && !current.quiz.revealed) {
          const pid=info.playerId;const ans=String(msg.answer??'').slice(0,300);current.answers=current.answers||{};current.answers[pid]={answer:ans,answerIndex:Number.isInteger(msg.answerIndex)?msg.answerIndex:null};current.quiz.answered=[...new Set([...(current.quiz.answered||[]),pid])];await this.state.storage.put('room',current);
          this.broadcast({type:'notice',event:'answer_update',count:current.quiz.answered.length,message:'تم تسجيل إجابة'});
        }
        if(msg.type==='quiz_reveal' && isHost && current.quiz) {current.quiz.revealed=true;await this.state.storage.put('room',current);this.broadcast({type:'notice',event:'quiz_result',data:current.quiz,message:'تم كشف الإجابة'});}
        if(msg.type==='quiz_next' && isHost && current.quiz) {current.quiz.index++;current.quiz.revealed=false;current.quiz.answered=[];current.answers={};if(current.quiz.index>=current.quiz.questions.length){current.quiz=null;current.status='انتهى التحدي';await this.state.storage.put('room',current);this.broadcast({type:'notice',event:'quiz',data:{questions:[],index:0},message:current.status});}else{await this.state.storage.put('room',current);this.broadcast({type:'notice',event:'quiz_next',data:current.quiz,message:'السؤال التالي'});}}
        if(msg.type==='quiz_end' && isHost) {current.quiz=null;current.status='عاد الجميع إلى الغرفة';await this.state.storage.put('room',current);this.broadcast({type:'notice',event:'quiz_end',message:current.status});}
        if(msg.type==='ping') server.send(JSON.stringify({type:'pong'}));
      });
      const cleanup=()=>{const info=this.sockets.get(server);this.sockets.delete(server);if(info&&info.playerId!=='host')this.removePlayer(info.playerId);};
      server.addEventListener('close',cleanup); server.addEventListener('error',cleanup);
      return new Response(null,{status:101,webSocket:client});
    }
    return json({error:'غير موجود'},404);
  }
  async removePlayer(id){const room=await this.state.storage.get('room');if(!room)return;room.players=room.players.filter(p=>p.id!==id);await this.state.storage.put('room',room);this.broadcast({type:'state',players:room.players,status:room.status});}
  broadcast(data){const raw=JSON.stringify(data);for(const ws of this.sockets.keys()){try{ws.send(raw)}catch{this.sockets.delete(ws)}}}
}
