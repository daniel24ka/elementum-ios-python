'use strict';
const byId = id => document.getElementById(id);
let token = sessionStorage.getItem('elementumToken') || '', pending = false;
function message(text) { byId('message').textContent = text || ''; }
async function api(path, body) {
  const response = await fetch('/api/' + path, {method: body === undefined ? 'GET' : 'POST',
    headers: {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'},
    body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(12000)});
  const result = await response.json();
  if (!response.ok) { if (response.status === 401) { byId('auth').hidden = false; byId('dashboard').hidden = true; }
    throw new Error(result.error || 'Request failed'); }
  return result;
}
const size = n => (Number(n || 0) / 1073741824).toFixed(2) + ' GiB';
function element(tag, text, cls) { const node = document.createElement(tag); node.textContent = text || ''; if (cls) node.className = cls; return node; }
async function action(id, name) {
  if (name === 'remove' && !confirm('Delete this download and all its saved data?')) return;
  try { await api('downloads/' + id + '/' + name, {confirm_delete: name === 'remove'}); await refresh(); }
  catch (error) { message(error.message); }
}
function render(job) {
  const card = element('article', '', 'job'); card.append(element('h3', job.title, 'job-title'));
  card.append(element('p', job.status + ' · ' + size(job.completed_bytes) + ' / ' + size(job.total_bytes) +
    ' · ' + (job.rate / 1048576).toFixed(1) + ' MiB/s', 'details'));
  const progress = document.createElement('progress'); progress.max = 100; progress.value = job.progress; progress.setAttribute('aria-label', 'Download progress'); card.append(progress);
  if (job.error) card.append(element('p', job.error, 'details'));
  const actions = element('div', '', 'actions');
  const busy = ['preparing', 'downloading', 'verifying', 'queued'].includes(job.status);
  const choices = job.status === 'completed' ? [['play', 'Play'], ['recheck','Verify'], ['remove', 'Delete']] : busy ?
    [['pause', 'Pause'], ['cancel', 'Cancel']] : ['pausing','cancelling'].includes(job.status) ? [] : [['resume','Resume / retry'],['remove','Delete']];
  for (const [name, label] of choices) { const button = element('button', label, name === 'remove' ? 'danger' : 'secondary');
    button.addEventListener('click', async () => { button.disabled = true; await action(job.id, name); button.disabled = false; }); actions.append(button); }
  card.append(actions); return card;
}
async function refresh() {
  if (!token || pending) return;
  pending = true;
  try { const data = await api('status'); byId('auth').hidden = true; byId('dashboard').hidden = false;
    byId('connection').textContent = 'Connected'; byId('player').textContent = data.player.active ? data.player.title : 'Idle';
    byId('count').textContent = data.downloads.length; byId('empty').hidden = data.downloads.length > 0;
    byId('jobs').replaceChildren(...data.downloads.slice().reverse().map(render));
  } catch (error) { byId('connection').textContent = 'Disconnected'; message(error.message + ' — check that Kodi is open.'); }
  finally { pending = false; }
}
byId('auth').addEventListener('submit', event => { event.preventDefault(); token = byId('token').value.trim(); sessionStorage.setItem('elementumToken', token); message(''); refresh(); });
byId('add').addEventListener('submit', async event => { event.preventDefault(); const button = event.target.querySelector('button'); button.disabled = true;
  try { await api('downloads', {uri: byId('uri').value.trim(), title: byId('title').value.trim(), all_files: byId('all').checked}); event.target.reset(); message(''); await refresh(); }
  catch (error) { message(error.message); } finally { button.disabled = false; } });
byId('refresh').addEventListener('click', refresh); setInterval(refresh, 3000); refresh();
