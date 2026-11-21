import {field} from './fields.js';
import {pill} from './panels.js';
import {STATUS} from '../../constants/enums.js';
import {handler, poll} from '../../hooks/action.js';
import {session} from '../../stores/session.js';
import {workspace} from '../../stores/workspace.js';
import {button, el, p} from '../../utils/dom.js';

const finished = item => item.status === 'complete' || item.status === 'failed';

export function simulationProgress(run) {
  const simulation = run.simulation;
  if (!simulation) return null;
  const detail = simulation.batch_count
    ? `已算批次 ${simulation.completed_batches} / ${simulation.batch_count} · 每批 ${simulation.batch_size} 次`
    : `每批 ${simulation.batch_size} 次`;
  return el('div', {class:'simulation-progress', 'data-testid':'simulation-progress'},
    el('p', {}, '超时概率模拟：', pill(STATUS[simulation.status] || simulation.status, simulation.status)),
    p(detail, 'subtle'),
    simulation.failed_batches ? p(`失败批次 ${simulation.failed_batches} 个，正在补算。`, 'feedback') : null);
}

export function reservedForm(job, message, onPublished) {
  const simulation = job.simulation;
  const revision = simulation?.revision;
  const published = simulation?.published_revision;
  const current = simulation?.reserved_minutes ?? job.params?.reserved_minutes;
  const pending = revision !== undefined && published !== revision;
  const submit = button('按同一批样本重算', {type:'submit', class:'primary', 'data-testid':'revise-reserved'});
  const form = el('form', {class:'fields', 'data-testid':'reserved-form',
    onSubmit:handler(submit, message, async event => {
      const minutes = Number(new FormData(event.target).get('reserved_minutes'));
      if (!Number.isFinite(minutes) || minutes < 1) throw new Error('请填写不小于 1 的每日预留分钟');
      const updated = await workspace.reviseReserved(job.id, minutes);
      if (updated.published) {
        message.textContent = '';
        onPublished();
        return;
      }
      message.textContent = `新预留分钟已登记（第 ${updated.revision} 版），样本批次算齐后再切换页面与下载。`;
      poll(() => workspace.getRun(job.id),
        item => item.simulation?.published_revision === item.simulation?.revision,
        () => true).then(item => { if (item) onPublished(); });
    })},
    field({key:'reserved_minutes', label:'每日预留（分钟）', type:'number', min:1, max:10080,
      required:true, default:current, help:'沿用已算样本只换阈值，不重新抽样。'}),
    el('div', {class:'actions'}, submit));
  return el('div', {class:'reserved-panel', 'data-testid':'reserved-panel'},
    form,
    pending ? p(published
      ? `第 ${revision} 版正在等全部批次算齐，页面与下载仍显示第 ${published} 版。`
      : `第 ${revision} 版正在等全部批次算齐，算齐前不生成页面与下载。`, 'subtle') : null);
}

export const canRevise = () => session.canWrite || session.canReview;
