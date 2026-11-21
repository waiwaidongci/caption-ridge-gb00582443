import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from sqlalchemy import and_, func, or_, select
from sqlalchemy.exc import IntegrityError
from src.config.database import Session
from src.constants.enums import Status
from src.domain.analysis import analyze
from src.domain.simulation import batch_plan, compose, probabilities, sample_batch
from src.domain.spec import SPEC
from src.models.jobs import Delivery, Run, Schedule, Simulation, SimulationBatch
from src.models.source import Dataset
from src.services.audit import record
from src.services.validation import normalize_params
from src.utils.exports import write_report
from src.utils.time import now

log = logging.getLogger(__name__)

_BATCH_WORKERS = int(SPEC.get('simulation_workers', 4))
_RUN_ATTEMPTS = 3
_SIMULATION_RUN_ATTEMPTS = 50


class Incomplete(Exception):
    """The requested probability revision still lacks completed sample batches."""


def materialize_due():
    if not SPEC.get('scheduled'):
        return
    stamp = now()
    with Session.begin() as db:
        schedules = db.scalars(select(Schedule).where(Schedule.enabled.is_(True), Schedule.next_at <= stamp)
                                .order_by(Schedule.next_at).limit(20).with_for_update(skip_locked=True))
        for item in schedules:
            due = item.next_at
            dataset_id = item.dataset_id
            params = dict(item.params)
            if item.repeat_daily:
                elapsed_days = (stamp - due).days
                due += timedelta(days=elapsed_days)
                period_key = SPEC['schedule_date_param']
                report_day = date.fromisoformat(params[period_key]) + timedelta(days=elapsed_days)
                params[period_key] = report_day.isoformat()
                item.params = {**params, period_key: (report_day + timedelta(days=1)).isoformat()}
                dataset_id = db.scalar(select(Dataset.id).order_by(Dataset.id.desc()).limit(1)) or dataset_id
                item.next_at = due + timedelta(days=1)
            else:
                item.enabled = False
            db.add(Run(dataset_id=dataset_id, schedule_id=item.id, scheduled_at=due,
                       name=item.name, params=params, author=item.author))


def claim():
    stamp = now()
    with Session.begin() as db:
        query = select(Run).where(or_(Run.status == Status.QUEUED,
                                      and_(Run.status == Status.RUNNING, Run.lease_until < stamp)))
        job = db.scalar(query.order_by(Run.id).limit(1).with_for_update(skip_locked=True))
        if job is None:
            return None
        # A simulation can be interrupted while its batches are leased. Keep
        # resuming from the stored batches instead of abandoning the run.
        resumable = db.scalar(select(Simulation.id).where(Simulation.run_id == job.id).limit(1))
        limit = _SIMULATION_RUN_ATTEMPTS if resumable else _RUN_ATTEMPTS
        if job.attempt >= limit:
            job.status = Status.FAILED
            job.error = '后台进程多次中断，请发起新的运行'
            job.finished_at = stamp
            record(db, 'worker', 'analysis.failed', job.id, reason=job.error)
            return None
        job.status = Status.RUNNING
        job.attempt += 1
        job.lease_until = stamp + timedelta(minutes=3)
        records = db.get(Dataset, job.dataset_id).rows
        return job.id, job.attempt, records, job.params


def _latest_simulation(db, run_id):
    return db.scalar(select(Simulation).where(Simulation.run_id == run_id)
                     .order_by(Simulation.version.desc()).limit(1))


def prepare_simulation(run_id, estimation, params):
    """Persist the sample plan once, so a resumed run reuses drawn batches."""
    inputs = estimation['simulation_inputs']
    iterations = int(params['simulation_iterations'])
    days = len(inputs['pending_days'])
    if days * iterations > 2_000_000:
        raise ValueError('抽样总量过大，请减少模拟次数或拆分待审日')
    with Session() as db:
        existing = _latest_simulation(db, run_id)
        if existing is not None:
            return existing.id, False
        run = db.get(Run, run_id)
        batch_size = int(params['simulation_batch_size'])
        sizes = batch_plan(iterations, batch_size)
        simulation = Simulation(
            run_id=run_id, version=1, status=Status.QUEUED, iterations=iterations,
            batch_size=batch_size, seed=run_id, reserved_minutes=float(params['reserved_minutes']),
            revision=1, inputs=inputs, estimation=estimation,
            author=run.author)
        db.add(simulation)
        try:
            db.flush()
            for index, size in enumerate(sizes):
                db.add(SimulationBatch(simulation_id=simulation.id, batch_index=index, size=size,
                                       seed=simulation.seed * 100_003 + index + 1))
            db.commit()
            return simulation.id, True
        except IntegrityError:
            # Another worker created the plan first; reuse its batches.
            db.rollback()
    with Session() as db:
        existing = _latest_simulation(db, run_id)
        if existing is None:
            raise
        return existing.id, False


def _expire_batches():
    stamp = now()
    with Session.begin() as db:
        stalled = db.scalars(select(SimulationBatch).where(
            or_(and_(SimulationBatch.status == Status.RUNNING, SimulationBatch.lease_until < stamp),
                SimulationBatch.status == Status.QUEUED),
            SimulationBatch.attempt >= 3).with_for_update(skip_locked=True))
        for batch in stalled:
            batch.status = Status.FAILED
            batch.lease_until = None
            batch.error = '批次多次中断，已停止重试'
            batch.finished_at = stamp
            record(db, 'worker', 'simulation.batch.failed', batch.simulation_id, batch=batch.batch_index)


def _claim_batches(limit, simulation_id=None):
    stamp = now()
    with Session.begin() as db:
        query = select(SimulationBatch).where(
            or_(SimulationBatch.status == Status.QUEUED,
                and_(SimulationBatch.status == Status.RUNNING, SimulationBatch.lease_until < stamp)),
            SimulationBatch.attempt < 3)
        if simulation_id is not None:
            query = query.where(SimulationBatch.simulation_id == simulation_id)
        leases = []
        for batch in db.scalars(query.order_by(SimulationBatch.simulation_id, SimulationBatch.batch_index)
                                .limit(limit).with_for_update(skip_locked=True)):
            batch.status = Status.RUNNING
            batch.attempt += 1
            batch.lease_until = stamp + timedelta(minutes=3)
            leases.append((batch.id, batch.simulation_id, batch.batch_index, batch.size, batch.seed, batch.attempt))
        return leases


def _run_batch(lease):
    batch_id, simulation_id, index, size, seed, attempt = lease
    try:
        with Session() as db:
            simulation = db.get(Simulation, simulation_id)
            inputs = simulation.inputs
        samples = sample_batch(inputs, size, seed)
        with Session.begin() as db:
            batch = db.get(SimulationBatch, batch_id, with_for_update=True)
            if batch is None or batch.attempt != attempt or batch.status != Status.RUNNING:
                return
            # One row per batch index: a retried batch overwrites the same
            # samples, so retries never count twice toward the probability.
            batch.samples = samples
            batch.status = Status.COMPLETE
            batch.lease_until = None
            batch.error = None
            batch.finished_at = now()
            record(db, 'worker', 'simulation.batch.complete', simulation_id, batch=index, size=size)
    except Exception as error:
        log.exception('Simulation batch %s failed', batch_id)
        with Session.begin() as db:
            batch = db.get(SimulationBatch, batch_id, with_for_update=True)
            if batch is None or batch.attempt != attempt:
                return
            batch.status = Status.FAILED if attempt >= 3 else Status.QUEUED
            batch.lease_until = None
            batch.error = str(error)[:500]


def _renew_run_lease(run_id, attempt, seconds=180):
    """Keep the run leased while its batches run; False if another attempt owns it."""
    with Session.begin() as db:
        job = db.get(Run, run_id)
        if job is None or job.attempt != attempt or job.status != Status.RUNNING:
            return False
        job.lease_until = now() + timedelta(seconds=seconds)
        return True


def _defer_run(run_id, attempt, seconds=2):
    """Release a run whose batches another worker holds, without using up an attempt."""
    with Session.begin() as db:
        job = db.get(Run, run_id)
        if job is None or job.attempt != attempt or job.status != Status.RUNNING:
            return
        job.attempt = max(0, job.attempt - 1)
        job.lease_until = now() + timedelta(seconds=seconds)


def _drain(simulation_id, run_id, attempt, workers=_BATCH_WORKERS):
    for _ in range(2000):
        _expire_batches()
        if not _renew_run_lease(run_id, attempt):
            return 'waiting'
        with Session() as db:
            if db.get(Simulation, simulation_id) is None:
                return '计算记录不存在'
            states = dict(db.execute(select(SimulationBatch.status, func.count())
                                     .where(SimulationBatch.simulation_id == simulation_id)
                                     .group_by(SimulationBatch.status)).all())
        if states.get(Status.FAILED):
            return '部分模拟批次失败，请重新发起运行'
        if states and states.get(Status.COMPLETE, 0) == sum(states.values()):
            return None
        leases = _claim_batches(workers, simulation_id)
        if not leases:
            # Remaining batches are leased by another worker. Hand the run back
            # without spending an attempt so it resumes once they land.
            _defer_run(run_id, attempt)
            return 'waiting'
        with ThreadPoolExecutor(max_workers=len(leases)) as pool:
            list(pool.map(_run_batch, leases))
    return 'waiting'


def _complete_batches(db, simulation_id):
    batches = list(db.scalars(select(SimulationBatch).where(SimulationBatch.simulation_id == simulation_id)
                              .order_by(SimulationBatch.batch_index)))
    if not batches or any(batch.status != Status.COMPLETE for batch in batches):
        raise Incomplete
    return [{'batch_index': batch.batch_index, 'size': batch.size, 'samples': batch.samples}
            for batch in batches]


def _render(simulation, batches):
    """Compose the page result for the simulation's current revision."""
    probabilities_result = probabilities(simulation.inputs, batches, simulation.reserved_minutes,
                                         simulation.iterations)
    meta = {'revision': simulation.revision, 'reserved_minutes': simulation.reserved_minutes,
            'iterations': simulation.iterations, 'batch_size': simulation.batch_size}
    return compose(simulation.estimation, probabilities_result, meta)


def _publish(simulation_id):
    """Atomically switch the page result and the export to a fully computed revision."""
    # Lock Simulation first, then Run, so concurrent publish/fail paths agree.
    with Session.begin() as db:
        simulation = db.get(Simulation, simulation_id, with_for_update=True)
        if simulation is None:
            return None
        if simulation.published_revision == simulation.revision and simulation.published_result is not None:
            return simulation.published_result
        run = db.get(Run, simulation.run_id, with_for_update=True)
        batches = _complete_batches(db, simulation_id)
        result = _render(simulation, batches)
        name = write_report(run.id, run.attempt, result, simulation.revision)
        run.result = result
        run.export_file = name
        run.status = Status.COMPLETE
        run.error = None
        run.lease_until = None
        run.finished_at = now()
        simulation.status = Status.COMPLETE
        simulation.error = None
        simulation.published_revision = simulation.revision
        simulation.published_result = result
        simulation.published_at = now()
        simulation.finished_at = now()
        record(db, 'worker', 'analysis.complete', run.id, attempt=run.attempt,
               revision=simulation.revision, iterations=simulation.iterations)
        _deliver(db, run)
        return result


def _deliver(db, run):
    if not run.schedule_id or db.scalar(select(Delivery.id).where(Delivery.run_id == run.id)):
        return
    plan = db.get(Schedule, run.schedule_id)
    receipt = Delivery(run_id=run.id, recipient=plan.recipient, state='delivered',
                       detail='已保存 CSV 到站内收件箱')
    db.add(receipt)
    record(db, 'worker', 'delivery.delivered', run.id, recipient=plan.recipient)


def _fail(run_id, attempt, failure):
    # Lock order is Simulation -> Run, matching _publish().
    with Session.begin() as db:
        simulation = _latest_simulation(db, run_id)
        if simulation is not None:
            simulation = db.get(Simulation, simulation.id, with_for_update=True)
        job = db.get(Run, run_id, with_for_update=True)
        if job is None or job.attempt != attempt or job.status != Status.RUNNING:
            return
        if simulation is not None and simulation.status != Status.FAILED:
            simulation.status = Status.FAILED
            simulation.error = failure
            simulation.finished_at = now()
            record(db, 'worker', 'simulation.failed', simulation.id, reason=failure)
        job.lease_until = None
        if job.result is not None:
            # A published revision stays available; only the new revision failed.
            job.status = Status.COMPLETE
            record(db, 'worker', 'analysis.revision.failed', job.id, attempt=attempt, reason=failure)
            return
        job.finished_at = now()
        job.status = Status.FAILED
        job.error = failure
        record(db, 'worker', 'analysis.failed', job.id, attempt=attempt, reason=failure)


def _publish_estimation(run_id, attempt, result):
    """Publish a result that needs no sampling, e.g. when nothing is pending."""
    with Session.begin() as db:
        run = db.get(Run, run_id, with_for_update=True)
        if run is None or run.attempt != attempt or run.status != Status.RUNNING:
            return
        run.result = result
        run.export_file = write_report(run.id, run.attempt, result)
        run.status = Status.COMPLETE
        run.error = None
        run.lease_until = None
        run.finished_at = now()
        record(db, 'worker', 'analysis.complete', run.id, attempt=attempt, iterations=0)
        _deliver(db, run)


def execute(lease):
    job_id, attempt, rows, params = lease
    simulation_id = None
    try:
        normalized = normalize_params(params)
        estimation = analyze(rows, normalized)
        if not estimation['simulation_inputs']['pending_days']:
            _publish_estimation(job_id, attempt, estimation)
            return
        simulation_id, created = prepare_simulation(job_id, estimation, normalized)
        if created:
            with Session.begin() as db:
                simulation = db.get(Simulation, simulation_id, with_for_update=True)
                simulation.status = Status.RUNNING
        drain_error = _drain(simulation_id, job_id, attempt)
        if drain_error == 'waiting':
            # Ran out of run lease or lost it to a retry; stored batches are
            # reused when claim() hands this run back.
            return
        if drain_error:
            _fail(job_id, attempt, drain_error)
            return
        try:
            _publish(simulation_id)
        except Incomplete:
            # Keep the previous published revision and retry once batches land.
            return
    except ValueError as error:
        _fail(job_id, attempt, str(error)[:500])
    except Exception:
        log.exception('Analysis %s failed', job_id)
        _fail(job_id, attempt, '计算失败，请检查字段口径后重试')


def publish_pending():
    """Publish a revised probability once every one of its sample batches is in."""
    _expire_batches()
    with Session() as db:
        candidates = db.execute(select(Simulation.id)
                                .where(Simulation.status != Status.FAILED,
                                       or_(Simulation.published_revision.is_(None),
                                           Simulation.published_revision < Simulation.revision))).all()
    for (simulation_id,) in candidates:
        with Session() as db:
            states = dict(db.execute(select(SimulationBatch.status, func.count())
                                     .where(SimulationBatch.simulation_id == simulation_id)
                                     .group_by(SimulationBatch.status)).all())
        if states.get(Status.FAILED):
            _fail_simulation(simulation_id, '部分模拟批次失败，请重新发起运行')
            continue
        if not states or states.get(Status.COMPLETE, 0) != sum(states.values()):
            # Still sampling: the page and export stay on the published revision.
            continue
        try:
            _publish(simulation_id)
        except Incomplete:
            continue
        except Exception:
            log.exception('Publishing simulation %s failed', simulation_id)


def _fail_simulation(simulation_id, failure):
    with Session.begin() as db:
        simulation = db.get(Simulation, simulation_id, with_for_update=True)
        if simulation is None or simulation.status == Status.FAILED:
            return
        simulation.status = Status.FAILED
        simulation.error = failure[:500]
        simulation.finished_at = now()
        record(db, 'worker', 'simulation.failed', simulation_id, reason=failure)


def revise_reserved(run_id, reserved_minutes, actor):
    """Recompute probabilities from stored samples; never redraw them.

    If any batch is still pending, the revision stays unpublished and the
    previous result and export remain in place until every batch is in.
    """
    with Session.begin() as db:
        simulation = _latest_simulation(db, run_id)
        if simulation is not None:
            simulation = db.get(Simulation, simulation.id, with_for_update=True)
        run = db.get(Run, run_id)
        if run is None or simulation is None or simulation.estimation is None:
            raise ValueError('该运行还没有可复算的模拟样本')
        if simulation.status == Status.FAILED:
            raise ValueError('该运行的模拟批次已失败，请重新发起运行')
        failed = db.scalar(select(func.count()).select_from(SimulationBatch)
                           .where(SimulationBatch.simulation_id == simulation.id,
                                  SimulationBatch.status == Status.FAILED))
        if failed:
            raise ValueError('该运行的模拟批次已失败，请重新发起运行')
        value = float(reserved_minutes)
        if value < 1 or value > 10080:
            raise ValueError('每日预留分钟数需在 1 到 10080 之间')
        if value != float(simulation.reserved_minutes):
            simulation.reserved_minutes = value
            simulation.revision += 1
            record(db, actor, 'simulation.revised', run_id, reserved_minutes=value,
                   revision=simulation.revision)
        revision = simulation.revision
        simulation_id = simulation.id
    published = False
    try:
        _publish(simulation_id)
        published = True
    except Incomplete:
        published = False
    return {'published': published, 'revision': revision, 'reserved_minutes': value}
