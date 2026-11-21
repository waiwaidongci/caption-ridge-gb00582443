from src.utils.time import iso


def dataset(item):
    return {'id': item.id, 'name': item.name, 'filename': item.filename, 'row_count': item.row_count,
            'checksum': item.checksum, 'author': item.author, 'created_at': iso(item.created_at)}


def run(item, detail=False):
    item_view = {'id': item.id, 'dataset_id': item.dataset_id, 'name': item.name, 'status': item.status,
                 'author': item.author, 'review': item.review, 'decision': item.decision,
                 'created_at': iso(item.created_at), 'finished_at': iso(item.finished_at),
                 'scheduled_at': iso(item.scheduled_at), 'schedule_id': item.schedule_id,
                 'has_export': bool(item.export_file), 'error': item.error}
    if detail:
        item_view.update(params=item.params, result=item.result, review_note=item.review_note)
        item_view['simulation'] = simulation_view(item.__dict__.get('_simulation'),
                                                  item.__dict__.get('_batch_counts'))
    return item_view


def simulation_view(simulation, counts=None):
    if simulation is None:
        return None
    counts = counts or {}
    return {'revision': simulation.revision,
            'published_revision': simulation.published_revision,
            'reserved_minutes': simulation.reserved_minutes,
            'iterations': simulation.iterations,
            'batch_size': simulation.batch_size,
            'status': simulation.status,
            'batch_count': sum(counts.values()),
            'completed_batches': counts.get('complete', 0),
            'failed_batches': counts.get('failed', 0),
            'published_at': iso(simulation.published_at)}


def schedule(item):
    from src.domain.spec import SPEC
    return {'id': item.id, 'name': item.name, 'dataset_id': item.dataset_id, 'recipient': item.recipient,
            'next_at': iso(item.next_at), 'repeat_daily': item.repeat_daily, 'enabled': item.enabled,
            'next_report_day': item.params.get(SPEC['schedule_date_param']) if item.enabled else None}


def delivery(item):
    return {'id': item.id, 'run_id': item.run_id, 'recipient': item.recipient,
            'state': item.state, 'detail': item.detail, 'created_at': iso(item.created_at)}
