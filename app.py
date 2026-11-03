import datetime
import json
import os
import random
import threading
import time
from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///webhooks.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class Event(db.Model):
  __tablename__ = 'events'
  id = db.Column(db.Integer, primary_key=True)
  event_id = db.Column(db.String(100), unique=True, nullable=False)
  type = db.Column(db.String(100), nullable=False)
  payload = db.Column(db.JSON, nullable=False)
  created_at = db.Column(
      db.DateTime,
      default=lambda: datetime.datetime.now(datetime.timezone.utc).replace(
          tzinfo=None
      ),
      nullable=False,
  )


class Task(db.Model):
  __tablename__ = 'tasks'
  id = db.Column(db.Integer, primary_key=True)
  event_id = db.Column(
      db.String(100),
      db.ForeignKey('events.event_id'),
      unique=True,
      nullable=False,
  )
  status = db.Column(
      db.String(20), default='pending', nullable=False
  )  # pending, success, failed, skipped
  attempts = db.Column(db.Integer, default=0, nullable=False)
  next_retry_at = db.Column(db.DateTime, nullable=True)
  updated_at = db.Column(
      db.DateTime,
      default=lambda: datetime.datetime.now(datetime.timezone.utc).replace(
          tzinfo=None
      ),
      onupdate=lambda: datetime.datetime.now(datetime.timezone.utc).replace(
          tzinfo=None
      ),
      nullable=False,
  )


with app.app_context():
  db.create_all()


def process_event(event_type, payload):
  """Заглушка обработчика событий с имитацией ошибок."""
  if random.random() < 0.4:
    raise RuntimeError('Simulated processing failure')

  if event_type == 'payment_succeeded':
    print(f'[Worker] Processing successful payment: {payload}')
  elif event_type == 'payment_failed':
    print(f'[Worker] Processing failed payment: {payload}')
  else:
    print(f'[Worker] Processing event type {event_type}: {payload}')


def worker_loop():
  """Фоновый воркер для обработки задач из очереди."""
  while True:
    time.sleep(1)
    with app.app_context():
      try:
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        # Выбираем задачу в статусе pending, время которой подошло для выполнения
        task = (
            Task.query.filter(
                Task.status == 'pending',
                (Task.next_retry_at == None) | (Task.next_retry_at <= now),  # noqa: E711
            )
            .order_by(Task.id)
            .first()
        )

        if not task:
          continue

        event = Event.query.filter_by(event_id=task.event_id).first()
        if not event:
          task.status = 'skipped'
          db.session.commit()
          continue

        task.attempts += 1
        db.session.commit()

        try:
          process_event(event.type, event.payload)
          task.status = 'success'
          task.next_retry_at = None
          db.session.commit()
        except Exception as e:
          print(f'[Worker] Task {task.id} failed (attempt {task.attempts}): {e}')
          if task.attempts >= 3:
            task.status = 'failed'
            task.next_retry_at = None
          else:
            task.status = 'pending'
            # Экспоненциальная задержка: 2^attempts секунд (2с, 4с, 8с)
            backoff_seconds = 2**task.attempts
            task.next_retry_at = (
                datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                + datetime.timedelta(seconds=backoff_seconds)
            )
          db.session.commit()
      except Exception as ex:
        print(f'[Worker Error]: {ex}')


# Запуск фонового воркера в отдельном потоке (если не отключено для тестов)
if os.environ.get('DISABLE_WORKER') != 'true':
  t = threading.Thread(target=worker_loop, daemon=True)
  t.start()


@app.route('/webhook', methods=['POST'])
def webhook():
  if not request.is_json:
    return jsonify({'error': 'Content-Type must be application/json'}), 400

  data = request.get_json()
  if not isinstance(data, dict):
    return jsonify({'error': 'Invalid JSON body'}), 400

  event_id = data.get('event_id')
  event_type = data.get('type')
  payload = data.get('payload')

  if not event_id or not event_type or payload is None:
    return (
        jsonify({'error': 'Missing required fields: event_id, type, payload'}),
        400,
    )

  # Проверка на идемпотентность (если event_id уже есть, возвращаем 200)
  existing_event = Event.query.filter_by(event_id=event_id).first()
  if existing_event:
    return (
        jsonify(
            {'status': 'ok', 'message': 'Event already exists (idempotent)'}
        ),
        200,
    )

  try:
    new_event = Event(event_id=event_id, type=event_type, payload=payload)
    db.session.add(new_event)

    new_task = Task(event_id=event_id, status='pending', attempts=0)
    db.session.add(new_task)

    db.session.commit()
  except Exception as e:
    db.session.rollback()
    # Защита от гонки (race condition) при одновременных запросах с одинаковым event_id
    if Event.query.filter_by(event_id=event_id).first():
      return (
          jsonify(
              {'status': 'ok', 'message': 'Event already exists (idempotent)'}
          ),
          200,
      )
    return jsonify({'error': str(e)}), 500

  return jsonify({'status': 'ok', 'message': 'Webhook received and queued'}), 201


@app.route('/stats', methods=['GET'])
def stats():
  event_counts = (
      db.session.query(Event.type, db.func.count(Event.id))
      .group_by(Event.type)
      .all()
  )
  events_stats = {t: count for t, count in event_counts}

  task_counts = (
      db.session.query(Task.status, db.func.count(Task.id))
      .group_by(Task.status)
      .all()
  )
  tasks_stats = {s: count for s, count in task_counts}

  return jsonify({'events': events_stats, 'tasks': tasks_stats}), 200


if __name__ == '__main__':
  app.run(debug=True, port=5000)