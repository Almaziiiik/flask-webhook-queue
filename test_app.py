import os

# Отключаем фоновый воркер перед импортом приложения для детерминированности тестов
os.environ['DISABLE_WORKER'] = 'true'

from app import app, db, Event, Task
import pytest


@pytest.fixture
def client():
  app.config['TESTING'] = True
  app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
  with app.app_context():
    db.create_all()
    yield app.test_client()
    db.drop_all()


def test_webhook_success(client):
  response = client.post(
      '/webhook',
      json={
          'event_id': 'abc-123',
          'type': 'payment_succeeded',
          'payload': {'amount': 1500},
      },
  )
  assert response.status_code == 201
  data = response.get_json()
  assert data['status'] == 'ok'

  with app.app_context():
    assert Event.query.filter_by(event_id='abc-123').count() == 1
    assert Task.query.filter_by(event_id='abc-123').count() == 1


def test_webhook_duplicate_idempotency(client):
  payload = {
      'event_id': 'abc-dup',
      'type': 'payment_succeeded',
      'payload': {'amount': 500},
  }

  # Первый запрос — создается запись (201)
  res1 = client.post('/webhook', json=payload)
  assert res1.status_code == 201

  # Повторный запрос с тем же event_id — идемпотентный ответ 200 без дублей
  res2 = client.post('/webhook', json=payload)
  assert res2.status_code == 200
  data = res2.get_json()
  assert 'already exists' in data['message']

  with app.app_context():
    assert Event.query.filter_by(event_id='abc-dup').count() == 1
    assert Task.query.filter_by(event_id='abc-dup').count() == 1


def test_webhook_invalid_body(client):
  # Некорректный JSON / пропущенные поля
  response = client.post('/webhook', json={'event_id': 'abc-bad'})
  assert response.status_code == 400
  data = response.get_json()
  assert 'error' in data


def test_stats_endpoint(client):
  client.post(
      '/webhook', json={'event_id': 'e1', 'type': 'sub_created', 'payload': {}}
  )
  client.post(
      '/webhook', json={'event_id': 'e2', 'type': 'sub_created', 'payload': {}}
  )

  response = client.get('/stats')
  assert response.status_code == 200
  data = response.get_json()

  assert data['events']['sub_created'] == 2
  assert data['tasks']['pending'] == 2