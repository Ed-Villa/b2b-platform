"""End-to-end checks against docker compose; uses unique IDs and preserves data."""
import copy
import json
import os
import subprocess
import time
import urllib.request
import uuid
from decimal import Decimal
from jsonschema import Draft202012Validator, FormatChecker
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def run(args, **kwargs):
    return subprocess.run(['docker', 'compose', *args], cwd=ROOT, text=True, capture_output=True, check=True,
                          **kwargs).stdout


def mongo(expression):
    return json.loads(
        run(['exec', '-T', 'mongo', 'mongosh', '--quiet', os.environ.get('MONGODB_DATABASE', 'b2b'), '--eval',
             'print(JSON.stringify(' + expression + '))']))


def wait_for(check, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value: return value
        time.sleep(1)
    raise AssertionError('Timed out waiting for expected durable result')


def schema(name): return json.loads((ROOT / 'contracts' / name).read_text(), parse_float=Decimal)


# Validate actual providers against their checked-in response contracts.
for service, url in [('clients', 'http://localhost:3000/clients/CLI-99821'),
                     ('products', 'http://localhost:8081/products/PRD-001?market=MX')]:
    with urllib.request.urlopen(url, timeout=5) as response: body = json.load(response)
    Draft202012Validator(schema(service + '.openapi.json')['components']['schemas']['Resource']).validate(body)

token = uuid.uuid4().hex[:12]
base = json.loads((ROOT / 'examples/approved.json').read_text())
base.update(eventId='EVT-' + token, orderId='ORD-' + token, eventVersion=1, orderVersion=2)
rejected = copy.deepcopy(base);
rejected.update(eventId='REJ-' + token, orderId='REJ-' + token, clientId='CLI-MX-2')
invalid = copy.deepcopy(base);
invalid.update(eventId='INV-' + token, orderId='INV-' + token, currency='USD')
events = [base, base, rejected, invalid]
lines = ''.join(e['orderId'] + '|' + json.dumps(e, separators=(',', ':')) + '\n' for e in events)
run(['exec', '-T', 'kafka', '/opt/kafka/bin/kafka-console-producer.sh', '--bootstrap-server', 'kafka:19092', '--topic',
     'orders.created.v1', '--property', 'parse.key=true', '--property', 'key.separator=|'], input=lines)


def order(order_id): return mongo('db.orders.findOne(' + json.dumps({'_id': order_id}) + ')')


approved_doc = wait_for(lambda: order(base['orderId']))
assert approved_doc['status'] == 'APPROVED', approved_doc
assert wait_for(lambda: order(rejected['orderId']))['status'] == 'REJECTED'
assert order(invalid['orderId']) is None


def entries(): return mongo('db.outbox.find({key:{$in:' + json.dumps(
    [base['orderId'], rejected['orderId'], invalid['orderId']]) + '}}).toArray()')


outbox = wait_for(
    lambda: (rows if len(rows) == 3 and all(row['sent'] for row in rows) else None) if (rows := entries()) else None)
for row in outbox:
    body = json.loads(row['payload'], parse_float=Decimal)
    name = 'orders.processed.v1.schema.json' if row[
                                                    'topic'] == 'orders.processed.v1' else 'orders.processing.dlt.schema.json'
    Draft202012Validator(schema(name), format_checker=FormatChecker()).validate(body)
    if body.get('status') in ['APPROVED', 'REJECTED']:
        assert body['eventVersion'] == 1 and body['sourceEventVersion'] == 1 and body['orderVersion'] == 2, body
    if body.get('status') == 'APPROVED':
        assert body['totals']['grandTotal'] == Decimal('2021.39'), body
    if row['topic'] == 'orders.processing.dlt': assert json.loads(body['originalMessage']) == invalid
assert mongo('db.revisions.countDocuments(' + json.dumps({'orderId': base['orderId']}) + ')') == 1

# Read the broker, not just sent flags. A timeout after reading is normal for a finite smoke run.
expected = {row['_id'] for row in outbox}
received = set()
for topic in ['orders.processed.v1', 'orders.processing.dlt']:
    proc = subprocess.run(
        ['docker', 'compose', 'exec', '-T', 'kafka', '/opt/kafka/bin/kafka-console-consumer.sh', '--bootstrap-server',
         'kafka:19092', '--topic', topic, '--from-beginning', '--timeout-ms', '5000'], cwd=ROOT, text=True,
        capture_output=True)
    for line in proc.stdout.splitlines():
        try:
            received.add(json.loads(line)['eventId'])
        except (ValueError, KeyError):
            pass
assert expected <= received, ('Missing broker events', expected - received)
print(
    'PASS: live contracts, approval, rejection, invalid DLT, duplicate, exact totals, MongoDB and Kafka publication. Run ' + token)
