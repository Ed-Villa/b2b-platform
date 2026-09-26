"""Publish an example with the required Kafka key, cross-platform."""
import json
import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
path = Path(sys.argv[1]) if len(sys.argv) > 1 else root / 'examples/approved.json'
event = json.loads(path.read_text(encoding='utf-8'))
line = event.get('orderId', 'invalid') + '|' + json.dumps(event, separators=(',', ':')) + '\n'
subprocess.run(
    ['docker', 'compose', 'exec', '-T', 'kafka', '/opt/kafka/bin/kafka-console-producer.sh', '--bootstrap-server',
     'kafka:19092', '--topic', 'orders.created.v1', '--property', 'parse.key=true', '--property', 'key.separator=|'],
    input=line, text=True, cwd=root, check=True)
