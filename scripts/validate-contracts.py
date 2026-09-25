import json
from pathlib import Path
from decimal import Decimal
from jsonschema import Draft202012Validator, FormatChecker
from openapi_spec_validator import validate_spec

root = Path(__file__).resolve().parent.parent
def read(path): return json.loads(path.read_text(encoding='utf-8'), parse_float=Decimal)
for path in (root/'contracts').glob('*.schema.json'):
    Draft202012Validator.check_schema(read(path))
for path in (root/'contracts').glob('*.openapi.json'):
    validate_spec(read(path))
input_validator = Draft202012Validator(read(root/'contracts/orders.created.v1.schema.json'), format_checker=FormatChecker())
for name in ['approved', 'rejected', 'approved-revision-2']:
    event = read(root/f'examples/{name}.json')
    input_validator.validate(event)
    ids = [line['productId'] for line in event['items']]
    assert len(ids) == len(set(ids))
assert not input_validator.is_valid(read(root/'examples/invalid.json'))
Draft202012Validator(read(root/'contracts/orders.processed.v1.schema.json'),format_checker=FormatChecker()).validate(read(root/'examples/processed.json'))
for name in ['processed', 'processed-revision-2']:
    event = read(root/f'examples/{name}.json')
    Draft202012Validator(read(root/'contracts/orders.processed.v1.schema.json'),format_checker=FormatChecker()).validate(event)
    assert event['eventVersion'] == event['sourceEventVersion'] == 1
    assert event['orderVersion'] == (2 if name.endswith('revision-2') else 1)
print('Schemas, OpenAPI and examples valid (including order revision 2 on schema v1)')
