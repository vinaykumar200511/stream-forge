import sqlite3
from pathlib import Path

base_dir = Path(__file__).resolve().parent
schema_path = base_dir / 'fleet_schema.sql'
db_path = base_dir / 'fleet.db'

if db_path.exists():
    db_path.unlink()

print(f'Creating database: {db_path}')

with sqlite3.connect(db_path) as conn:
    conn.executescript(schema_path.read_text(encoding='utf-8'))

    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()

    counts = {
        'owners': conn.execute('SELECT COUNT(*) FROM owners').fetchone()[0],
        'trucks': conn.execute('SELECT COUNT(*) FROM trucks').fetchone()[0],
        'drivers': conn.execute('SELECT COUNT(*) FROM drivers').fetchone()[0],
        'trips': conn.execute('SELECT COUNT(*) FROM trips').fetchone()[0],
    }

    print('TABLES:', tables)
    print('COUNTS:', counts)

    print('SAMPLE_TRIP:', conn.execute(
        'SELECT trip_id, origin, destination, trip_status FROM trips ORDER BY trip_id LIMIT 1'
    ).fetchone())
