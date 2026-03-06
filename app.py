import os
import json
import sqlite3
from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)
# On Render free tier the app root is read-only; /tmp is writable.
# Locally this falls back to a climbs.db next to app.py.
_default_db = os.path.join(app.root_path, 'climbs.db')
DATABASE = os.environ.get('DATABASE_PATH', _default_db)

# ── Grade definitions ──────────────────────────────────────────────────────────
# Points use a progressive scale that meaningfully rewards harder sends.
GRADE_COLORS = [
    {'key': 'orange', 'label': 'Orange', 'grade': 'VB',    'points':  1, 'hex': '#FF7700', 'text': '#fff'},
    {'key': 'yellow', 'label': 'Yellow', 'grade': 'V0–V2', 'points':  3, 'hex': '#FFD700', 'text': '#111'},
    {'key': 'green',  'label': 'Green',  'grade': 'V1–V3', 'points':  6, 'hex': '#27AE60', 'text': '#fff'},
    {'key': 'blue',   'label': 'Blue',   'grade': 'V2–V4', 'points': 10, 'hex': '#2980B9', 'text': '#fff'},
    {'key': 'purple', 'label': 'Purple', 'grade': 'V3–V5', 'points': 15, 'hex': '#8E44AD', 'text': '#fff'},
    {'key': 'red',    'label': 'Red',    'grade': 'V4–V6', 'points': 21, 'hex': '#E74C3C', 'text': '#fff'},
    {'key': 'black',  'label': 'Black',  'grade': 'V5–V7', 'points': 30, 'hex': '#222222', 'text': '#fff'},
    {'key': 'white',  'label': 'White',  'grade': 'V7+',   'points': 50, 'hex': '#E8E8E8', 'text': '#111'},
]

GRADE_MAP    = {g['key']: g for g in GRADE_COLORS}
VALID_TYPES  = {'overhang', 'neutral', 'slab'}
VALID_STYLES = {'dyno', 'static'}
VALID_HOLDS  = {'jugs', 'crimps', 'slopers', 'dual-tex'}
VALID_SORTS  = {'total_points', 'total_climbs'} | set(GRADE_MAP.keys())


# ── Database ───────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            name       TEXT    NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS climbs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            grade_color TEXT    NOT NULL,
            climb_type  TEXT    NOT NULL,
            style       TEXT    NOT NULL,
            holds       TEXT    NOT NULL,
            points      INTEGER NOT NULL,
            date        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
    ''')
    conn.commit()
    conn.close()


# ── Template helpers ──────────────────────────────────────────────────────────

@app.template_filter('from_json')
def from_json_filter(s):
    try:
        return json.loads(s)
    except (ValueError, TypeError):
        return []


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    conn   = get_db()
    users  = conn.execute('SELECT * FROM users ORDER BY name').fetchall()
    recent = conn.execute('''
        SELECT c.*, u.name AS climber
        FROM   climbs c
        JOIN   users  u ON c.user_id = u.id
        ORDER  BY c.date DESC
        LIMIT  20
    ''').fetchall()
    conn.close()
    return render_template('index.html', users=users, recent=recent, grade_map=GRADE_MAP)


@app.route('/add_user', methods=['POST'])
def add_user():
    name = request.form.get('name', '').strip()
    if 1 <= len(name) <= 50:
        conn = get_db()
        try:
            conn.execute('INSERT INTO users (name) VALUES (?)', (name,))
            conn.commit()
        except sqlite3.IntegrityError:
            pass  # name taken — silently skip
        conn.close()
    return redirect(url_for('index'))


@app.route('/log', methods=['GET', 'POST'])
def log_climb():
    conn  = get_db()
    users = conn.execute('SELECT * FROM users ORDER BY name').fetchall()
    conn.close()

    if request.method == 'POST':
        error = None

        # Validate user
        try:
            user_id = int(request.form.get('user_id', ''))
        except (ValueError, TypeError):
            user_id = None
            error = 'Please select a climber.'

        grade = request.form.get('grade_color', '')
        ctype = request.form.get('climb_type', '')
        style = request.form.get('style', '')
        holds = request.form.getlist('holds')

        if not error:
            if grade not in GRADE_MAP:
                error = 'Please select a grade.'
            elif ctype not in VALID_TYPES:
                error = 'Please select a climb type.'
            elif style not in VALID_STYLES:
                error = 'Please select a style.'
            elif not holds:
                error = 'Please select at least one hold type.'
            elif not all(h in VALID_HOLDS for h in holds):
                error = 'Invalid hold type submitted.'

        if error:
            return render_template('log.html', users=users, grades=GRADE_COLORS, error=error)

        points = GRADE_MAP[grade]['points']
        conn = get_db()
        conn.execute(
            'INSERT INTO climbs (user_id, grade_color, climb_type, style, holds, points) VALUES (?,?,?,?,?,?)',
            (user_id, grade, ctype, style, json.dumps(holds), points)
        )
        conn.commit()
        conn.close()
        return redirect(url_for('index'))

    return render_template('log.html', users=users, grades=GRADE_COLORS)


@app.route('/leaderboard')
def leaderboard():
    sort_by = request.args.get('sort', 'total_points')
    if sort_by not in VALID_SORTS:
        sort_by = 'total_points'

    conn       = get_db()
    users      = conn.execute('SELECT * FROM users').fetchall()
    all_climbs = conn.execute('SELECT * FROM climbs').fetchall()
    conn.close()

    stats = {
        u['id']: {
            'name':         u['name'],
            'total_points': 0,
            'total_climbs': 0,
            'grade_counts': {g['key']: 0 for g in GRADE_COLORS},
        }
        for u in users
    }

    for climb in all_climbs:
        uid   = climb['user_id']
        color = climb['grade_color']
        if uid in stats and color in stats[uid]['grade_counts']:
            stats[uid]['total_points'] += climb['points']
            stats[uid]['total_climbs'] += 1
            stats[uid]['grade_counts'][color] += 1

    rows = list(stats.values())

    if sort_by == 'total_climbs':
        rows.sort(key=lambda x: x['total_climbs'], reverse=True)
    elif sort_by in GRADE_MAP:
        rows.sort(key=lambda x: x['grade_counts'][sort_by], reverse=True)
    else:
        rows.sort(key=lambda x: x['total_points'], reverse=True)

    return render_template('leaderboard.html',
                           rows=rows,
                           grades=GRADE_COLORS,
                           grade_map=GRADE_MAP,
                           sort_by=sort_by)


# ── Startup ───────────────────────────────────────────────────────────────────

with app.app_context():
    init_db()

if __name__ == '__main__':
    app.run(debug=True)
