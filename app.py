import os
import json
import sqlite3
from flask import Flask, render_template, request, redirect, url_for, session as flask_session

app = Flask(__name__)
# On Render free tier the app root is read-only; /tmp is writable.
# Locally this falls back to a climbs.db next to app.py.
_default_db = os.path.join(app.root_path, 'climbs.db')
DATABASE         = os.environ.get('DATABASE_PATH', _default_db)
app.secret_key   = os.environ.get('SECRET_KEY', 'dev-secret-change-in-prod')
ADMIN_PASSWORD   = os.environ.get('ADMIN_PASSWORD', '')

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
        CREATE TABLE IF NOT EXISTS sessions (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            gym_name   TEXT    NOT NULL DEFAULT 'The Wall',
            notes      TEXT    NOT NULL DEFAULT '',
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ended_at   TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS climbs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            session_id  INTEGER,
            grade_color TEXT    NOT NULL,
            climb_type  TEXT    NOT NULL,
            style       TEXT    NOT NULL,
            holds       TEXT    NOT NULL,
            points      INTEGER NOT NULL,
            flashed     INTEGER NOT NULL DEFAULT 0,
            attempts    INTEGER NOT NULL DEFAULT 1,
            date        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id)    REFERENCES users(id),
            FOREIGN KEY (session_id) REFERENCES sessions(id)
        );
    ''')
    # Migrate: add session_id to existing climbs table if absent
    cols = [row[1] for row in conn.execute('PRAGMA table_info(climbs)')]
    if 'session_id' not in cols:
        conn.execute('ALTER TABLE climbs ADD COLUMN session_id INTEGER REFERENCES sessions(id)')
    if 'flashed' not in cols:
        conn.execute('ALTER TABLE climbs ADD COLUMN flashed INTEGER NOT NULL DEFAULT 0')
    if 'attempts' not in cols:
        conn.execute('ALTER TABLE climbs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 1')
    conn.commit()
    conn.close()


# ── Admin helpers ────────────────────────────────────────────────────────────

def is_admin():
    return flask_session.get('is_admin', False)


@app.context_processor
def inject_admin():
    return {'is_admin': is_admin()}


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
        SELECT c.*, u.name AS climber, s.gym_name AS session_gym
        FROM   climbs c
        JOIN   users  u ON c.user_id = u.id
        LEFT   JOIN sessions s ON c.session_id = s.id
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
    active_sessions = conn.execute(
        'SELECT * FROM sessions WHERE ended_at IS NULL ORDER BY started_at DESC'
    ).fetchall()
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
        flashed  = 1 if request.form.get('flashed') == '1' else 0
        try:
            attempts = max(1, int(request.form.get('attempts', 1)))
        except (ValueError, TypeError):
            attempts = 1
        if flashed:
            attempts = 1  # a flash is always 1 attempt

        # Optional session
        raw_sid = request.form.get('session_id', '').strip()
        try:
            session_id = int(raw_sid) if raw_sid else None
        except ValueError:
            session_id = None

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
            return render_template('log.html', users=users, grades=GRADE_COLORS,
                                   active_sessions=active_sessions, error=error)

        # Verify session is still open
        if session_id is not None:
            conn = get_db()
            valid = conn.execute(
                'SELECT id FROM sessions WHERE id = ? AND ended_at IS NULL', (session_id,)
            ).fetchone()
            conn.close()
            if valid is None:
                session_id = None

        points = GRADE_MAP[grade]['points']
        conn = get_db()
        conn.execute(
            'INSERT INTO climbs (user_id, session_id, grade_color, climb_type, style, holds, points, flashed, attempts) '
            'VALUES (?,?,?,?,?,?,?,?,?)',
            (user_id, session_id, grade, ctype, style, json.dumps(holds), points, flashed, attempts)
        )
        conn.commit()
        conn.close()
        if session_id:
            return redirect(url_for('session_detail', session_id=session_id))
        return redirect(url_for('index'))

    selected_session_id = request.args.get('session_id', type=int)
    return render_template('log.html', users=users, grades=GRADE_COLORS,
                           active_sessions=active_sessions,
                           selected_session_id=selected_session_id)


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


# ── Admin routes ─────────────────────────────────────────────────────────────

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    error = None
    if request.method == 'POST':
        pw = request.form.get('password', '')
        if ADMIN_PASSWORD and pw == ADMIN_PASSWORD:
            flask_session['is_admin'] = True
            next_url = request.form.get('next', '') or url_for('index')
            return redirect(next_url)
        error = 'Wrong password.'
    return render_template('admin_login.html', error=error,
                           next=request.args.get('next', ''))


@app.route('/admin/logout', methods=['POST'])
def admin_logout():
    flask_session.pop('is_admin', None)
    return redirect(url_for('index'))


@app.route('/delete_climb/<int:climb_id>', methods=['POST'])
def delete_climb(climb_id):
    if not is_admin():
        return redirect(url_for('admin_login'))
    conn = get_db()
    row  = conn.execute('SELECT session_id FROM climbs WHERE id = ?', (climb_id,)).fetchone()
    conn.execute('DELETE FROM climbs WHERE id = ?', (climb_id,))
    conn.commit()
    conn.close()
    if row and row['session_id']:
        return redirect(url_for('session_detail', session_id=row['session_id']))
    return redirect(url_for('index'))


@app.route('/delete_session/<int:session_id>', methods=['POST'])
def delete_session(session_id):
    if not is_admin():
        return redirect(url_for('admin_login'))
    conn = get_db()
    conn.execute('DELETE FROM climbs   WHERE session_id = ?', (session_id,))
    conn.execute('DELETE FROM sessions WHERE id = ?',         (session_id,))
    conn.commit()
    conn.close()
    return redirect(url_for('sessions'))


# ── Session routes ───────────────────────────────────────────────────────────

@app.route('/sessions')
def sessions():
    conn = get_db()
    rows = conn.execute('''
        SELECT s.*,
               COUNT(c.id)                AS climb_count,
               COALESCE(SUM(c.points), 0) AS total_points
        FROM   sessions s
        LEFT   JOIN climbs c ON c.session_id = s.id
        GROUP  BY s.id
        ORDER  BY s.started_at DESC
    ''').fetchall()
    conn.close()
    return render_template('sessions.html', sessions=rows)


@app.route('/start_session', methods=['POST'])
def start_session():
    gym   = request.form.get('gym_name', '').strip()[:100] or 'The Wall'
    notes = request.form.get('notes', '').strip()[:500]
    conn  = get_db()
    cur   = conn.execute('INSERT INTO sessions (gym_name, notes) VALUES (?, ?)', (gym, notes))
    sid   = cur.lastrowid
    conn.commit()
    conn.close()
    return redirect(url_for('session_detail', session_id=sid))


@app.route('/session/<int:session_id>')
def session_detail(session_id):
    conn    = get_db()
    session = conn.execute('SELECT * FROM sessions WHERE id = ?', (session_id,)).fetchone()
    if session is None:
        conn.close()
        return redirect(url_for('sessions'))
    climbs = conn.execute('''
        SELECT c.*, u.name AS climber
        FROM   climbs c
        JOIN   users  u ON c.user_id = u.id
        WHERE  c.session_id = ?
        ORDER  BY c.date ASC
    ''', (session_id,)).fetchall()
    conn.close()
    climber_stats = {}
    total_points  = 0
    for c in climbs:
        total_points += c['points']
        name = c['climber']
        if name not in climber_stats:
            climber_stats[name] = {'total_points': 0, 'total_climbs': 0}
        climber_stats[name]['total_points'] += c['points']
        climber_stats[name]['total_climbs']  += 1
    return render_template('session.html',
                           session=session,
                           climbs=climbs,
                           climber_stats=climber_stats,
                           total_points=total_points,
                           grade_map=GRADE_MAP)


@app.route('/end_session/<int:session_id>', methods=['POST'])
def end_session(session_id):
    conn = get_db()
    conn.execute(
        'UPDATE sessions SET ended_at = CURRENT_TIMESTAMP WHERE id = ? AND ended_at IS NULL',
        (session_id,)
    )
    conn.commit()
    conn.close()
    return redirect(url_for('session_detail', session_id=session_id))


# ── Stats route ───────────────────────────────────────────────────────────────

@app.route('/stats')
def stats():
    conn = get_db()

    # Record single day: user with most climbs in one calendar day
    record_day = conn.execute('''
        SELECT u.name, DATE(c.date) AS day, COUNT(*) AS cnt
        FROM   climbs c
        JOIN   users  u ON c.user_id = u.id
        GROUP  BY c.user_id, DATE(c.date)
        ORDER  BY cnt DESC
        LIMIT  1
    ''').fetchone()

    # Best session haul: user with most points in a single session
    best_session = conn.execute('''
        SELECT u.name, s.gym_name, s.started_at,
               SUM(c.points) AS pts, COUNT(c.id) AS cnt
        FROM   climbs   c
        JOIN   users    u ON c.user_id    = u.id
        JOIN   sessions s ON c.session_id = s.id
        WHERE  c.session_id IS NOT NULL
        GROUP  BY c.user_id, c.session_id
        ORDER  BY pts DESC
        LIMIT  1
    ''').fetchone()

    users      = conn.execute('SELECT * FROM users ORDER BY name').fetchall()
    all_climbs = conn.execute('SELECT * FROM climbs').fetchall()
    conn.close()

    grade_order = [g['key'] for g in GRADE_COLORS]  # ascending difficulty
    user_stats  = {
        u['id']: {'id': u['id'], 'name': u['name'], 'total_climbs': 0,
                  'total_points': 0, 'best_grade': None,
                  'flashes': 0}
        for u in users
    }
    for c in all_climbs:
        uid, color = c['user_id'], c['grade_color']
        if uid not in user_stats or color not in GRADE_MAP:
            continue
        s = user_stats[uid]
        s['total_climbs'] += 1
        s['total_points'] += c['points']
        if c['flashed']:
            s['flashes'] += 1
        cur = s['best_grade']
        if cur is None or grade_order.index(color) > grade_order.index(cur['key']):
            s['best_grade'] = GRADE_MAP[color]

    for s in user_stats.values():
        s['flash_pct'] = round(100 * s['flashes'] / s['total_climbs']) if s['total_climbs'] else 0

    user_rows = sorted(user_stats.values(),
                       key=lambda x: x['total_points'], reverse=True)

    return render_template('stats.html',
                           record_day=record_day,
                           best_session=best_session,
                           user_rows=user_rows)


# ── Climber profile ──────────────────────────────────────────────────────────

@app.route('/climber/<int:user_id>')
def climber_profile(user_id):
    conn = get_db()
    user = conn.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
    if user is None:
        conn.close()
        return redirect(url_for('stats'))

    climbs = conn.execute('''
        SELECT c.*, s.gym_name AS session_gym
        FROM   climbs c
        LEFT   JOIN sessions s ON c.session_id = s.id
        WHERE  c.user_id = ?
        ORDER  BY c.date ASC
    ''', (user_id,)).fetchall()

    # Sessions this climber attended (with their personal totals)
    session_rows = conn.execute('''
        SELECT s.id, s.gym_name, s.started_at, s.ended_at,
               COUNT(c.id)                AS climb_count,
               COALESCE(SUM(c.points), 0) AS session_points
        FROM   sessions s
        JOIN   climbs   c ON c.session_id = s.id AND c.user_id = ?
        GROUP  BY s.id
        ORDER  BY s.started_at DESC
    ''', (user_id,)).fetchall()
    conn.close()

    total_climbs  = len(climbs)
    total_points  = sum(c['points'] for c in climbs)
    total_flashes = sum(1 for c in climbs if c['flashed'])
    flash_pct     = round(100 * total_flashes / total_climbs) if total_climbs else 0

    grade_order = [g['key'] for g in GRADE_COLORS]
    best_grade  = None
    for c in climbs:
        if c['grade_color'] in GRADE_MAP:
            cur = best_grade
            if cur is None or grade_order.index(c['grade_color']) > grade_order.index(cur['key']):
                best_grade = GRADE_MAP[c['grade_color']]

    # Grade breakdown
    grade_counts = {g['key']: 0 for g in GRADE_COLORS}
    for c in climbs:
        if c['grade_color'] in grade_counts:
            grade_counts[c['grade_color']] += 1

    # Flash rate per grade
    grade_flashes = {g['key']: {'sends': 0, 'flashes': 0} for g in GRADE_COLORS}
    for c in climbs:
        if c['grade_color'] in grade_flashes:
            grade_flashes[c['grade_color']]['sends'] += 1
            if c['flashed']:
                grade_flashes[c['grade_color']]['flashes'] += 1

    # Type / style / holds breakdown
    type_counts  = {t: 0 for t in VALID_TYPES}
    style_counts = {s: 0 for s in VALID_STYLES}
    hold_counts  = {h: 0 for h in VALID_HOLDS}
    for c in climbs:
        if c['climb_type'] in type_counts:
            type_counts[c['climb_type']] += 1
        if c['style'] in style_counts:
            style_counts[c['style']] += 1
        for h in json.loads(c['holds']):
            if h in hold_counts:
                hold_counts[h] += 1

    recent_climbs = list(reversed(climbs))[:15]

    return render_template('climber.html',
                           user=user,
                           total_climbs=total_climbs,
                           total_points=total_points,
                           total_flashes=total_flashes,
                           flash_pct=flash_pct,
                           best_grade=best_grade,
                           grade_counts=grade_counts,
                           grade_flashes=grade_flashes,
                           type_counts=type_counts,
                           style_counts=style_counts,
                           hold_counts=hold_counts,
                           recent_climbs=recent_climbs,
                           session_rows=session_rows,
                           grades=GRADE_COLORS,
                           grade_map=GRADE_MAP)


# ── Startup ───────────────────────────────────────────────────────────────────

with app.app_context():
    init_db()

if __name__ == '__main__':
    app.run(debug=True)
