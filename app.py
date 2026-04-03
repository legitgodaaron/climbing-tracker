import os
import json
import psycopg2
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor
from psycopg2 import pool as pg_pool
from flask import Flask, render_template, request, redirect, url_for, session as flask_session, g, flash
from werkzeug.security import generate_password_hash, check_password_hash
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)
_db_url = os.environ.get('DATABASE_URL', '')
if not _db_url:
    raise RuntimeError("DATABASE_URL environment variable is not set.")
DATABASE_URL     = _db_url.replace('postgres://', 'postgresql://', 1)
app.secret_key   = os.environ.get('SECRET_KEY', 'dev-secret-change-in-prod')
app.permanent_session_lifetime = timedelta(days=30)
ADMIN_PASSWORD   = os.environ.get('ADMIN_PASSWORD', '')

# ── Grade definitions ──────────────────────────────────────────────────────────
# Points use a progressive scale that meaningfully rewards harder sends.
GRADE_COLORS = [
    {'key': 'orange', 'label': 'Orange', 'grade': 'VB',    'points':  1, 'hex': '#FF7700', 'text': '#fff'},
    {'key': 'yellow', 'label': 'Yellow', 'grade': 'V0–V2', 'points':  3, 'hex': '#FFD700', 'text': '#fff'},
    {'key': 'green',  'label': 'Green',  'grade': 'V1–V3', 'points':  6, 'hex': '#27AE60', 'text': '#fff'},
    {'key': 'blue',   'label': 'Blue',   'grade': 'V2–V4', 'points': 10, 'hex': '#2980B9', 'text': '#fff'},
    {'key': 'purple', 'label': 'Purple', 'grade': 'V3–V5', 'points': 15, 'hex': '#8E44AD', 'text': '#fff'},
    {'key': 'red',    'label': 'Red',    'grade': 'V4–V6', 'points': 21, 'hex': '#E74C3C', 'text': '#fff'},
    {'key': 'black',  'label': 'Black',  'grade': 'V5–V7', 'points': 30, 'hex': '#909090', 'text': '#fff'},
    {'key': 'white',  'label': 'White',  'grade': 'V7+',   'points': 50, 'hex': '#E8E8E8', 'text': '#fff'},
]

GRADE_MAP    = {g['key']: g for g in GRADE_COLORS}

# Sub-grades for the white tag (V7+), progressive points starting at 50.
WHITE_SUBGRADES = [
    {'key': 'v7',  'label': 'V7',  'points':  50},
    {'key': 'v8',  'label': 'V8',  'points':  70},
    {'key': 'v9',  'label': 'V9',  'points':  95},
    {'key': 'v10', 'label': 'V10', 'points': 125},
    {'key': 'v11', 'label': 'V11', 'points': 160},
    {'key': 'v12', 'label': 'V12', 'points': 200},
    {'key': 'v13', 'label': 'V13', 'points': 245},
    {'key': 'v14', 'label': 'V14', 'points': 295},
    {'key': 'v15', 'label': 'V15', 'points': 350},
    {'key': 'v16', 'label': 'V16', 'points': 410},
    {'key': 'v17', 'label': 'V17', 'points': 475},
    {'key': 'v18', 'label': 'V18', 'points': 545},
]
WHITE_SUBGRADE_MAP    = {g['key']: g for g in WHITE_SUBGRADES}
VALID_WHITE_SUBGRADES = set(WHITE_SUBGRADE_MAP.keys())

VALID_TYPES  = {'overhang', 'neutral', 'slab'}
VALID_STYLES = {'dyno', 'static'}
VALID_HOLDS  = {'jugs', 'crimps', 'slopers', 'dual-tex'}
VALID_SORTS  = {'total_points', 'total_climbs'} | set(GRADE_MAP.keys())


# ── Database ───────────────────────────────────────────────────────────────────

_db_pool = pg_pool.ThreadedConnectionPool(
    minconn=1, maxconn=10, dsn=DATABASE_URL
)


def get_db():
    if 'db_conn' not in g:
        g.db_conn = _db_pool.getconn()
        g.db_conn.cursor_factory = RealDictCursor
    return g.db_conn


def parse_hold_list(raw_holds):
    try:
        values = json.loads(raw_holds) if raw_holds else []
    except (TypeError, ValueError):
        return []
    return [value for value in values if value in VALID_HOLDS]


def get_last_session_preferences():
    prefs = flask_session.get('last_session_by_climber')
    return prefs if isinstance(prefs, dict) else {}


def get_last_session_for_climber(user_id):
    raw_value = get_last_session_preferences().get(str(user_id))
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


def set_last_session_for_climber(user_id, session_id):
    prefs = dict(get_last_session_preferences())
    key = str(user_id)
    if session_id is None:
        prefs.pop(key, None)
    else:
        prefs[key] = int(session_id)
    flask_session['last_session_by_climber'] = prefs


def get_session_options(include_session_id=None):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM sessions WHERE ended_at IS NULL ORDER BY started_at DESC')
    rows = list(cur.fetchall())
    if include_session_id is not None and not any(row['id'] == include_session_id for row in rows):
        cur.execute('SELECT * FROM sessions WHERE id = %s', (include_session_id,))
        extra_row = cur.fetchone()
        if extra_row is not None:
            rows.insert(0, extra_row)
    cur.close()
    return rows


def can_manage_climb(user_id):
    return is_admin() or flask_session.get('climber_id') == user_id


def build_climb_form_data(*, climb=None, user_id=None, selected_session_id=None, alt_user_id=''):
    if climb is not None:
        flashed = bool(climb['flashed'])
        attempts = str(climb['attempts']) if climb['attempts'] else ''
        return {
            'user_id': str(user_id if user_id is not None else climb['user_id']),
            'alt_user_id': alt_user_id,
            'session_id': '' if climb['session_id'] is None else str(climb['session_id']),
            'gym_id': str(climb['gym_id']) if climb.get('gym_id') else '',
            'grade_color': climb['grade_color'],
            'sub_grade': climb.get('sub_grade', '') or '',
            'climb_type': climb['climb_type'],
            'style': climb['style'],
            'holds': parse_hold_list(climb['holds']),
            'flashed': flashed,
            'attempts': attempts,
        }

    return {
        'user_id': '' if user_id is None else str(user_id),
        'alt_user_id': alt_user_id,
        'session_id': '' if selected_session_id is None else str(selected_session_id),
        'gym_id': '',
        'grade_color': '',
        'sub_grade': '',
        'climb_type': '',
        'style': '',
        'holds': [],
        'flashed': False,
        'attempts': '',
    }


@app.teardown_appcontext
def close_db(exc):
    conn = g.pop('db_conn', None)
    if conn is not None:
        if exc is not None:
            conn.rollback()
        _db_pool.putconn(conn)


def init_db():
    conn = get_db()
    cur  = conn.cursor()

    # ── Core tables ───────────────────────────────────────────────────────────
    cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id         SERIAL PRIMARY KEY,
            name       TEXT   NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS gyms (
            id           SERIAL PRIMARY KEY,
            name         TEXT    NOT NULL UNIQUE,
            notes        TEXT    NOT NULL DEFAULT '',
            is_approved  BOOLEAN NOT NULL DEFAULT FALSE,
            requested_by INTEGER REFERENCES users(id),
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS gym_grades (
            id            SERIAL PRIMARY KEY,
            gym_id        INTEGER NOT NULL REFERENCES gyms(id) ON DELETE CASCADE,
            key           TEXT    NOT NULL,
            label         TEXT    NOT NULL,
            grade_range   TEXT    NOT NULL DEFAULT '',
            points        INTEGER NOT NULL,
            hex           TEXT    NOT NULL DEFAULT '#888888',
            text_color    TEXT    NOT NULL DEFAULT '#fff',
            sort_order    INTEGER NOT NULL DEFAULT 0,
            has_subgrades BOOLEAN NOT NULL DEFAULT FALSE,
            UNIQUE (gym_id, key)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS sessions (
            id         SERIAL PRIMARY KEY,
            gym_name   TEXT   NOT NULL DEFAULT 'Unknown Gym',
            notes      TEXT   NOT NULL DEFAULT '',
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ended_at   TIMESTAMP
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS climbs (
            id          SERIAL PRIMARY KEY,
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
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS competitions (
            id         SERIAL PRIMARY KEY,
            name       TEXT   NOT NULL,
            month      DATE   NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS comp_sends (
            id             SERIAL PRIMARY KEY,
            user_id        INTEGER NOT NULL REFERENCES users(id),
            competition_id INTEGER NOT NULL REFERENCES competitions(id),
            problem_number INTEGER NOT NULL CHECK (problem_number BETWEEN 1 AND 30),
            date           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (user_id, competition_id, problem_number)
        )
    ''')
    conn.commit()

    # ── Seed Alien Bloc gym ───────────────────────────────────────────────────
    cur.execute("""
        INSERT INTO gyms (name, notes, is_approved)
        VALUES ('Alien Bloc', '', TRUE)
        ON CONFLICT (name) DO NOTHING
    """)
    conn.commit()
    cur.execute("SELECT id FROM gyms WHERE name = 'Alien Bloc'")
    alien_bloc_id = cur.fetchone()['id']

    for sort_order, g in enumerate(GRADE_COLORS):
        cur.execute("""
            INSERT INTO gym_grades (gym_id, key, label, grade_range, points, hex, text_color, sort_order, has_subgrades)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (gym_id, key) DO NOTHING
        """, (alien_bloc_id, g['key'], g['label'], g['grade'], g['points'],
              g['hex'], g['text'], sort_order, g['key'] == 'white'))
    conn.commit()

    # ── Migrations: climbs ────────────────────────────────────────────────────
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'climbs'")
    cols = [row['column_name'] for row in cur.fetchall()]
    if 'session_id' not in cols:
        cur.execute('ALTER TABLE climbs ADD COLUMN session_id INTEGER REFERENCES sessions(id)')
    if 'flashed' not in cols:
        cur.execute('ALTER TABLE climbs ADD COLUMN flashed INTEGER NOT NULL DEFAULT 0')
    if 'attempts' not in cols:
        cur.execute('ALTER TABLE climbs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 1')
    if 'sub_grade' not in cols:
        cur.execute('ALTER TABLE climbs ADD COLUMN sub_grade TEXT')
    if 'gym_id' not in cols:
        cur.execute('ALTER TABLE climbs ADD COLUMN gym_id INTEGER REFERENCES gyms(id)')
        conn.commit()
        cur.execute('UPDATE climbs SET gym_id = %s WHERE gym_id IS NULL', (alien_bloc_id,))
    conn.commit()

    # ── Migrations: users ─────────────────────────────────────────────────────
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'users'")
    user_cols = [row['column_name'] for row in cur.fetchall()]
    if 'password_hash' not in user_cols:
        cur.execute('ALTER TABLE users ADD COLUMN password_hash TEXT')
    if 'main_gym_id' not in user_cols:
        cur.execute('ALTER TABLE users ADD COLUMN main_gym_id INTEGER REFERENCES gyms(id)')
    conn.commit()

    # ── Migrations: sessions ──────────────────────────────────────────────────
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'sessions'")
    session_cols = [row['column_name'] for row in cur.fetchall()]
    if 'gym_id' not in session_cols:
        cur.execute('ALTER TABLE sessions ADD COLUMN gym_id INTEGER REFERENCES gyms(id)')
        conn.commit()
        cur.execute('UPDATE sessions SET gym_id = %s WHERE gym_id IS NULL', (alien_bloc_id,))
        # Keep gym_name in sync with the gym for existing rows
        cur.execute("UPDATE sessions SET gym_name = 'Alien Bloc' WHERE gym_id = %s", (alien_bloc_id,))
    conn.commit()

    cur.close()


def get_approved_gyms():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('SELECT * FROM gyms WHERE is_approved = TRUE ORDER BY name')
    result = cur.fetchall()
    cur.close()
    return result


def get_gym_grades(gym_id):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('SELECT * FROM gym_grades WHERE gym_id = %s ORDER BY sort_order', (gym_id,))
    result = cur.fetchall()
    cur.close()
    return result


def get_alien_bloc_id():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT id FROM gyms WHERE name = 'Alien Bloc' LIMIT 1")
    row = cur.fetchone()
    cur.close()
    return row['id'] if row else None


# ── Achievements definition ───────────────────────────────────────────────────

ACHIEVEMENTS = [
    {'id': 'first_climb',  'name': 'First Step',       'emoji': '🧗', 'desc': 'Log your very first climb'},
    {'id': 'first_flash',  'name': 'Flash!',            'emoji': '⚡', 'desc': 'Flash a climb'},
    {'id': 'climbs_10',    'name': '10 Sends',          'emoji': '🔟', 'desc': 'Log 10 climbs'},
    {'id': 'climbs_50',    'name': '50 Sends',          'emoji': '🏅', 'desc': 'Log 50 climbs'},
    {'id': 'climbs_100',   'name': 'Century Club',      'emoji': '💯', 'desc': 'Log 100 climbs'},
    {'id': 'pts_100',      'name': 'Point Collector',   'emoji': '⭐', 'desc': 'Earn 100 points total'},
    {'id': 'pts_500',      'name': 'Point Hoarder',     'emoji': '🌟', 'desc': 'Earn 500 points total'},
    {'id': 'pts_1000',     'name': 'Point Legend',      'emoji': '👑', 'desc': 'Earn 1000 points total'},
    {'id': 'first_orange', 'name': 'Orange Sent',       'emoji': '🟠', 'desc': 'Send your first orange'},
    {'id': 'first_yellow', 'name': 'Yellow Sent',       'emoji': '🟡', 'desc': 'Send your first yellow'},
    {'id': 'first_green',  'name': 'Going Green',       'emoji': '🟢', 'desc': 'Send your first green'},
    {'id': 'first_blue',   'name': 'Blue Streak',       'emoji': '🔵', 'desc': 'Send your first blue'},
    {'id': 'first_purple', 'name': 'Pretty Purple',     'emoji': '🟣', 'desc': 'Send your first purple'},
    {'id': 'first_red',    'name': 'Red Alert',         'emoji': '🔴', 'desc': 'Send your first red'},
    {'id': 'first_black',  'name': 'Into the Dark',     'emoji': '⚫', 'desc': 'Send your first black'},
    {'id': 'first_white',  'name': 'Top of the World',  'emoji': '🤍', 'desc': 'Send your first white'},
]


# ── Admin helpers ────────────────────────────────────────────────────────────

def is_admin():
    return flask_session.get('is_admin', False)


@app.context_processor
def inject_globals():
    climber_id   = flask_session.get('climber_id')
    climber_name = flask_session.get('climber_name')
    main_gym_id  = flask_session.get('main_gym_id')
    current_climber = {'id': climber_id, 'name': climber_name, 'main_gym_id': main_gym_id} if climber_id else None
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute('SELECT id, name FROM users ORDER BY name')
        all_users = cur.fetchall()
        cur.execute('SELECT * FROM gyms WHERE is_approved = TRUE ORDER BY name')
        approved_gyms = cur.fetchall()
        cur.close()
    except Exception:
        all_users = []
        approved_gyms = []
    return {'is_admin': is_admin(), 'current_climber': current_climber, 'all_users': all_users, 'approved_gyms': approved_gyms}


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
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('SELECT * FROM users ORDER BY name')
    users = cur.fetchall()
    cur.execute('''
        SELECT c.*, u.name AS climber, s.gym_name AS session_gym
        FROM   climbs c
        JOIN   users  u ON c.user_id = u.id
        LEFT   JOIN sessions s ON c.session_id = s.id
        ORDER  BY c.date DESC
        LIMIT  20
    ''')
    recent_climbs = [dict(r, item_type='climb') for r in cur.fetchall()]
    cur.execute('''
        SELECT cs.*, u.name AS climber, comp.name AS comp_name, comp.id AS comp_id
        FROM   comp_sends  cs
        JOIN   users       u    ON cs.user_id       = u.id
        JOIN   competitions comp ON cs.competition_id = comp.id
        ORDER  BY cs.date DESC
        LIMIT  20
    ''')
    recent_comp = [dict(r, item_type='comp') for r in cur.fetchall()]
    cur.close()
    recent = sorted(recent_climbs + recent_comp, key=lambda x: x['date'], reverse=True)[:20]
    return render_template('index.html', users=users, recent=recent, grade_map=GRADE_MAP)


@app.route('/add_user', methods=['POST'])
def add_user():
    if not is_admin():
        return redirect(url_for('admin_login'))
    name = request.form.get('name', '').strip()
    if 1 <= len(name) <= 50:
        conn = get_db()
        cur  = conn.cursor()
        try:
            cur.execute('INSERT INTO users (name) VALUES (%s)', (name,))
            conn.commit()
        except psycopg2.IntegrityError:
            conn.rollback()
        cur.close()
    return redirect(url_for('index'))


@app.route('/log', methods=['GET', 'POST'])
def log_climb():
    if not flask_session.get('climber_id'):
        return redirect(url_for('register', next=url_for('log_climb')))

    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM users ORDER BY name')
    users = cur.fetchall()
    cur.close()

    # Gym grades for the grade picker (used by both GET and POST error re-render)
    _all_gyms = get_approved_gyms()
    gym_grades_map = {gym['id']: get_gym_grades(gym['id']) for gym in _all_gyms}
    default_gym_id = flask_session.get('main_gym_id') or get_alien_bloc_id()

    current_climber_id = flask_session.get('climber_id')
    selected_session_id = request.args.get('session_id', type=int)
    if selected_session_id is None and current_climber_id:
        selected_session_id = get_last_session_for_climber(current_climber_id)

    active_sessions = get_session_options(include_session_id=selected_session_id)
    valid_session_ids = {row['id'] for row in active_sessions}
    if selected_session_id not in valid_session_ids:
        selected_session_id = None

    field_errors = {}
    form_data = build_climb_form_data(
        user_id=current_climber_id,
        selected_session_id=selected_session_id,
    )
    success_state = flask_session.pop('log_success', None)

    if request.method == 'POST':
        error = None
        form_data = {
            'user_id': '',
            'alt_user_id': '',
            'session_id': request.form.get('session_id', '').strip(),
            'gym_id': request.form.get('gym_id', '').strip(),
            'grade_color': request.form.get('grade_color', ''),
            'sub_grade': request.form.get('sub_grade', '').strip(),
            'climb_type': request.form.get('climb_type', '').strip(),
            'style': request.form.get('style', '').strip(),
            'holds': [hold for hold in request.form.getlist('holds') if hold],
            'flashed': request.form.get('flashed') == '1',
            'attempts': request.form.get('attempts', '').strip(),
        }

        # Always log as the current signed-in climber.
        user_id = current_climber_id

        grade = form_data['grade_color']
        sub_grade = form_data['sub_grade']
        ctype = form_data['climb_type']
        style = form_data['style']
        holds = form_data['holds']
        flashed = 1 if form_data['flashed'] else 0
        attempts_raw = form_data['attempts']
        attempts = 0
        if flashed:
            attempts = 1  # a flash is always 1 attempt
        elif attempts_raw:
            try:
                attempts = max(1, int(attempts_raw))
            except (ValueError, TypeError):
                field_errors['attempts'] = 'Attempts must be a whole number.'

        # Optional session
        raw_sid = form_data['session_id']
        try:
            session_id = int(raw_sid) if raw_sid else None
        except ValueError:
            session_id = None
            field_errors['session_id'] = 'Please choose a valid session.'

        if session_id is not None and session_id not in valid_session_ids:
            field_errors['session_id'] = 'Please choose an active session.'

        # Load grades for the submitted gym
        try:
            gym_id = int(form_data['gym_id']) if form_data['gym_id'] else None
        except (ValueError, TypeError):
            gym_id = None
        db_grades = get_gym_grades(gym_id) if gym_id else []
        db_grade_map = {g['key']: g for g in db_grades}
        subgrades_key = next((g['key'] for g in db_grades if g['has_subgrades']), None)

        if not db_grade_map:
            field_errors['grade_color'] = 'Please select a valid gym.'
        elif grade not in db_grade_map:
            field_errors['grade_color'] = 'Please select a grade.'
        elif grade == subgrades_key and sub_grade not in VALID_WHITE_SUBGRADES:
            field_errors['sub_grade'] = 'Please select a V-grade for the top grade.'
        elif ctype and ctype not in VALID_TYPES:
            field_errors['climb_type'] = 'Please select a climb type.'
        elif style and style not in VALID_STYLES:
            field_errors['style'] = 'Please select a style.'
        elif not all(h in VALID_HOLDS for h in holds):
            field_errors['holds'] = 'Invalid hold type submitted.'

        if field_errors:
            error = next(iter(field_errors.values()))
            return render_template('log.html', users=users,
                                   white_subgrades=WHITE_SUBGRADES,
                                   gym_grades_map=gym_grades_map,
                                   default_gym_id=default_gym_id,
                                   active_sessions=active_sessions, error=error,
                                   field_errors=field_errors, form_data=form_data,
                                   success_state=success_state)

        if grade == subgrades_key and sub_grade in WHITE_SUBGRADE_MAP:
            points = WHITE_SUBGRADE_MAP[sub_grade]['points']
        else:
            points = db_grade_map[grade]['points']
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            'INSERT INTO climbs (user_id, session_id, gym_id, grade_color, sub_grade, climb_type, style, holds, points, flashed, attempts) '
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
            (user_id, session_id, gym_id, grade, sub_grade or None, ctype, style, json.dumps(holds), points, flashed, attempts)
        )
        conn.commit()
        cur.close()
        set_last_session_for_climber(user_id, session_id)
        flask_session['log_success'] = {
            'message': 'Climb logged successfully.',
            'session_id': session_id,
            'user_id': user_id,
        }
        if session_id is not None:
            return redirect(url_for('log_climb', session_id=session_id))
        return redirect(url_for('log_climb'))

    return render_template('log.html', users=users,
                           white_subgrades=WHITE_SUBGRADES,
                           gym_grades_map=gym_grades_map,
                           default_gym_id=default_gym_id,
                           active_sessions=active_sessions,
                           selected_session_id=selected_session_id,
                           form_data=form_data,
                           field_errors=field_errors,
                           success_state=success_state)


@app.route('/climb/<int:climb_id>/edit', methods=['GET', 'POST'])
def edit_climb(climb_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        SELECT c.*, u.name AS climber_name
        FROM   climbs c
        JOIN   users  u ON u.id = c.user_id
        WHERE  c.id = %s
    ''', (climb_id,))
    climb = cur.fetchone()
    cur.close()

    if climb is None:
        flash('That climb could not be found.', 'error')
        return redirect(url_for('index'))

    if not can_manage_climb(climb['user_id']):
        flash('You can only edit your own climbs.', 'error')
        return redirect(url_for('climber_profile', user_id=climb['user_id']))

    session_options = get_session_options(include_session_id=climb['session_id'])
    valid_session_ids = {row['id'] for row in session_options}
    field_errors = {}
    form_data = build_climb_form_data(climb=climb)

    if request.method == 'POST':
        form_data = {
            'user_id': str(climb['user_id']),
            'alt_user_id': '',
            'session_id': request.form.get('session_id', '').strip(),
            'grade_color': request.form.get('grade_color', ''),
            'sub_grade': request.form.get('sub_grade', '').strip(),
            'climb_type': request.form.get('climb_type', '').strip(),
            'style': request.form.get('style', '').strip(),
            'holds': [hold for hold in request.form.getlist('holds') if hold],
            'flashed': request.form.get('flashed') == '1',
            'attempts': request.form.get('attempts', '').strip(),
        }

        grade = form_data['grade_color']
        sub_grade = form_data['sub_grade']
        ctype = form_data['climb_type']
        style = form_data['style']
        holds = form_data['holds']
        flashed = 1 if form_data['flashed'] else 0
        attempts_raw = form_data['attempts']
        attempts = 0

        if flashed:
            attempts = 1
        elif attempts_raw:
            try:
                attempts = max(1, int(attempts_raw))
            except (ValueError, TypeError):
                field_errors['attempts'] = 'Attempts must be a whole number.'

        raw_sid = form_data['session_id']
        try:
            session_id = int(raw_sid) if raw_sid else None
        except ValueError:
            session_id = None
            field_errors['session_id'] = 'Please choose a valid session.'

        if session_id is not None and session_id not in valid_session_ids:
            field_errors['session_id'] = 'Please choose an available session.'

        if grade not in GRADE_MAP:
            field_errors['grade_color'] = 'Please select a grade.'
        elif grade == 'white' and sub_grade not in VALID_WHITE_SUBGRADES:
            field_errors['sub_grade'] = 'Please select a V-grade for the white tag.'
        elif ctype and ctype not in VALID_TYPES:
            field_errors['climb_type'] = 'Please select a climb type.'
        elif style and style not in VALID_STYLES:
            field_errors['style'] = 'Please select a style.'
        elif not all(h in VALID_HOLDS for h in holds):
            field_errors['holds'] = 'Invalid hold type submitted.'

        if field_errors:
            error = next(iter(field_errors.values()))
            return render_template(
                'edit_climb.html',
                climb=climb,
                climber_name=climb['climber_name'],
                grades=GRADE_COLORS,
                white_subgrades=WHITE_SUBGRADES,
                session_options=session_options,
                field_errors=field_errors,
                error=error,
                form_data=form_data,
            )

        if grade == 'white' and sub_grade in WHITE_SUBGRADE_MAP:
            points = WHITE_SUBGRADE_MAP[sub_grade]['points']
        else:
            points = GRADE_MAP[grade]['points']
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            '''
            UPDATE climbs
            SET    session_id = %s,
                   grade_color = %s,
                   sub_grade = %s,
                   climb_type = %s,
                   style = %s,
                   holds = %s,
                   points = %s,
                   flashed = %s,
                   attempts = %s
            WHERE  id = %s
            ''',
            (session_id, grade, sub_grade or None, ctype, style, json.dumps(holds), points, flashed, attempts, climb_id)
        )
        conn.commit()
        cur.close()

        if flask_session.get('climber_id') == climb['user_id']:
            set_last_session_for_climber(climb['user_id'], session_id)

        flash('Climb updated.', 'success')
        return redirect(url_for('climber_profile', user_id=climb['user_id']))

    return render_template(
        'edit_climb.html',
        climb=climb,
        climber_name=climb['climber_name'],
        grades=GRADE_COLORS,
        white_subgrades=WHITE_SUBGRADES,
        session_options=session_options,
        field_errors=field_errors,
        error=None,
        form_data=form_data,
    )


@app.route('/leaderboard')
def leaderboard():
    sort_by = request.args.get('sort', 'total_points')
    if sort_by not in VALID_SORTS:
        sort_by = 'total_points'

    conn = get_db()
    cur  = conn.cursor()
    cur.execute('SELECT * FROM users')
    users = cur.fetchall()
    cur.execute('SELECT * FROM climbs')
    all_climbs = cur.fetchall()
    cur.close()

    stats = {
        u['id']: {
            'id':           u['id'],
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
    # Legacy route — just redirect to the regular login page
    return redirect(url_for('login', next=request.args.get('next', '')))


@app.route('/admin/logout', methods=['POST'])
def admin_logout():
    flask_session.pop('is_admin', None)
    flask_session.pop('climber_id', None)
    flask_session.pop('climber_name', None)
    flask_session.pop('main_gym_id', None)
    return redirect(url_for('index'))


# ── Climber identity routes ──────────────────────────────────────────────────

@app.route('/login', methods=['GET', 'POST'])
def login():
    if flask_session.get('climber_id') or flask_session.get('is_admin'):
        return redirect(url_for('index'))
    error = None
    next_url = request.args.get('next', '') or url_for('index')
    name_val = ''
    if request.method == 'POST':
        name     = request.form.get('name', '').strip()
        password = request.form.get('password', '')
        next_url = request.form.get('next', '') or url_for('index')
        name_val = name
        # Admin account — never stored in the users table
        if name.lower() == 'admin':
            if ADMIN_PASSWORD and password == ADMIN_PASSWORD:
                flask_session.permanent = True
                flask_session['is_admin'] = True
                return redirect(next_url)
            error = 'Invalid name or password.'
            return render_template('login.html', error=error, next=next_url, name_val=name_val)
        conn = get_db()
        cur  = conn.cursor()
        cur.execute('SELECT id, name, password_hash, main_gym_id FROM users WHERE name ILIKE %s', (name,))
        user = cur.fetchone()
        cur.close()
        if user is None or not user['password_hash'] or not check_password_hash(user['password_hash'], password):
            error = 'Invalid name or password.'
        else:
            flask_session.permanent = True
            flask_session['climber_id']   = user['id']
            flask_session['climber_name'] = user['name']
            flask_session['main_gym_id']  = user['main_gym_id']
            return redirect(next_url)
    return render_template('login.html', error=error, next=next_url, name_val=name_val)


@app.route('/register', methods=['GET', 'POST'])
def register():
    if flask_session.get('climber_id'):
        return redirect(url_for('index'))
    error = None
    next_url = request.args.get('next', '') or url_for('index')
    form_data = {'name': ''}
    if request.method == 'POST':
        name     = request.form.get('name', '').strip()
        password = request.form.get('password', '')
        confirm  = request.form.get('confirm', '')
        next_url = request.form.get('next', '') or url_for('index')
        form_data = {'name': name}
        if name.lower() == 'admin':
            error = 'That name is not available.'
        elif not (1 <= len(name) <= 50):
            error = 'Name must be between 1 and 50 characters.'
        elif len(password) < 6:
            error = 'Password must be at least 6 characters.'
        elif password != confirm:
            error = 'Passwords do not match.'
        else:
            conn = get_db()
            cur  = conn.cursor()
            cur.execute('SELECT id, password_hash FROM users WHERE name ILIKE %s', (name,))
            existing = cur.fetchone()
            if existing and existing['password_hash']:
                error = 'That name is already taken.'
                cur.close()
            elif existing and not existing['password_hash']:
                # Claim an account created by admin (no password yet)
                cur.execute('UPDATE users SET password_hash = %s WHERE id = %s',
                            (generate_password_hash(password), existing['id']))
                conn.commit()
                cur.close()
                flask_session.permanent = True
                flask_session['climber_id']   = existing['id']
                flask_session['climber_name'] = name
                flask_session['main_gym_id']  = existing.get('main_gym_id')
                return redirect(next_url)
            else:
                try:
                    cur.execute(
                        'INSERT INTO users (name, password_hash) VALUES (%s, %s) RETURNING id',
                        (name, generate_password_hash(password))
                    )
                    new_id = cur.fetchone()['id']
                    conn.commit()
                    cur.close()
                    flask_session.permanent = True
                    flask_session['climber_id']   = new_id
                    flask_session['climber_name'] = name
                    flask_session['main_gym_id']  = None
                    return redirect(next_url)
                except psycopg2.IntegrityError:
                    conn.rollback()
                    cur.close()
                    error = 'That name is already taken.'
    return render_template('register.html', error=error, form_data=form_data, next=next_url)


@app.route('/logout', methods=['POST'])
def logout():
    flask_session.pop('climber_id',   None)
    flask_session.pop('climber_name', None)
    flask_session.pop('main_gym_id',  None)
    flask_session.pop('is_admin',     None)
    next_url = request.form.get('next', '') or url_for('index')
    return redirect(next_url)


@app.route('/admin/rename_user/<int:user_id>', methods=['POST'])
def rename_user(user_id):
    if not is_admin():
        return redirect(url_for('admin_login'))
    new_name = request.form.get('name', '').strip()
    if 1 <= len(new_name) <= 50:
        conn = get_db()
        cur  = conn.cursor()
        try:
            cur.execute('UPDATE users SET name = %s WHERE id = %s', (new_name, user_id))
            conn.commit()
        except psycopg2.IntegrityError:
            conn.rollback()
        cur.close()
    return redirect(url_for('climber_profile', user_id=user_id))


@app.route('/admin/delete_user/<int:user_id>', methods=['POST'])
def delete_user(user_id):
    if not is_admin():
        return redirect(url_for('admin_login'))
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('DELETE FROM comp_sends WHERE user_id = %s', (user_id,))
    cur.execute('DELETE FROM climbs     WHERE user_id = %s', (user_id,))
    cur.execute('DELETE FROM users      WHERE id = %s',      (user_id,))
    conn.commit()
    cur.close()
    return redirect(url_for('leaderboard'))


@app.route('/delete_climb/<int:climb_id>', methods=['POST'])
def delete_climb(climb_id):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('SELECT user_id, session_id FROM climbs WHERE id = %s', (climb_id,))
    row  = cur.fetchone()
    if row is None:
        cur.close()
        return redirect(url_for('index'))
    if not can_manage_climb(row['user_id']):
        cur.close()
        flash('You can only delete your own climbs.', 'error')
        return redirect(url_for('climber_profile', user_id=row['user_id']))
    cur.execute('DELETE FROM climbs WHERE id = %s', (climb_id,))
    conn.commit()
    cur.close()
    next_url = request.form.get('next', '')
    if next_url:
        return redirect(next_url)
    if row['session_id']:
        return redirect(url_for('session_detail', session_id=row['session_id']))
    return redirect(url_for('index'))


@app.route('/bulk_delete_climbs', methods=['POST'])
def bulk_delete_climbs():
    if not is_admin():
        return redirect(url_for('admin_login'))
    raw_ids  = request.form.getlist('climb_ids')
    next_url = request.form.get('next', url_for('index'))
    ids = []
    for v in raw_ids:
        try:
            ids.append(int(v))
        except (ValueError, TypeError):
            pass
    if ids:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute('DELETE FROM climbs WHERE id = ANY(%s)', (ids,))
        conn.commit()
        cur.close()
    return redirect(next_url)


@app.route('/delete_session/<int:session_id>', methods=['POST'])
def delete_session(session_id):
    if not is_admin():
        return redirect(url_for('admin_login'))
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('DELETE FROM climbs   WHERE session_id = %s', (session_id,))
    cur.execute('DELETE FROM sessions WHERE id = %s',         (session_id,))
    conn.commit()
    cur.close()
    return redirect(url_for('sessions'))


# ── Session routes ───────────────────────────────────────────────────────────

@app.route('/sessions')
def sessions():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('''
        SELECT s.*,
               COUNT(c.id)                AS climb_count,
               COALESCE(SUM(c.points), 0) AS total_points
        FROM   sessions s
        LEFT   JOIN climbs c ON c.session_id = s.id
        GROUP  BY s.id
        ORDER  BY s.started_at DESC
    ''')
    rows = cur.fetchall()
    cur.close()
    return render_template('sessions.html', sessions=rows, approved_gyms=get_approved_gyms())


@app.route('/start_session', methods=['POST'])
def start_session():
    gym_id_raw = request.form.get('gym_id', '').strip()
    notes      = request.form.get('notes', '').strip()[:500]
    conn = get_db()
    cur  = conn.cursor()
    try:
        gym_id = int(gym_id_raw)
    except (ValueError, TypeError):
        gym_id = None
    gym_name = 'Unknown Gym'
    if gym_id:
        cur.execute('SELECT name FROM gyms WHERE id = %s AND is_approved = TRUE', (gym_id,))
        gym_row = cur.fetchone()
        if gym_row:
            gym_name = gym_row['name']
        else:
            gym_id   = None
            gym_name = 'Unknown Gym'
    cur.execute(
        'INSERT INTO sessions (gym_id, gym_name, notes) VALUES (%s, %s, %s) RETURNING id',
        (gym_id, gym_name, notes)
    )
    sid = cur.fetchone()['id']
    conn.commit()
    cur.close()
    return redirect(url_for('session_detail', session_id=sid))


@app.route('/session/<int:session_id>')
def session_detail(session_id):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('SELECT * FROM sessions WHERE id = %s', (session_id,))
    session = cur.fetchone()
    if session is None:
        cur.close()
        return redirect(url_for('sessions'))
    cur.execute('''
        SELECT c.*, u.name AS climber
        FROM   climbs c
        JOIN   users  u ON c.user_id = u.id
        WHERE  c.session_id = %s
        ORDER  BY c.date ASC
    ''', (session_id,))
    climbs = cur.fetchall()
    cur.close()
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
    cur  = conn.cursor()
    cur.execute(
        'UPDATE sessions SET ended_at = CURRENT_TIMESTAMP WHERE id = %s AND ended_at IS NULL',
        (session_id,)
    )
    conn.commit()
    cur.close()
    return redirect(url_for('session_detail', session_id=session_id))


# ── Stats route ───────────────────────────────────────────────────────────────

@app.route('/records')
def records():
    conn = get_db()
    cur  = conn.cursor()
    grade_order = [g['key'] for g in GRADE_COLORS]  # ascending difficulty

    # Most climbs in a single calendar day
    cur.execute('''
        SELECT u.name, DATE(c.date) AS day, COUNT(*) AS cnt
        FROM   climbs c
        JOIN   users  u ON c.user_id = u.id
        GROUP  BY c.user_id, u.name, DATE(c.date)
        ORDER  BY cnt DESC
        LIMIT  1
    ''')
    record_day = cur.fetchone()

    # Best session by points
    cur.execute('''
        SELECT u.name, s.gym_name, s.started_at,
               SUM(c.points) AS pts, COUNT(c.id) AS cnt
        FROM   climbs   c
        JOIN   users    u ON c.user_id    = u.id
        JOIN   sessions s ON c.session_id = s.id
        WHERE  c.session_id IS NOT NULL
        GROUP  BY c.user_id, c.session_id, u.name, s.gym_name, s.started_at
        ORDER  BY pts DESC
        LIMIT  1
    ''')
    best_session_pts = cur.fetchone()

    # Highest grade sent ever
    cur.execute('''
        SELECT u.name, c.grade_color, c.flashed, c.date
        FROM   climbs c
        JOIN   users  u ON c.user_id = u.id
    ''')
    all_sends = cur.fetchall()

    highest_grade_sent = None
    for row in all_sends:
        grade_key = row['grade_color']
        if grade_key not in GRADE_MAP:
            continue
        if highest_grade_sent is None:
            highest_grade_sent = row
            continue
        current_rank = grade_order.index(grade_key)
        best_rank = grade_order.index(highest_grade_sent['grade_color'])
        if current_rank > best_rank:
            highest_grade_sent = row
        elif current_rank == best_rank and row['date'] < highest_grade_sent['date']:
            highest_grade_sent = row

    if highest_grade_sent:
        highest_grade_sent = {
            'name': highest_grade_sent['name'],
            'grade': GRADE_MAP[highest_grade_sent['grade_color']],
            'date': highest_grade_sent['date'],
        }

    # Highest grade flashed
    highest_grade_flashed = None
    for row in all_sends:
        if not row['flashed']:
            continue
        grade_key = row['grade_color']
        if grade_key not in GRADE_MAP:
            continue
        if highest_grade_flashed is None:
            highest_grade_flashed = row
            continue
        current_rank = grade_order.index(grade_key)
        best_rank = grade_order.index(highest_grade_flashed['grade_color'])
        if current_rank > best_rank:
            highest_grade_flashed = row
        elif current_rank == best_rank and row['date'] < highest_grade_flashed['date']:
            highest_grade_flashed = row

    if highest_grade_flashed:
        highest_grade_flashed = {
            'name': highest_grade_flashed['name'],
            'grade': GRADE_MAP[highest_grade_flashed['grade_color']],
            'date': highest_grade_flashed['date'],
        }

    # Most sends per grade (for each grade: who has the most sends and how many)
    cur.execute('SELECT * FROM users ORDER BY name')
    users = cur.fetchall()
    cur.execute('SELECT user_id, grade_color, COUNT(*) AS cnt FROM climbs GROUP BY user_id, grade_color')
    grade_climb_rows = cur.fetchall()
    cur.close()

    user_map = {u['id']: u['name'] for u in users}

    # group counts by grade
    grade_user_counts = {}
    for row in grade_climb_rows:
        col = row['grade_color']
        if col not in GRADE_MAP:
            continue
        grade_user_counts.setdefault(col, []).append((row['user_id'], row['cnt']))

    most_sends_per_grade = []
    for g in reversed(GRADE_COLORS):
        entries = grade_user_counts.get(g['key'], [])
        if not entries:
            most_sends_per_grade.append({
                'grade': GRADE_MAP[g['key']],
                'name': None,
                'count': 0,
            })
        else:
            best_uid, best_cnt = max(entries, key=lambda x: x[1])
            most_sends_per_grade.append({
                'grade': GRADE_MAP[g['key']],
                'name': user_map.get(best_uid),
                'count': best_cnt,
            })

    return render_template('records.html',
                           record_day=record_day,
                           best_session_pts=best_session_pts,
                           highest_grade_sent=highest_grade_sent,
                           highest_grade_flashed=highest_grade_flashed,
                           most_sends_per_grade=most_sends_per_grade)


@app.route('/stats')
def stats():
    conn = get_db()

    cur = conn.cursor()
    cur.execute('SELECT * FROM users ORDER BY name')
    users = cur.fetchall()
    cur.execute('SELECT * FROM climbs')
    all_climbs = cur.fetchall()
    cur.close()

    grade_order = [g['key'] for g in GRADE_COLORS]  # ascending difficulty
    user_stats  = {
        u['id']: {'id': u['id'], 'name': u['name'], 'total_climbs': 0,
                  'total_points': 0, 'best_grade': None,
                  'flashes': 0, 'grades_sent': set()}
        for u in users
    }
    for c in all_climbs:
        uid, color = c['user_id'], c['grade_color']
        if uid not in user_stats or color not in GRADE_MAP:
            continue
        s = user_stats[uid]
        s['total_climbs'] += 1
        s['total_points'] += c['points']
        s['grades_sent'].add(color)
        if c['flashed']:
            s['flashes'] += 1
        cur = s['best_grade']
        if cur is None or grade_order.index(color) > grade_order.index(cur['key']):
            s['best_grade'] = GRADE_MAP[color]

    def _unlocked(aid, tc, tp, tf, gs):
        if aid == 'first_climb':  return tc >= 1
        if aid == 'first_flash':  return tf >= 1
        if aid == 'climbs_10':    return tc >= 10
        if aid == 'climbs_50':    return tc >= 50
        if aid == 'climbs_100':   return tc >= 100
        if aid == 'pts_100':      return tp >= 100
        if aid == 'pts_500':      return tp >= 500
        if aid == 'pts_1000':     return tp >= 1000
        if aid.startswith('first_'): return aid[len('first_'):] in gs
        return False

    for s in user_stats.values():
        s['flash_pct'] = round(100 * s['flashes'] / s['total_climbs']) if s['total_climbs'] else 0
        s['achievement_count'] = sum(
            1 for a in ACHIEVEMENTS
            if _unlocked(a['id'], s['total_climbs'], s['total_points'], s['flashes'], s['grades_sent'])
        )

    user_rows = sorted(user_stats.values(),
                       key=lambda x: x['total_points'], reverse=True)

    return render_template('stats.html',
                           user_rows=user_rows,
                           total_achievements=len(ACHIEVEMENTS))


# ── Achievements ─────────────────────────────────────────────────────────────

@app.route('/achievements')
def achievements():
    climber_id = flask_session.get('climber_id')
    if not climber_id:
        return redirect(url_for('index'))
    return redirect(url_for('climber_achievements', user_id=climber_id))


@app.route('/climber/<int:user_id>/achievements')
def climber_achievements(user_id):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('SELECT * FROM users WHERE id = %s', (user_id,))
    user = cur.fetchone()
    if user is None:
        cur.close()
        return redirect(url_for('index'))

    cur.execute('SELECT * FROM climbs WHERE user_id = %s ORDER BY date ASC', (user_id,))
    climbs = cur.fetchall()
    cur.close()

    total_climbs  = len(climbs)
    total_points  = sum(c['points'] for c in climbs)
    total_flashes = sum(1 for c in climbs if c['flashed'])
    grades_sent   = {c['grade_color'] for c in climbs}

    grade_send_ids = {f"first_{g['key']}" for g in GRADE_COLORS}
    grade_send_order = {f"first_{g['key']}": idx for idx, g in enumerate(GRADE_COLORS)}

    def progress_for(aid):
        if aid == 'first_climb':
            return min(total_climbs, 1), 1
        if aid == 'first_flash':
            return min(total_flashes, 1), 1
        if aid == 'climbs_10':
            return min(total_climbs, 10), 10
        if aid == 'climbs_50':
            return min(total_climbs, 50), 50
        if aid == 'climbs_100':
            return min(total_climbs, 100), 100
        if aid == 'pts_100':
            return min(total_points, 100), 100
        if aid == 'pts_500':
            return min(total_points, 500), 500
        if aid == 'pts_1000':
            return min(total_points, 1000), 1000
        if aid in grade_send_ids:
            grade_key = aid[len('first_'):]
            return (1 if grade_key in grades_sent else 0), 1
        return 0, 1

    result = []
    for idx, a in enumerate(ACHIEVEMENTS):
        current, target = progress_for(a['id'])
        is_grade_send = a['id'] in grade_send_ids
        unlocked = current >= target
        progress_pct = int((current / target) * 100) if target else 0
        result.append(dict(
            a,
            unlocked=unlocked,
            progress_current=current,
            progress_target=target,
            progress_pct=progress_pct,
            is_grade_send=is_grade_send,
            original_index=idx,
        ))

    locked_list = [a for a in result if not a['unlocked']]
    unlocked_list = [a for a in result if a['unlocked']]

    # Locked sorting: closest completion first for non-grade milestones.
    # Grade-send milestones are excluded from proximity sorting and stay in grade order.
    locked_non_grade = [a for a in locked_list if not a['is_grade_send']]
    locked_grade = [a for a in locked_list if a['is_grade_send']]

    locked_non_grade.sort(
        key=lambda a: (
            -(a['progress_current'] / a['progress_target']) if a['progress_target'] else 0,
            a['progress_target'] - a['progress_current'],
            a['original_index'],
        )
    )
    locked_grade.sort(key=lambda a: grade_send_order.get(a['id'], 999))
    locked_list = locked_non_grade + locked_grade

    unlocked_count = len(unlocked_list)

    return render_template('achievements.html',
                           user=user,
                           locked_achievements=locked_list,
                           unlocked_achievements=unlocked_list,
                           unlocked_count=unlocked_count,
                           total_count=len(ACHIEVEMENTS),
                           total_climbs=total_climbs,
                           total_points=total_points,
                           total_flashes=total_flashes)


# ── Climber profile ──────────────────────────────────────────────────────────

@app.route('/climber/<int:user_id>')
def climber_profile(user_id):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('SELECT * FROM users WHERE id = %s', (user_id,))
    user = cur.fetchone()
    if user is None:
        cur.close()
        return redirect(url_for('leaderboard'))

    cur.execute('''
        SELECT c.*, s.gym_name AS session_gym
        FROM   climbs c
        LEFT   JOIN sessions s ON c.session_id = s.id
        WHERE  c.user_id = %s
        ORDER  BY c.date ASC
    ''', (user_id,))
    climbs = cur.fetchall()

    # Sessions this climber attended (with their personal totals)
    cur.execute('''
        SELECT s.id, s.gym_name, s.started_at, s.ended_at,
               COUNT(c.id)                AS climb_count,
               COALESCE(SUM(c.points), 0) AS session_points
        FROM   sessions s
        JOIN   climbs   c ON c.session_id = s.id AND c.user_id = %s
        GROUP  BY s.id
        ORDER  BY s.started_at DESC
    ''', (user_id,))
    session_rows = cur.fetchall()

    # Competition results for this climber
    cur.execute('''
        SELECT comp.id, comp.name, comp.month,
               MAX(cs.problem_number)             AS best_problem,
               COUNT(cs.id)                       AS total_sends,
               COALESCE(SUM(cs.problem_number), 0) AS total_points,
               ARRAY_AGG(cs.problem_number ORDER BY cs.problem_number DESC) AS problems
        FROM   comp_sends   cs
        JOIN   competitions comp ON cs.competition_id = comp.id
        WHERE  cs.user_id = %s
        GROUP  BY comp.id, comp.name, comp.month
        ORDER  BY comp.month DESC
    ''', (user_id,))
    comp_results = cur.fetchall()

    cur.execute('''
        SELECT cs.*, comp.name AS comp_name, comp.id AS comp_id
        FROM   comp_sends   cs
        JOIN   competitions comp ON cs.competition_id = comp.id
        WHERE  cs.user_id = %s
        ORDER  BY cs.date DESC
        LIMIT  20
    ''', (user_id,))
    recent_comp_rows = cur.fetchall()
    cur.close()

    total_climbs  = len(climbs)
    total_points  = sum(c['points'] for c in climbs)
    total_flashes = sum(1 for c in climbs if c['flashed'])
    flash_pct     = round(100 * total_flashes / total_climbs) if total_climbs else 0

    grade_order = [g['key'] for g in GRADE_COLORS]
    best_grade  = None
    for c in climbs:
        if c['grade_color'] in GRADE_MAP:
            _prev = best_grade
            if _prev is None or grade_order.index(c['grade_color']) > grade_order.index(_prev['key']):
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

    recent_climbs = [dict(c, item_type='climb') for c in climbs]
    recent_comp   = [dict(r, item_type='comp') for r in recent_comp_rows]

    recent_activity = sorted(recent_climbs + recent_comp, key=lambda x: x['date'], reverse=True)[:15]

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
                           recent_climbs=recent_activity,
                           session_rows=session_rows,
                           comp_results=comp_results,
                           can_edit_climbs=can_manage_climb(user_id),
                           grades=GRADE_COLORS,
                           grade_map=GRADE_MAP)


# ── Competition routes ───────────────────────────────────────────────────────

@app.route('/competitions')
def competitions():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('''
        SELECT c.*,
               COUNT(DISTINCT cs.user_id)       AS participant_count,
               COALESCE(MAX(cs.problem_number), 0) AS top_problem,
               COUNT(cs.id)                     AS total_sends
        FROM   competitions c
        LEFT   JOIN comp_sends cs ON cs.competition_id = c.id
        GROUP  BY c.id
        ORDER  BY c.month DESC
    ''')
    comps = cur.fetchall()
    cur.close()
    return render_template('competitions.html', comps=comps)


@app.route('/competitions/<int:comp_id>')
def competition_detail(comp_id):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('SELECT * FROM competitions WHERE id = %s', (comp_id,))
    comp = cur.fetchone()
    if comp is None:
        cur.close()
        return redirect(url_for('competitions'))
    cur.execute('''
        SELECT u.id, u.name,
               MAX(cs.problem_number)              AS best_problem,
               COUNT(cs.id)                        AS total_sends,
               COALESCE(SUM(cs.problem_number), 0) AS total_points,
               ARRAY_AGG(cs.problem_number ORDER BY cs.problem_number DESC) AS problems,
               ARRAY_AGG(cs.id            ORDER BY cs.problem_number DESC) AS send_ids
        FROM   comp_sends cs
        JOIN   users      u ON cs.user_id = u.id
        WHERE  cs.competition_id = %s
        GROUP  BY u.id, u.name
        ORDER  BY best_problem DESC, total_points DESC
    ''', (comp_id,))
    results = cur.fetchall()
    cur.close()
    return render_template('competition.html', comp=comp, results=results)


@app.route('/competitions/log', methods=['GET', 'POST'])
def log_comp_send():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('SELECT * FROM users ORDER BY name')
    users = cur.fetchall()
    cur.execute('SELECT * FROM competitions ORDER BY month DESC')
    comps = cur.fetchall()
    cur.close()

    if request.method == 'POST':
        error = None
        try:
            user_id = int(request.form.get('user_id', ''))
        except (ValueError, TypeError):
            user_id = None
            error   = 'Please select a climber.'
        try:
            comp_id = int(request.form.get('competition_id', ''))
        except (ValueError, TypeError):
            comp_id = None
            if not error:
                error = 'Please select a competition.'
        try:
            problem_number = int(request.form.get('problem_number', ''))
            if not (1 <= problem_number <= 30):
                raise ValueError
        except (ValueError, TypeError):
            problem_number = None
            if not error:
                error = 'Please select a problem (1\u201330).'

        if not error:
            conn2 = get_db()
            cur2  = conn2.cursor()
            try:
                cur2.execute(
                    'INSERT INTO comp_sends (user_id, competition_id, problem_number) '
                    'VALUES (%s, %s, %s)',
                    (user_id, comp_id, problem_number)
                )
                conn2.commit()
            except psycopg2.IntegrityError:
                conn2.rollback()
                error = f'Problem #{problem_number} is already logged for this climber in this competition.'
            finally:
                cur2.close()

        if error:
            conn3 = get_db()
            cur3  = conn3.cursor()
            cur3.execute('SELECT * FROM users ORDER BY name')
            users = cur3.fetchall()
            cur3.execute('SELECT * FROM competitions ORDER BY month DESC')
            comps = cur3.fetchall()
            cur3.close()
            return render_template('log_comp.html', users=users, comps=comps, error=error,
                                   selected_comp_id=request.form.get('competition_id', type=int))
        return redirect(url_for('log_comp_send', comp_id=comp_id))

    selected_comp_id = request.args.get('comp_id', type=int)
    return render_template('log_comp.html', users=users, comps=comps,
                           selected_comp_id=selected_comp_id)


@app.route('/competitions/create', methods=['POST'])
def create_competition():
    if not is_admin():
        return redirect(url_for('admin_login', next=url_for('competitions')))
    name      = request.form.get('name', '').strip()[:100]
    month_str = request.form.get('month', '').strip()
    if not name or not month_str:
        return redirect(url_for('competitions'))
    try:
        month = datetime.strptime(month_str, '%Y-%m').date()
    except ValueError:
        return redirect(url_for('competitions'))
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('INSERT INTO competitions (name, month) VALUES (%s, %s)', (name, month))
    conn.commit()
    cur.close()
    return redirect(url_for('competitions'))


@app.route('/competitions/delete_send/<int:send_id>', methods=['POST'])
def delete_comp_send(send_id):
    if not is_admin():
        return redirect(url_for('admin_login'))
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('SELECT competition_id FROM comp_sends WHERE id = %s', (send_id,))
    row = cur.fetchone()
    cur.execute('DELETE FROM comp_sends WHERE id = %s', (send_id,))
    conn.commit()
    cur.close()
    if row:
        return redirect(url_for('competition_detail', comp_id=row['competition_id']))
    return redirect(url_for('competitions'))


# ── Gym routes ────────────────────────────────────────────────────────────────

@app.route('/gyms')
def gyms():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('SELECT * FROM gyms WHERE is_approved = TRUE ORDER BY name')
    approved = cur.fetchall()
    pending_request = None
    climber_id = flask_session.get('climber_id')
    if climber_id:
        cur.execute(
            'SELECT * FROM gyms WHERE is_approved = FALSE AND requested_by = %s LIMIT 1',
            (climber_id,)
        )
        pending_request = cur.fetchone()
    cur.close()
    return render_template('gyms.html', approved_gyms=approved, pending_request=pending_request)


@app.route('/gym/request', methods=['POST'])
def request_gym():
    climber_id = flask_session.get('climber_id')
    if not climber_id:
        return redirect(url_for('login', next=url_for('gyms')))
    name  = request.form.get('name', '').strip()[:100]
    notes = request.form.get('notes', '').strip()[:1000]
    if not name:
        flash('Please provide a gym name.', 'error')
        return redirect(url_for('gyms'))
    conn = get_db()
    cur  = conn.cursor()
    try:
        cur.execute(
            'INSERT INTO gyms (name, notes, is_approved, requested_by) VALUES (%s, %s, FALSE, %s)',
            (name, notes, climber_id)
        )
        conn.commit()
        flash(f'"{name}" has been submitted for admin approval.', 'success')
    except psycopg2.IntegrityError:
        conn.rollback()
        flash('A gym with that name already exists.', 'error')
    cur.close()
    return redirect(url_for('gyms'))


@app.route('/admin/gyms')
def admin_gyms():
    if not is_admin():
        return redirect(url_for('login'))
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('''
        SELECT g.*, u.name AS requester_name
        FROM   gyms  g
        LEFT   JOIN users u ON u.id = g.requested_by
        WHERE  g.is_approved = FALSE
        ORDER  BY g.created_at ASC
    ''')
    pending = cur.fetchall()
    cur.execute('''
        SELECT g.*, u.name AS requester_name
        FROM   gyms  g
        LEFT   JOIN users u ON u.id = g.requested_by
        WHERE  g.is_approved = TRUE
        ORDER  BY g.name ASC
    ''')
    approved = cur.fetchall()
    # Load grades for each approved gym
    cur.execute('SELECT * FROM gym_grades ORDER BY gym_id, sort_order')
    all_grades = cur.fetchall()
    cur.close()
    grades_by_gym = {}
    for gr in all_grades:
        grades_by_gym.setdefault(gr['gym_id'], []).append(gr)
    return render_template('admin_gyms.html',
                           pending=pending,
                           approved=approved,
                           grades_by_gym=grades_by_gym)


@app.route('/admin/gym/<int:gym_id>/approve', methods=['POST'])
def approve_gym(gym_id):
    if not is_admin():
        return redirect(url_for('login'))
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('UPDATE gyms SET is_approved = TRUE WHERE id = %s', (gym_id,))
    conn.commit()
    # Seed default V-scale grades if none exist yet
    cur.execute('SELECT COUNT(*) AS cnt FROM gym_grades WHERE gym_id = %s', (gym_id,))
    if cur.fetchone()['cnt'] == 0:
        default_grades = [
            ('vb',  'Orange', 'VB',    1,   '#FF7700', '#fff', 0, False),
            ('v0',  'Yellow', 'V0',    2,   '#FFD700', '#fff', 1, False),
            ('v1',  'Green',  'V1',    3,   '#27AE60', '#fff', 2, False),
            ('v2',  'Blue',   'V2',    6,   '#2980B9', '#fff', 3, False),
            ('v3',  'Purple', 'V3',   10,   '#8E44AD', '#fff', 4, False),
            ('v4',  'Red',    'V4',   15,   '#E74C3C', '#fff', 5, False),
            ('v5',  'Black',  'V5',   21,   '#909090', '#fff', 6, False),
            ('v6',  'Pink',   'V6',   30,   '#E91E9E', '#fff', 7, False),
            ('v7p', 'White',  'V7+',  50,   '#E8E8E8', '#fff', 8, True),
        ]
        for key, label, grade_range, points, hex_c, text_c, sort_order, has_sub in default_grades:
            cur.execute('''
                INSERT INTO gym_grades (gym_id, key, label, grade_range, points, hex, text_color, sort_order, has_subgrades)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (gym_id, key) DO NOTHING
            ''', (gym_id, key, label, grade_range, points, hex_c, text_c, sort_order, has_sub))
        conn.commit()
    cur.close()
    flash('Gym approved.', 'success')
    return redirect(url_for('admin_gyms'))


@app.route('/admin/gym/<int:gym_id>/reject', methods=['POST'])
def reject_gym(gym_id):
    if not is_admin():
        return redirect(url_for('login'))
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('DELETE FROM gyms WHERE id = %s AND is_approved = FALSE', (gym_id,))
    conn.commit()
    cur.close()
    flash('Gym request rejected.', 'success')
    return redirect(url_for('admin_gyms'))


@app.route('/admin/gym/<int:gym_id>/delete', methods=['POST'])
def delete_gym(gym_id):
    if not is_admin():
        return redirect(url_for('login'))
    conn = get_db()
    cur  = conn.cursor()
    # Null out FK references before deleting so existing climbs/sessions are preserved
    cur.execute('UPDATE climbs   SET gym_id = NULL WHERE gym_id = %s', (gym_id,))
    cur.execute('UPDATE sessions SET gym_id = NULL WHERE gym_id = %s', (gym_id,))
    cur.execute('UPDATE users    SET main_gym_id = NULL WHERE main_gym_id = %s', (gym_id,))
    cur.execute('DELETE FROM gyms WHERE id = %s', (gym_id,))
    conn.commit()
    cur.close()
    flash('Gym deleted.', 'success')
    return redirect(url_for('admin_gyms'))


@app.route('/admin/gym/<int:gym_id>/grades', methods=['GET', 'POST'])
def edit_gym_grades(gym_id):
    if not is_admin():
        return redirect(url_for('login'))
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('SELECT * FROM gyms WHERE id = %s', (gym_id,))
    gym = cur.fetchone()
    if gym is None:
        cur.close()
        return redirect(url_for('admin_gyms'))

    if request.method == 'POST':
        action = request.form.get('action', '')

        if action == 'add':
            key         = request.form.get('key', '').strip().lower().replace(' ', '_')[:30]
            label       = request.form.get('label', '').strip()[:50]
            grade_range = request.form.get('grade_range', '').strip()[:30]
            hex_c       = request.form.get('hex', '#888888').strip()[:7]
            text_c      = request.form.get('text_color', '#ffffff').strip()[:7]
            has_sub     = request.form.get('has_subgrades') == '1'
            try:
                points = int(request.form.get('points', 0))
                points = max(1, points)
            except (ValueError, TypeError):
                points = 1
            if key and label:
                cur.execute('SELECT COALESCE(MAX(sort_order),0)+1 AS next FROM gym_grades WHERE gym_id=%s', (gym_id,))
                sort_order = cur.fetchone()['next']
                try:
                    cur.execute('''
                        INSERT INTO gym_grades (gym_id, key, label, grade_range, points, hex, text_color, sort_order, has_subgrades)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ''', (gym_id, key, label, grade_range, points, hex_c, text_c, sort_order, has_sub))
                    conn.commit()
                    flash('Grade added.', 'success')
                except psycopg2.IntegrityError:
                    conn.rollback()
                    flash(f'A grade with key "{key}" already exists for this gym.', 'error')

        elif action == 'update':
            grade_id    = request.form.get('grade_id', type=int)
            label       = request.form.get('label', '').strip()[:50]
            grade_range = request.form.get('grade_range', '').strip()[:30]
            hex_c       = request.form.get('hex', '#888888').strip()[:7]
            text_c      = request.form.get('text_color', '#ffffff').strip()[:7]
            has_sub     = request.form.get('has_subgrades') == '1'
            try:
                points = int(request.form.get('points', 0))
                points = max(1, points)
            except (ValueError, TypeError):
                points = 1
            if grade_id and label:
                cur.execute('''
                    UPDATE gym_grades
                    SET label=%s, grade_range=%s, points=%s, hex=%s, text_color=%s, has_subgrades=%s
                    WHERE id=%s AND gym_id=%s
                ''', (label, grade_range, points, hex_c, text_c, has_sub, grade_id, gym_id))
                conn.commit()
                flash('Grade updated.', 'success')

        elif action == 'delete':
            grade_id = request.form.get('grade_id', type=int)
            if grade_id:
                cur.execute('DELETE FROM gym_grades WHERE id=%s AND gym_id=%s', (grade_id, gym_id))
                conn.commit()
                flash('Grade deleted.', 'success')

        cur.close()
        return redirect(url_for('edit_gym_grades', gym_id=gym_id))

    cur.execute('SELECT * FROM gym_grades WHERE gym_id=%s ORDER BY sort_order', (gym_id,))
    grades = cur.fetchall()
    cur.close()
    return render_template('edit_gym_grades.html', gym=gym, grades=grades)


# ── Account settings ──────────────────────────────────────────────────────────

@app.route('/account/main-gym', methods=['POST'])
def set_main_gym():
    if not flask_session.get('climber_id'):
        return redirect(url_for('login'))
    raw = request.form.get('main_gym_id', '').strip()
    gym_id = int(raw) if raw.isdigit() else None
    if gym_id is not None:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute('SELECT id FROM gyms WHERE id = %s AND is_approved = TRUE', (gym_id,))
        if not cur.fetchone():
            gym_id = None
        cur.close()
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('UPDATE users SET main_gym_id = %s WHERE id = %s',
                (gym_id, flask_session['climber_id']))
    conn.commit()
    cur.close()
    flask_session['main_gym_id'] = gym_id
    flash('Main gym updated.', 'success')
    next_url = request.form.get('next', '') or url_for('index')
    return redirect(next_url)


# ── Startup ───────────────────────────────────────────────────────────────────

with app.app_context():
    init_db()

if __name__ == '__main__':
    app.run(debug=True)
