"""Skool Tracker — Community analytics + link tracking."""

import os
import csv
import io
import hashlib
from datetime import datetime, timedelta
from functools import wraps
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
load_dotenv()

from flask import (
    Flask, request, redirect, render_template, session,
    flash, url_for, jsonify, Response
)
from werkzeug.security import generate_password_hash, check_password_hash
from models import init_db, get_db

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-change-me")

SKOOL_URL = os.environ.get("SKOOL_INVITE_URL", "https://www.skool.com/stepizy-sois-enfin-visible-5378/about")
TZ = ZoneInfo("Europe/Paris")

DEFAULT_CHANNELS = [
    "youtube", "linkedin", "instagram", "tiktok", "x", "facebook",
    "newsletter", "google-ads", "reddit", "substack", "direct", "autre"
]

init_db(app)

# Create or update admin on every startup
with app.app_context():
    db = get_db()
    admin_pw_raw = os.environ.get("ADMIN_PASSWORD", "admin")
    print(f"[STARTUP] ADMIN_PASSWORD from env: '{admin_pw_raw[:3]}***' (length: {len(admin_pw_raw)})")
    admin_pw = generate_password_hash(admin_pw_raw)
    existing = db.execute("SELECT id FROM users WHERE username = 'admin'").fetchone()
    if existing:
        db.execute("UPDATE users SET password_hash = ? WHERE username = 'admin'", (admin_pw,))
        print("[STARTUP] Admin password UPDATED")
    else:
        db.execute(
            "INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
            ("admin", admin_pw, "admin", datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"))
        )
        print("[STARTUP] Admin user CREATED")
    db.commit()
    # Verify it works
    verify = db.execute("SELECT password_hash FROM users WHERE username = 'admin'").fetchone()
    print(f"[STARTUP] Verify login test: {check_password_hash(verify['password_hash'], admin_pw_raw)}")


# ==================== AUTH ====================

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        if session.get("role") != "admin":
            flash("Accès réservé aux administrateurs.", "error")
            return redirect(url_for("dashboard"))
        return f(*args, **kwargs)
    return decorated


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
        if user and check_password_hash(user["password_hash"], password):
            session["user_id"] = user["id"]
            session["username"] = user["username"]
            session["role"] = user["role"]
            return redirect(url_for("dashboard"))
        print(f"[LOGIN FAILED] username='{username}', user_found={user is not None}")
        flash("Identifiants incorrects", "error")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ==================== DASHBOARD ====================

@app.route("/")
@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("dashboard.html")


@app.route("/api/overview")
@login_required
def api_overview():
    db = get_db()
    now = datetime.now(TZ)
    this_month = now.strftime("%Y-%m")
    last_month = (now.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")

    total = db.execute("SELECT COUNT(*) as c FROM members WHERE status = 'active'").fetchone()["c"]
    total_ever = db.execute("SELECT COUNT(*) as c FROM members WHERE email NOT LIKE '__no_email_%'").fetchone()["c"]
    churned = db.execute("SELECT COUNT(*) as c FROM members WHERE status = 'churned'").fetchone()["c"]
    this_month_new = db.execute(
        "SELECT COUNT(*) as c FROM members WHERE joined_at LIKE ?", (this_month + "%",)
    ).fetchone()["c"]
    last_month_new = db.execute(
        "SELECT COUNT(*) as c FROM members WHERE joined_at LIKE ?", (last_month + "%",)
    ).fetchone()["c"]

    # MRR — only count active members who have actually paid (LTV > 0)
    mrr = db.execute("SELECT SUM(price) as s FROM members WHERE status = 'active' AND price > 0 AND ltv > 0 AND recurring_interval = 'month'").fetchone()["s"] or 0
    annual_monthly = db.execute("SELECT SUM(price) as s FROM members WHERE status = 'active' AND price > 0 AND ltv > 0 AND recurring_interval = 'year'").fetchone()["s"] or 0
    mrr += annual_monthly / 12

    avg_ltv = db.execute("SELECT AVG(ltv) as a FROM members WHERE ltv > 0").fetchone()["a"] or 0
    total_ltv = db.execute("SELECT SUM(ltv) as s FROM members").fetchone()["s"] or 0

    referral_count = db.execute("SELECT COUNT(*) as c FROM members WHERE invited_by != ''").fetchone()["c"]
    referral_pct = round(referral_count / total * 100, 1) if total > 0 else 0

    # Growth rate
    growth = 0
    if last_month_new > 0:
        growth = round((this_month_new - last_month_new) / last_month_new * 100, 1)

    # Members by month for sparkline
    monthly = db.execute("""
        SELECT SUBSTR(joined_at, 1, 7) as month, COUNT(*) as cnt
        FROM members GROUP BY month ORDER BY month
    """).fetchall()

    return jsonify({
        "total_members": total,
        "total_ever": total_ever,
        "churned": churned,
        "this_month_new": this_month_new,
        "last_month_new": last_month_new,
        "growth_pct": growth,
        "mrr": round(mrr, 2),
        "avg_ltv": round(avg_ltv, 2),
        "total_ltv": round(total_ltv, 2),
        "referral_count": referral_count,
        "referral_pct": referral_pct,
        "monthly": [{"month": r["month"], "count": r["cnt"]} for r in monthly]
    })


# ==================== GROWTH ====================

@app.route("/growth")
@login_required
def growth_page():
    return render_template("growth.html")


@app.route("/api/growth")
@login_required
def api_growth():
    db = get_db()
    group_by = request.args.get("group", "day")

    if group_by == "week":
        sql = """SELECT SUBSTR(joined_at, 1, 4) || '-W' ||
                 CAST((JULIANDAY(joined_at) - JULIANDAY(SUBSTR(joined_at, 1, 4) || '-01-01')) / 7 + 1 AS INTEGER) as period,
                 COUNT(*) as cnt FROM members GROUP BY period ORDER BY period"""
    elif group_by == "month":
        sql = "SELECT SUBSTR(joined_at, 1, 7) as period, COUNT(*) as cnt FROM members GROUP BY period ORDER BY period"
    else:
        sql = "SELECT DATE(joined_at) as period, COUNT(*) as cnt FROM members GROUP BY period ORDER BY period"

    rows = db.execute(sql).fetchall()
    data = [{"period": r["period"], "count": r["cnt"]} for r in rows]

    # Cumulative
    cumulative = []
    total = 0
    for d in data:
        total += d["count"]
        cumulative.append({"period": d["period"], "total": total})

    return jsonify({"signups": data, "cumulative": cumulative})


# ==================== REVENUE ====================

@app.route("/revenue")
@login_required
def revenue_page():
    return render_template("revenue.html")


@app.route("/api/revenue")
@login_required
def api_revenue():
    db = get_db()

    # Price distribution
    prices = db.execute("""
        SELECT price, COUNT(*) as cnt FROM members
        WHERE price > 0 GROUP BY price ORDER BY cnt DESC
    """).fetchall()

    # Tier distribution
    tiers = db.execute("""
        SELECT tier, COUNT(*) as cnt FROM members
        WHERE tier != '' GROUP BY tier ORDER BY cnt DESC
    """).fetchall()

    # LTV distribution buckets
    ltv_buckets = db.execute("""
        SELECT
            CASE
                WHEN ltv = 0 THEN '0 (gratuit)'
                WHEN ltv <= 30 THEN '1-30'
                WHEN ltv <= 60 THEN '31-60'
                WHEN ltv <= 100 THEN '61-100'
                WHEN ltv <= 200 THEN '101-200'
                ELSE '200+'
            END as bucket,
            COUNT(*) as cnt
        FROM members GROUP BY bucket ORDER BY MIN(ltv)
    """).fetchall()

    # Revenue per month (new members * price)
    monthly_rev = db.execute("""
        SELECT SUBSTR(joined_at, 1, 7) as month, SUM(price) as revenue, COUNT(*) as members
        FROM members WHERE price > 0
        GROUP BY month ORDER BY month
    """).fetchall()

    # Free vs paid
    free = db.execute("SELECT COUNT(*) as c FROM members WHERE price = 0 OR price IS NULL OR price = ''").fetchone()["c"]
    paid = db.execute("SELECT COUNT(*) as c FROM members WHERE price > 0").fetchone()["c"]

    return jsonify({
        "prices": [{"price": r["price"], "count": r["cnt"]} for r in prices],
        "tiers": [{"tier": r["tier"], "count": r["cnt"]} for r in tiers],
        "ltv_buckets": [{"bucket": r["bucket"], "count": r["cnt"]} for r in ltv_buckets],
        "monthly_revenue": [{"month": r["month"], "revenue": r["revenue"], "members": r["members"]} for r in monthly_rev],
        "free": free,
        "paid": paid
    })


# ==================== REFERRALS ====================

@app.route("/referrals")
@login_required
def referrals_page():
    return render_template("referrals.html")


@app.route("/api/referrals")
@login_required
def api_referrals():
    db = get_db()

    top_referrers = db.execute("""
        SELECT invited_by, COUNT(*) as cnt FROM members
        WHERE invited_by != '' GROUP BY invited_by ORDER BY cnt DESC LIMIT 20
    """).fetchall()

    organic_count = db.execute("SELECT COUNT(*) as c FROM members WHERE invited_by = ''").fetchone()["c"]
    referral_count = db.execute("SELECT COUNT(*) as c FROM members WHERE invited_by != ''").fetchone()["c"]

    # Referrals over time
    monthly = db.execute("""
        SELECT SUBSTR(joined_at, 1, 7) as month,
               SUM(CASE WHEN invited_by != '' THEN 1 ELSE 0 END) as referrals,
               SUM(CASE WHEN invited_by = '' THEN 1 ELSE 0 END) as organic
        FROM members GROUP BY month ORDER BY month
    """).fetchall()

    return jsonify({
        "top_referrers": [{"name": r["invited_by"], "count": r["cnt"]} for r in top_referrers],
        "organic": organic_count,
        "referral": referral_count,
        "monthly": [{"month": r["month"], "referrals": r["referrals"], "organic": r["organic"]} for r in monthly]
    })


# ==================== CHURN ====================

@app.route("/churn")
@login_required
def churn_page():
    return render_template("churn.html")


@app.route("/api/churn")
@login_required
def api_churn():
    db = get_db()

    active = db.execute("SELECT COUNT(*) as c FROM members WHERE status = 'active' AND email NOT LIKE '__no_email_%'").fetchone()["c"]
    churned = db.execute("SELECT COUNT(*) as c FROM members WHERE status = 'churned'").fetchone()["c"]
    total_ever = active + churned
    churn_pct = round(churned / total_ever * 100, 1) if total_ever > 0 else 0
    retention_pct = round(100 - churn_pct, 1)

    # Lost revenue (sum of last known price of churned members)
    lost_mrr = db.execute("SELECT SUM(ltv) as s FROM members WHERE status = 'churned'").fetchone()["s"] or 0

    # Churned members list
    churned_list = db.execute("""
        SELECT first_name, last_name, email, joined_at, churned_at, ltv, invited_by
        FROM members WHERE status = 'churned'
        ORDER BY churned_at DESC LIMIT 100
    """).fetchall()

    # Churn by month (when they churned)
    monthly_churn = db.execute("""
        SELECT SUBSTR(churned_at, 1, 7) as month, COUNT(*) as cnt
        FROM members WHERE status = 'churned' AND churned_at != ''
        GROUP BY month ORDER BY month
    """).fetchall()

    # Average lifetime (days between join and churn)
    avg_lifetime = db.execute("""
        SELECT AVG(JULIANDAY(churned_at) - JULIANDAY(joined_at)) as avg_days
        FROM members WHERE status = 'churned' AND churned_at != '' AND joined_at != ''
    """).fetchone()["avg_days"] or 0

    return jsonify({
        "active": active,
        "churned": churned,
        "total_ever": total_ever,
        "churn_pct": churn_pct,
        "retention_pct": retention_pct,
        "lost_ltv": round(lost_mrr, 2),
        "avg_lifetime_days": round(avg_lifetime, 0),
        "monthly_churn": [{"month": r["month"], "count": r["cnt"]} for r in monthly_churn],
        "churned_list": [{
            "name": f"{r['first_name']} {r['last_name']}",
            "email": r["email"], "joined_at": r["joined_at"],
            "churned_at": r["churned_at"], "ltv": r["ltv"],
            "invited_by": r["invited_by"]
        } for r in churned_list]
    })


# ==================== FORECAST ====================

@app.route("/forecast")
@login_required
def forecast_page():
    return render_template("forecast.html")


@app.route("/api/forecast")
@login_required
def api_forecast():
    db = get_db()
    months_ahead = int(request.args.get("months", 6))

    monthly = db.execute("""
        SELECT SUBSTR(joined_at, 1, 7) as month, COUNT(*) as cnt,
               SUM(price) as revenue
        FROM members GROUP BY month ORDER BY month
    """).fetchall()

    if len(monthly) < 2:
        return jsonify({"error": "Pas assez de données pour une prévision"})

    # Simple linear regression on monthly signups
    counts = [r["cnt"] for r in monthly]
    revenues = [r["revenue"] or 0 for r in monthly]
    n = len(counts)

    # Trend calculation
    x_mean = (n - 1) / 2
    y_mean = sum(counts) / n
    slope = sum((i - x_mean) * (counts[i] - y_mean) for i in range(n)) / max(sum((i - x_mean)**2 for i in range(n)), 1)

    # Revenue trend
    rev_mean = sum(revenues) / n
    rev_slope = sum((i - x_mean) * (revenues[i] - rev_mean) for i in range(n)) / max(sum((i - x_mean)**2 for i in range(n)), 1)

    # Generate forecasts
    last_month = monthly[-1]["month"]
    last_date = datetime.strptime(last_month + "-01", "%Y-%m-%d")
    forecasts = []
    cumulative = sum(counts)

    for i in range(1, months_ahead + 1):
        future = last_date + timedelta(days=32 * i)
        future_month = future.strftime("%Y-%m")
        predicted_signups = max(0, round(y_mean + slope * (n - 1 + i)))
        predicted_revenue = max(0, round(rev_mean + rev_slope * (n - 1 + i)))
        cumulative += predicted_signups
        forecasts.append({
            "month": future_month,
            "signups": predicted_signups,
            "revenue": predicted_revenue,
            "cumulative": cumulative
        })

    return jsonify({
        "historical": [{"month": r["month"], "signups": r["cnt"], "revenue": r["revenue"] or 0} for r in monthly],
        "forecast": forecasts,
        "trend": {"signup_slope": round(slope, 1), "revenue_slope": round(rev_slope, 1)}
    })


# ==================== MEMBERS LIST ====================

@app.route("/members")
@login_required
def members_page():
    return render_template("members.html")


@app.route("/api/members")
@login_required
def api_members():
    db = get_db()
    search = request.args.get("search", "")
    sort = request.args.get("sort", "joined_at")
    order = request.args.get("order", "DESC")

    if sort not in ("joined_at", "ltv", "first_name", "price"):
        sort = "joined_at"
    if order not in ("ASC", "DESC"):
        order = "DESC"

    if search:
        rows = db.execute(f"""
            SELECT * FROM members
            WHERE first_name LIKE ? OR last_name LIKE ? OR email LIKE ?
            ORDER BY {sort} {order} LIMIT 500
        """, (f"%{search}%", f"%{search}%", f"%{search}%")).fetchall()
    else:
        rows = db.execute(f"SELECT * FROM members ORDER BY {sort} {order} LIMIT 500").fetchall()

    return jsonify([{
        "id": r["id"], "first_name": r["first_name"], "last_name": r["last_name"],
        "email": r["email"], "invited_by": r["invited_by"],
        "joined_at": r["joined_at"], "price": r["price"],
        "tier": r["tier"], "ltv": r["ltv"],
        "status": r["status"] if "status" in r.keys() else "active",
        "churned_at": r["churned_at"] if "churned_at" in r.keys() else ""
    } for r in rows])


# ==================== CSV IMPORT ====================

@app.route("/upload", methods=["GET", "POST"])
@login_required
def upload_csv():
    stats = None
    if request.method == "POST":
        file = request.files.get("csvfile")
        if not file:
            flash("Aucun fichier sélectionné", "error")
            return render_template("upload.html", stats=None)

        try:
            content = file.read().decode("utf-8-sig")
        except UnicodeDecodeError:
            content = file.read().decode("latin-1")

        stats = process_skool_csv(content)
        if stats.get("error"):
            flash(stats["error"], "error")
        else:
            # Save upload history snapshot
            save_upload_snapshot(stats)
            msg = f"{stats['imported']} membres importés ({stats['new']} nouveaux, {stats['updated']} mis à jour)"
            if stats.get('churned', 0) > 0:
                msg += f", {stats['churned']} churned détectés"
            if stats.get('reactivated', 0) > 0:
                msg += f", {stats['reactivated']} réactivés"
            flash(msg, "success")

    return render_template("upload.html", stats=stats)


def process_skool_csv(content):
    """Parse and import Skool CSV with churn detection."""
    delimiter = ";" if ";" in content.split("\n")[0] else ","
    reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
    rows = list(reader)

    if not rows:
        return {"error": "Fichier vide"}

    db = get_db()
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    batch = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
    imported = 0
    new_count = 0
    updated = 0
    reactivated = 0

    # Remove old placeholder entries (members without email from previous uploads)
    db.execute("DELETE FROM members WHERE email LIKE '__no_email_%'")

    # Collect all real emails in this upload
    csv_emails = set()

    no_email_idx = 0
    for row in rows:
        email = row.get("Email", "").strip()
        is_placeholder = False
        if not email:
            no_email_idx += 1
            email = f"__no_email_{no_email_idx}_{batch}__"
            is_placeholder = True
        else:
            csv_emails.add(email)

        first_name = row.get("FirstName", "").strip()
        last_name = row.get("LastName", "").strip()
        invited_by = row.get("Invited By", "").strip()
        joined_at = row.get("JoinedDate", "").strip()
        price_str = row.get("Price", "0").replace("$", "").replace(",", "").strip()
        price = float(price_str) if price_str else 0
        interval = row.get("Recurring Interval", "").strip()
        tier = row.get("Tier", "").strip()
        ltv_str = row.get("LTV", "0").replace("$", "").replace(",", "").strip()
        ltv = float(ltv_str) if ltv_str else 0

        existing = db.execute("SELECT id, status FROM members WHERE email = ?", (email,)).fetchone()

        if existing:
            was_churned = existing["status"] == "churned"
            db.execute("""
                UPDATE members SET first_name=?, last_name=?, invited_by=?,
                price=?, recurring_interval=?, tier=?, ltv=?, upload_batch=?,
                status='active', churned_at='', last_seen_at=?
                WHERE email=?
            """, (first_name, last_name, invited_by, price, interval, tier, ltv, batch, now, email))
            updated += 1
            if was_churned:
                reactivated += 1
        else:
            db.execute("""
                INSERT INTO members (first_name, last_name, email, invited_by, joined_at,
                    price, recurring_interval, tier, ltv, status, first_seen_at, last_seen_at, upload_batch)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)
            """, (first_name, last_name, email, invited_by, joined_at, price, interval, tier, ltv, now, now, batch))
            new_count += 1

        imported += 1

    # CHURN DETECTION: members with real emails who are in DB as active
    # but NOT in this CSV upload = churned
    churned = 0
    if csv_emails:  # only detect churn if we have real emails
        active_in_db = db.execute(
            "SELECT email FROM members WHERE status = 'active' AND email NOT LIKE '__no_email_%'"
        ).fetchall()
        for row in active_in_db:
            if row["email"] not in csv_emails:
                db.execute(
                    "UPDATE members SET status='churned', churned_at=?, price=0 WHERE email=?",
                    (now, row["email"])
                )
                churned += 1

    db.commit()
    return {
        "imported": imported, "new": new_count, "updated": updated,
        "churned": churned, "reactivated": reactivated, "batch": batch
    }


def save_upload_snapshot(stats):
    """Save a snapshot of current state after import."""
    db = get_db()
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

    total = db.execute("SELECT COUNT(*) as c FROM members WHERE email NOT LIKE '__no_email_%'").fetchone()["c"]
    active = db.execute("SELECT COUNT(*) as c FROM members WHERE status = 'active' AND email NOT LIKE '__no_email_%'").fetchone()["c"]
    paid = db.execute("SELECT COUNT(*) as c FROM members WHERE status = 'active' AND price > 0 AND ltv > 0").fetchone()["c"]
    free = active - paid

    mrr = db.execute("SELECT SUM(price) as s FROM members WHERE price > 0 AND ltv > 0 AND recurring_interval = 'month' AND status = 'active'").fetchone()["s"] or 0
    annual = db.execute("SELECT SUM(price) as s FROM members WHERE price > 0 AND ltv > 0 AND recurring_interval = 'year' AND status = 'active'").fetchone()["s"] or 0
    mrr += annual / 12

    total_ltv = db.execute("SELECT SUM(ltv) as s FROM members").fetchone()["s"] or 0
    avg_ltv = db.execute("SELECT AVG(ltv) as a FROM members WHERE ltv > 0").fetchone()["a"] or 0

    db.execute("""
        INSERT INTO upload_history (batch, uploaded_at, total_members, active_members,
            new_members, updated_members, churned_members, reactivated_members,
            paid_members, free_members, mrr, total_ltv, avg_ltv)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (stats["batch"], now, total, active, stats["new"], stats["updated"],
          stats.get("churned", 0), stats.get("reactivated", 0),
          paid, free, round(mrr, 2), round(total_ltv, 2), round(avg_ltv, 2)))
    db.commit()


# ==================== UPLOAD HISTORY ====================

@app.route("/history")
@login_required
def history_page():
    return render_template("history.html")


@app.route("/api/history")
@login_required
def api_history():
    db = get_db()
    rows = db.execute("SELECT * FROM upload_history ORDER BY uploaded_at DESC").fetchall()

    history = []
    for i, r in enumerate(rows):
        entry = {
            "id": r["id"], "batch": r["batch"], "uploaded_at": r["uploaded_at"],
            "total_members": r["total_members"], "active_members": r["active_members"],
            "new_members": r["new_members"], "updated_members": r["updated_members"],
            "churned_members": r["churned_members"], "reactivated_members": r["reactivated_members"],
            "paid_members": r["paid_members"], "free_members": r["free_members"],
            "mrr": r["mrr"], "total_ltv": r["total_ltv"], "avg_ltv": r["avg_ltv"]
        }
        # Compute deltas vs previous import
        if i < len(rows) - 1:
            prev = rows[i + 1]
            entry["delta_members"] = r["active_members"] - prev["active_members"]
            entry["delta_mrr"] = round(r["mrr"] - prev["mrr"], 2)
            entry["delta_ltv"] = round(r["total_ltv"] - prev["total_ltv"], 2)
            entry["delta_paid"] = r["paid_members"] - prev["paid_members"]
        else:
            entry["delta_members"] = 0
            entry["delta_mrr"] = 0
            entry["delta_ltv"] = 0
            entry["delta_paid"] = 0
        history.append(entry)

    return jsonify(history)


# ==================== LINK TRACKER ====================

@app.route("/go/<channel>")
def track_click(channel):
    channel = channel.lower().strip()
    ip_raw = request.remote_addr or "unknown"
    ip_hash = hashlib.sha256(ip_raw.encode()).hexdigest()[:16]
    user_agent = request.headers.get("User-Agent", "")[:500]
    referer = request.headers.get("Referer", "")[:500]
    now = datetime.now(TZ)

    db = get_db()
    db.execute(
        "INSERT INTO clicks (channel, clicked_at, ip_hash, user_agent, referer) VALUES (?, ?, ?, ?, ?)",
        (channel, now.strftime("%Y-%m-%d %H:%M:%S"), ip_hash, user_agent, referer)
    )
    db.commit()

    # Check for custom tracking link
    link = db.execute("SELECT destination_url, utm_source, utm_campaign FROM tracking_links WHERE channel = ?", (channel,)).fetchone()

    if link:
        dest = link["destination_url"]
        params = {}
        if link["utm_source"]:
            params["utm_source"] = link["utm_source"]
        if link["utm_campaign"]:
            params["utm_campaign"] = link["utm_campaign"]
        # Also pick up any UTM params from the URL itself
        for k, v in request.args.items():
            if k.startswith("utm_"):
                params[k] = v
        if params:
            sep = "&" if "?" in dest else "?"
            dest += sep + "&".join(f"{k}={v}" for k, v in params.items())
    else:
        dest = SKOOL_URL
        utm_params = {k: v for k, v in request.args.items() if k.startswith("utm_")}
        if utm_params:
            sep = "&" if "?" in dest else "?"
            dest += sep + "&".join(f"{k}={v}" for k, v in utm_params.items())

    return redirect(dest, code=302)


@app.route("/links", methods=["GET", "POST"])
@login_required
def links_page():
    db = get_db()

    if request.method == "POST":
        channel = request.form.get("channel_name", "").strip().lower()
        channel = "".join(c for c in channel if c.isalnum() or c in "-_")
        dest_url = request.form.get("destination_url", "").strip()
        utm_source = request.form.get("utm_source", "").strip()
        utm_campaign = request.form.get("utm_campaign", "").strip()

        if not channel:
            flash("Nom de canal requis", "error")
        elif not dest_url:
            flash("URL de destination requise", "error")
        else:
            if not dest_url.startswith("http"):
                dest_url = "https://" + dest_url
            try:
                db.execute(
                    "INSERT INTO tracking_links (channel, destination_url, utm_source, utm_campaign, created_at) VALUES (?, ?, ?, ?, ?)",
                    (channel, dest_url, utm_source, utm_campaign, datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"))
                )
                # Also add to custom_channels if not exists
                try:
                    db.execute("INSERT INTO custom_channels (name, created_at) VALUES (?, ?)",
                        (channel, datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")))
                except Exception:
                    pass
                db.commit()
                flash(f"Lien « {channel} » créé !", "success")
            except Exception:
                flash(f"Le canal « {channel} » existe déjà.", "error")

    base_url = request.host_url.rstrip("/")
    links = db.execute("SELECT * FROM tracking_links ORDER BY created_at DESC").fetchall()

    # Get click counts per channel
    click_counts = {}
    for row in db.execute("SELECT channel, COUNT(*) as cnt FROM clicks GROUP BY channel").fetchall():
        click_counts[row["channel"]] = row["cnt"]

    links_data = [{
        "id": l["id"], "channel": l["channel"], "destination_url": l["destination_url"],
        "utm_source": l["utm_source"], "utm_campaign": l["utm_campaign"],
        "url": f"{base_url}/go/{l['channel']}", "clicks": click_counts.get(l["channel"], 0),
        "created_at": l["created_at"]
    } for l in links]

    return render_template("links.html", links=links_data)


@app.route("/api/links/<int:link_id>/delete", methods=["POST"])
@login_required
def delete_link(link_id):
    db = get_db()
    link = db.execute("SELECT channel FROM tracking_links WHERE id = ?", (link_id,)).fetchone()
    if link:
        db.execute("DELETE FROM clicks WHERE channel = ?", (link["channel"],))
        db.execute("DELETE FROM tracking_links WHERE id = ?", (link_id,))
        db.execute("DELETE FROM custom_channels WHERE name = ?", (link["channel"],))
        db.commit()
        return jsonify({"ok": True})
    return jsonify({"error": "Lien introuvable"}), 404


@app.route("/channels")
@login_required
def channels_page():
    return render_template("channels.html")


@app.route("/api/clicks")
@login_required
def api_clicks():
    db = get_db()
    days = int(request.args.get("days", 30))
    since = (datetime.now(TZ) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

    by_channel = db.execute(
        "SELECT channel, COUNT(*) as cnt FROM clicks WHERE clicked_at >= ? GROUP BY channel ORDER BY cnt DESC",
        (since,)
    ).fetchall()

    daily = db.execute(
        "SELECT DATE(clicked_at) as day, channel, COUNT(*) as cnt FROM clicks WHERE clicked_at >= ? GROUP BY day, channel ORDER BY day",
        (since,)
    ).fetchall()

    daily_map = {}
    for r in daily:
        if r["day"] not in daily_map:
            daily_map[r["day"]] = {}
        daily_map[r["day"]][r["channel"]] = r["cnt"]

    total = sum(r["cnt"] for r in by_channel)

    return jsonify({
        "total": total,
        "by_channel": {r["channel"]: r["cnt"] for r in by_channel},
        "daily_by_channel": daily_map
    })


@app.route("/api/export")
@login_required
def export_clicks():
    db = get_db()
    rows = db.execute("SELECT * FROM clicks ORDER BY clicked_at DESC").fetchall()
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow(["ID", "Canal", "Date/Heure", "IP Hash", "User Agent", "Referer"])
    for r in rows:
        writer.writerow([r["id"], r["channel"], r["clicked_at"], r["ip_hash"], r["user_agent"], r["referer"]])
    return Response(output.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=clicks_{datetime.now(TZ).strftime('%Y%m%d')}.csv"})


# ==================== USER MANAGEMENT ====================

@app.route("/settings")
@admin_required
def settings_page():
    return render_template("settings.html")


@app.route("/api/users", methods=["GET"])
@admin_required
def api_users():
    db = get_db()
    users = db.execute("SELECT id, username, role, created_at FROM users ORDER BY id").fetchall()
    return jsonify([{"id": r["id"], "username": r["username"], "role": r["role"], "created_at": r["created_at"]} for r in users])


@app.route("/api/users/create", methods=["POST"])
@admin_required
def create_user():
    data = request.get_json()
    username = data.get("username", "").strip()
    password = data.get("password", "")
    role = data.get("role", "viewer")

    if not username or not password:
        return jsonify({"error": "Nom d'utilisateur et mot de passe requis"}), 400
    if role not in ("admin", "viewer"):
        role = "viewer"

    db = get_db()
    try:
        db.execute(
            "INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
            (username, generate_password_hash(password), role, datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"))
        )
        db.commit()
        return jsonify({"success": True, "message": f"Utilisateur {username} créé"})
    except Exception:
        return jsonify({"error": f"L'utilisateur {username} existe déjà"}), 400


@app.route("/api/users/<int:user_id>/password", methods=["POST"])
@admin_required
def change_password(user_id):
    data = request.get_json()
    new_password = data.get("password", "")
    if not new_password:
        return jsonify({"error": "Mot de passe requis"}), 400

    db = get_db()
    db.execute("UPDATE users SET password_hash = ? WHERE id = ?", (generate_password_hash(new_password), user_id))
    db.commit()
    return jsonify({"success": True})


@app.route("/api/users/<int:user_id>/delete", methods=["POST"])
@admin_required
def delete_user(user_id):
    if user_id == session.get("user_id"):
        return jsonify({"error": "Vous ne pouvez pas supprimer votre propre compte"}), 400
    db = get_db()
    db.execute("DELETE FROM users WHERE id = ?", (user_id,))
    db.commit()
    return jsonify({"success": True})


@app.route("/change-password", methods=["POST"])
@login_required
def change_own_password():
    old_pw = request.form.get("old_password", "")
    new_pw = request.form.get("new_password", "")
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id = ?", (session["user_id"],)).fetchone()
    if not check_password_hash(user["password_hash"], old_pw):
        flash("Ancien mot de passe incorrect", "error")
    elif len(new_pw) < 4:
        flash("Le nouveau mot de passe doit faire au moins 4 caractères", "error")
    else:
        db.execute("UPDATE users SET password_hash = ? WHERE id = ?", (generate_password_hash(new_pw), session["user_id"]))
        db.commit()
        flash("Mot de passe modifié !", "success")
    return redirect(request.referrer or url_for("dashboard"))


# ==================== MAIN ====================

# Temporary route to reset admin password (remove after first login!)
@app.route("/reset-admin")
def reset_admin():
    db = get_db()
    new_pw = generate_password_hash("admin123")
    db.execute("DELETE FROM users")
    db.execute(
        "INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
        ("admin", new_pw, "admin", datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"))
    )
    db.commit()
    return "Admin reset! Login: admin / admin123  <a href='/login'>→ Login</a>"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"\n{'='*50}")
    print(f"  Skool Tracker demarre !")
    print(f"  http://localhost:{port}/dashboard")
    print(f"  Login: admin / admin")
    print(f"{'='*50}\n")
    app.run(debug=True, host="0.0.0.0", port=port)
