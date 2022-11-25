import psycopg2 as psycopg2
from flask import Flask, render_template, request

app = Flask(__name__)


def run_query(query, params):
    connection = psycopg2.connect(user="pulp", password="pulp", host="localhost", port="5432", database="pulp")
    cursor = connection.cursor()
    cursor.execute(query, params)
    return cursor


@app.route('/content')
def content():
    search = request.args.get("search") or ""
    typ = request.args.get("type") or "all"
    page = request.args.get("page") or 0
    page = int(page)

    records = []
    next_page = True

    def get_data():
        params = {'search': f"%{'%'.join(search.split(' '))}%", 'typ': typ, 'typs': f"%{typ}%", "page100": page * 100}

        query = "select name, version, 'RPM' typ, content_ptr_id from rpm_package WHERE " \
                "(concat_ws('', name::text, version::text) LIKE %(search)s) AND ('rpm' LIKE %(typs)s OR %(typ)s = 'all') " \
                "LIMIT 100 OFFSET %(page100)s"
        cursor = run_query(query, params)
        records.extend(cursor.fetchall())

        query = "select package, version, 'DEB' typ, content_ptr_id from deb_package WHERE " \
                "(concat_ws('', package::text, version::text) LIKE %(search)s) AND ('deb' LIKE %(typs)s OR %(typ)s = 'all')" \
                " LIMIT 100 OFFSET %(page100)s"
        cursor = run_query(query, params)
        records.extend(cursor.fetchall())

    get_data()
    while len(records) == 0 and page > 0:
        page -= 1
        get_data()
        next_page = False

    records = [{"name": r[0], "version": r[1], "type": r[2], "id": r[3]} for r in records]
    return render_template('content.html', packages=records, search=search, type=typ, type_choices=["all", "rpm", "deb"], page=page, nextPage=next_page)


@app.route('/repos')
def repos():
    search = request.args.get("search") or ""
    typ = request.args.get("type") or "all"

    query = "select name, pulp_type from core_repository WHERE " \
            "name LIKE %(search)s AND (%(typs)s LIKE pulp_type OR %(typ)s = 'all')"
    params = {'search': f"%{search}%", 'typ': typ, 'typs': f"%{typ}%"}
    cursor = run_query(query, params)

    records = cursor.fetchall()
    records = [{"name": r[0], "type": "DEB" if "deb" in r[1] else "RPM"} for r in records]
    return render_template('repos.html', repos=records, search=search, type=typ, type_choices=["all", "rpm", "deb"])


@app.route('/remotes')
def remotes():
    search = request.args.get("search") or ""
    typ = request.args.get("type") or "all"
    query = f"select name, policy, url, pulp_type from core_remote where " \
            f"(name like %(search)s OR url like %(search)s) AND (pulp_type like %(typs)s OR %(typ)s = 'all')"

    connection = psycopg2.connect(user="pulp", password="pulp", host="localhost", port="5432", database="pulp")
    cursor = connection.cursor()
    cursor.execute(query, {'search': f"%{search}%", 'typ': typ, 'typs': f"%{typ}%"})
    records = cursor.fetchall()
    records = [{"name": r[0], "policy": r[1], "url": r[2], "type": "DEB" if "deb" in r[3] else "RPM"} for r in records]
    return render_template('remotes.html', remotes=records, search=search, type=typ, type_choices=["all", "rpm", "deb"])


@app.route('/distributions')
def distributions():
    search = request.args.get("search") or ""
    typ = request.args.get("type") or "all"
    query = f"select name, pulp_type from core_distribution where " \
            f"(name like %(search)s) AND (pulp_type like %(typs)s OR %(typ)s = 'all')"

    connection = psycopg2.connect(user="pulp", password="pulp", host="localhost", port="5432", database="pulp")
    cursor = connection.cursor()
    cursor.execute(query, {'search': f"%{search}%", 'typ': typ, 'typs': f"%{typ}%"})
    records = cursor.fetchall()
    records = [{"name": r[0], "type": "DEB" if "deb" in r[1] else "RPM"} for r in records]
    return render_template('distributions.html', distributions=records, search=search, type=typ, type_choices=["all", "rpm", "deb"])


@app.route("/")
def index():
    return render_template("index.html")


if __name__ == "__main__":
    app.run("0.0.0.0", 8080, True)
