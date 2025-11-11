import marimo

__generated_with = "0.17.7"
app = marimo.App(width="medium")


@app.cell
def _():
    import pyodbc; print(pyodbc.drivers())
    return


@app.cell
def _():
    import urllib
    from sqlalchemy import create_engine
    from sqlalchemy import select

    params = urllib.parse.quote_plus(
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=tcp:badm-test.database.windows.net,1433;"
        "Database=BADM_TEST_1;"
        "Uid=badm_test;"
        "Pwd=Saad@1993;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", pool_pre_ping=True)
    return create_engine, engine, select


@app.cell
def _(engine):
    def azure_connect():
        from sqlalchemy import text

        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
            print("✅ Connected to Azure SQL Database successfully!")
        except Exception as e:
            print("❌ Connection failed")
            print("Error:", e)


    azure_connect()
    return


@app.cell
def _(engine):
    def azure_query():
        from sqlalchemy import text
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT TOP 1 * FROM Business"))
                print("✅ Query executed. Marimo is connected.")
                print(result.fetchone())
        except Exception as e:
            print("❌ Unable to query Business table")
            print("Error:", e)
    azure_query()
    return


@app.cell
def _(mo):

    top_n = mo.ui.slider(5, 50, 5, value=10, label="Top-N Businesses")
    top_n   # returning the UI element makes it interactive
    return (top_n,)


@app.cell
def _(mo):
    def metrics(count_val, avg_val, top_df, good_df, bad_df):
        mo.ui.tabs({
            "Summary": mo.md(f"**Businesses**: {count_val}<br>**Avg Rating**: {avg_val}"),
            "Top Businesses": mo.ui.table(top_df) if not top_df.empty else mo.md("_No results_"),
            "Good Reviews": mo.ui.table(good_df) if not good_df.empty else mo.md("_No results_"),
            "Bad Reviews": mo.ui.table(bad_df) if not bad_df.empty else mo.md("_No results_"),
        })
    return


@app.cell
def _(Business, desc, engine, pd, select, top_n):

    stmt = (
        select(
            Business.c.name,
            Business.c.address,
            Business.c.stars,
            Business.c.review_count,
        )
        .order_by(desc(Business.c.stars), desc(Business.c.review_count))
        .limit(int(top_n.value))
    )

    with engine.begin() as conn:
        df = pd.read_sql(stmt, conn)

    df   # marimo displays the dataframe as an interactive table
    return


@app.cell
def _(engine):
    def query1():
        from sqlalchemy import MetaData, Table, select, func

        metadata = MetaData()

        # If your table is under dbo schema (typical for SQL Server), set schema="dbo".
        # If not sure, try schema=None first; if it fails, use schema="dbo".
        Business = Table("Business", metadata, autoload_with=engine, schema="dbo")

        # Build the equivalent of:
        # SELECT COUNT(*) FROM Business WHERE city='Philadelphia' AND categories LIKE '%Italian%'
        stmt = (
            select(func.count())
            .select_from(Business)
            .where(
                Business.c.city == "Philadelphia",
                Business.c.categories.ilike("%Italian%")
            )
        )

        with engine.begin() as conn:
            count_val = conn.scalar(stmt)
        return print("Count:", count_val)


    query1()
    return


@app.cell
def _(engine):
    def query2():
        from sqlalchemy import MetaData, Table, select, func

        metadata = MetaData()

        # Reflect the Business table
        Business = Table("Business", metadata, autoload_with=engine, schema="dbo")

        # SQL equivalent:
        # SELECT AVG(STARS) AS AVG_RATING
        # FROM Business
        # WHERE city = 'Philadelphia'
        #   AND categories LIKE '%Italian%'
        stmt = (
            select(func.avg(Business.c.stars).label("AVG_RATING"))
            .select_from(Business)
            .where(
                Business.c.city == "Philadelphia",
                Business.c.categories.ilike("%Italian%")
            )
        )

        with engine.begin() as conn:
            avg_val = conn.scalar(stmt)

        return print("Average Rating:", avg_val)


    # Run it:
    query2()
    return


@app.cell
def _(engine):
    def query3():
        from sqlalchemy import MetaData, Table, select
        import pandas as pd

        metadata = MetaData()

        # Reflect the Business table
        Business = Table("Business", metadata, autoload_with=engine, schema="dbo")

        # SQL equivalent:
        # SELECT TOP 10 name, address, stars, review_count
        # FROM Business
        # WHERE city = 'Philadelphia'
        #   AND categories LIKE '%Italian%'
        # ORDER BY stars DESC, review_count DESC

        stmt = (
            select(
                Business.c.name,
                Business.c.address,
                Business.c.stars,
                Business.c.review_count,
            )
            .where(
                Business.c.city == "Philadelphia",
                Business.c.categories.ilike("%Italian%"),
            )
            .order_by(
                Business.c.stars.desc(),
                Business.c.review_count.desc(),
            )
            .limit(10)   # SQLAlchemy way to do TOP 10
        )

        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn)

        return df


    # Run it
    query3()
    return


@app.cell
def _(engine):
    def query4():
        from sqlalchemy import MetaData, Table, select
        import pandas as pd

        metadata = MetaData()

        # Reflect both tables
        Business = Table("Business", metadata, autoload_with=engine, schema="dbo")
        Review = Table("Review", metadata, autoload_with=engine, schema="dbo")

        # Subquery equivalent of:
        # SELECT business_id
        # FROM Business
        # WHERE city='Philadelphia'
        #   AND categories LIKE '%Italian%'
        business_subq = (
            select(Business.c.business_id)
            .where(
                Business.c.city == "Philadelphia",
                Business.c.categories.ilike("%Italian%")
            )
        )

        # Main query equivalent of:
        # SELECT TOP 100 text
        # FROM Review
        # WHERE business_id IN (subquery)
        #   AND stars >= 4
        stmt = (
            select(Review.c.text)
            .where(
                Review.c.business_id.in_(business_subq),
                Review.c.stars >= 4
            )
            .limit(100)
        )

        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn)

        return df


    # Run it
    query4()
    return


@app.cell
def _(engine):
    def query5():
        from sqlalchemy import MetaData, Table, select
        import pandas as pd

        metadata = MetaData()

        # Reflect tables
        Business = Table("Business", metadata, autoload_with=engine, schema="dbo")
        Review = Table("Review", metadata, autoload_with=engine, schema="dbo")

        # Subquery:
        # SELECT business_id FROM Business
        # WHERE city='Philadelphia' AND categories LIKE '%Italian%'
        business_subq = (
            select(Business.c.business_id)
            .where(
                Business.c.city == "Philadelphia",
                Business.c.categories.ilike("%Italian%")
            )
        )

        # Main query:
        # SELECT TOP 100 text FROM Review
        # WHERE business_id IN (subquery)
        #   AND stars <= 2
        stmt = (
            select(Review.c.text)
            .where(
                Review.c.business_id.in_(business_subq),
                Review.c.stars <= 2
            )
            .limit(100)
        )

        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn)

        return df


    # Run it
    query5()
    return


@app.cell
def _(engine):
    def query6():
        from sqlalchemy import MetaData, Table, select, func, desc
        import pandas as pd

        metadata = MetaData()

        # Reflect the Business table
        Business = Table("Business", metadata, autoload_with=engine, schema="dbo")

        # SQL equivalent:
        # SELECT TOP 5 postal_code,
        #        COUNT(*) AS italian_count,
        #        AVG(stars) AS avg_rating
        # FROM business
        # WHERE city='Philadelphia' AND categories LIKE '%Italian%'
        # GROUP BY postal_code
        # ORDER BY italian_count ASC, avg_rating DESC

        stmt = (
            select(
                Business.c.postal_code,
                func.count().label("italian_count"),
                func.avg(Business.c.stars).label("avg_rating")
            )
            .where(
                Business.c.city == "Philadelphia",
                Business.c.categories.ilike("%Italian%")
            )
            .group_by(Business.c.postal_code)
            .order_by(
                func.count().asc(),     # italian_count ASC
                func.avg(Business.c.stars).desc()  # avg_rating DESC
            )
            .limit(5)  # TOP 5
        )

        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn)

        return df


    # Run it
    query6()
    return


@app.cell
def _():
    # app.py
    # Marimo web app: interactive queries against Azure SQL with SQLAlchemy Core (no raw SQL)
    # Publish with:  marimo run app.py   (or develop with: marimo edit app.py)

    import os
    from urllib.parse import quote_plus

    import marimo as mo
    import pandas as pd
    from sqlalchemy import  MetaData, Table, func, desc, and_, literal_column
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError

    app = mo.App()

    # -----------------------
    # CONFIG: set credentials
    # -----------------------
    # Prefer environment variables for secrets. Example:
    #   setx AZURE_SQL_USER "badm_test"
    #   setx AZURE_SQL_PWD "your_password"
    #   setx AZURE_SQL_SERVER "badm-test.database.windows.net"
    #   setx AZURE_SQL_DB "BADM_TEST_1"
    # (Linux/macOS: export ...)

    AZURE_SQL_USER = os.getenv("AZURE_SQL_USER", "badm_test")
    AZURE_SQL_PWD = os.getenv("AZURE_SQL_PWD", "CHANGE_ME")
    AZURE_SQL_SERVER = os.getenv("AZURE_SQL_SERVER", "badm-test.database.windows.net")
    AZURE_SQL_DB = os.getenv("AZURE_SQL_DB", "BADM_TEST_1")
    ODBC_DRIVER = os.getenv("AZURE_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

    # Build a pyodbc connection URL safely (no raw SQL later; this is just the engine config).
    # Equivalent to the ODBC you shared:
    #   Driver={ODBC Driver 18 for SQL Server};
    #   Server=tcp:badm-test.database.windows.net,1433;
    #   Database=BADM_TEST_1;
    #   Uid=badm_test;
    #   Pwd=...;
    #   Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;
    odbc_str = (
        "DRIVER={%s};"
        "SERVER=tcp:%s,1433;"
        "DATABASE=%s;"
        "UID=%s;"
        "PWD=%s;"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    ) % (ODBC_DRIVER, AZURE_SQL_SERVER, AZURE_SQL_DB, AZURE_SQL_USER, AZURE_SQL_PWD)

    SQLALCHEMY_URL = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"


    @app.cell
    def _():
        return SQLALCHEMY_URL
    return Engine, MetaData, Table, app, desc, mo, pd


@app.cell
def _(Engine, create_engine):
    def _(SQLALCHEMY_URL) -> Engine:
        engine = create_engine(SQLALCHEMY_URL, pool_pre_ping=True, fast_executemany=True)
        return engine
    return


@app.cell
def _(MetaData, Table, engine):
    # Reflect tables without ORM mapping (avoids primary key mapper errors).
    # If your schema is dbo (typical), pass schema="dbo". Otherwise set to None or correct schema.
    metadata = MetaData()
    try:
        Business = Table("Business", metadata, autoload_with=engine, schema="dbo")
    except Exception:
        # fallback when schema isn't dbo
        Business = Table("Business", metadata, autoload_with=engine)

    try:
        Review = Table("review", metadata, autoload_with=engine, schema="dbo")
    except Exception:
        Review = Table("review", metadata, autoload_with=engine)
    return (Business,)


@app.cell
def _():
    def slider():
        import marimo as mo

        # ------------- UI controls -------------
        city = mo.ui.text(value="Philadelphia", label="City")
        category_substr = mo.ui.text(value="Italian", label="Category contains")
        min_stars = mo.ui.slider(0, 5, 1, value=4, label="Minimum stars (for 'good' reviews)")
        max_stars = mo.ui.slider(0, 5, 1, value=2, label="Maximum stars (for 'bad' reviews)")
        limit_top = mo.ui.slider(5, 200, 5, value=10, label="Top-N businesses limit")
        run_btn = mo.ui.button(label="Run Queries")

        controls = mo.vstack([
            mo.hstack([city, category_substr, limit_top]),
            mo.hstack([min_stars, max_stars, run_btn])
        ])

        # Return controls so the cell renders them
        return


    slider()
    return


@app.function
def dynamic_query():
    from typing import Tuple
    from sqlalchemy import select, func, desc

    def like_filter(col, needle: str):
    # case-insensitive contains
        return col.ilike(f"%{needle}%") if needle else col.ilike("%")


    def stmt_count_businesses(Business, city: str, cat_substr: str):
        return (
            select(func.count())
            .select_from(Business)
            .where(
                Business.c.city == city,
                like_filter(Business.c.categories, cat_substr),
            )
        )

    def stmt_avg_stars(Business, city: str, cat_substr: str):
        return (
            select(func.avg(Business.c.stars).label("AVG_RATING"))
            .where(
                Business.c.city == city,
                like_filter(Business.c.categories, cat_substr),
            )
        )

    def stmt_top_businesses(Business, city: str, cat_substr: str, limit_n: int):
    # ORDER BY stars DESC, review_count DESC LIMIT N (TOP N equivalent)
        return (
            select(
                Business.c.name,
                Business.c.address,
                Business.c.stars,
                Business.c.review_count,
            )
            .where(
                Business.c.city == city,
                like_filter(Business.c.categories, cat_substr),
            )
            .order_by(desc(Business.c.stars), desc(Business.c.review_count))
            .limit(limit_n)
        )

    def stmt_good_reviews(Business, Review, city: str, cat_substr: str, min_stars: int):
    # SELECT review.text WHERE review.stars >= min_stars AND review.business_id IN (...)
        subq = (
            select(Business.c.business_id)
        .   where(
            Business.c.city == city,
            like_filter(Business.c.categories, cat_substr),
        )
        .subquery()
    )
        return (
            select(Review.c.text)
            .where(
                Review.c.business_id.in_(select(subq.c.business_id)),
                Review.c.stars >= min_stars,
            )
            .limit(100)
        )

    def stmt_bad_reviews(Business, Review, city: str, cat_substr: str, max_stars: int):
        subq = (
            select(Business.c.business_id)
            .where(
            Business.c.city == city,
            like_filter(Business.c.categories, cat_substr),
        )
        .subquery()
    )
        return (
            select(Review.c.text)
            .where(
                Review.c.business_id.in_(select(subq.c.business_id)),
                Review.c.stars <= max_stars,
            )
            .limit(100)
        )

    return (
        stmt_count_businesses,
        stmt_avg_stars,
        stmt_top_businesses,
        stmt_good_reviews,
        stmt_bad_reviews,
    )


@app.cell
def _(pd):
    def query(
        engine,
        Business,
        Review,
        city,
        category_substr,
        min_stars,
        max_stars,
        limit_top,
        run_btn,
        stmt_count_businesses,
        stmt_avg_stars,
        stmt_top_businesses,
        stmt_good_reviews,
        stmt_bad_reviews,
    ):
    # Use button clicks as a recompute trigger
        _ = run_btn.value

        city_val = city.value.strip()
        cat_val = category_substr.value.strip()
        min_s = int(min_stars.value)
        max_s = int(max_stars.value)
        top_n = int(limit_top.value)

        with engine.begin() as conn:
            # 1) Count businesses
            count_val = conn.scalar(
                stmt_count_businesses(Business, city_val, cat_val)
            )

            # 2) Average stars for those businesses
            avg_row = conn.execute(
                stmt_avg_stars(Business, city_val, cat_val)
            ).one_or_none()
            avg_rating = float(avg_row[0]) if avg_row and avg_row[0] is not None else None

            # 3) Top-N businesses table
            top_df = pd.DataFrame(
                conn.execute(
                    stmt_top_businesses(Business, city_val, cat_val, top_n)
                ).mappings().all()
            )

            # 4) Good reviews (stars >= min_s)
            good_reviews_df = pd.DataFrame(
                conn.execute(
                    stmt_good_reviews(Business, Review, city_val, cat_val, min_s)
                ).mappings().all()
            )

            # 5) Bad reviews (stars <= max_s)
            bad_reviews_df = pd.DataFrame(
                conn.execute(
                    stmt_bad_reviews(Business, Review, city_val, cat_val, max_s)
                ).mappings().all()
            )

        summary = {
            "city": city_val,
            "category_contains": cat_val,
            "count_businesses": count_val,
            "avg_rating": avg_rating,
            "top_n": top_n,
            "min_stars_good": min_s,
            "max_stars_bad": max_s,
        }

        # Display: summary dict + dataframes (marimo renders them nicely)
        summary, top_df, good_reviews_df.head(10), bad_reviews_df.head(10)
    return


@app.cell
def _(app):
    @app.cell
    def _(
        engine,
        Business,
        Review,
        city,
        category_substr,
        min_stars,
        max_stars,
        limit_top,
    ):
        import pandas as pd
        from sqlalchemy import select, func, desc

        # --- helper for like ---
        def like_filter(col, val):

        # --- ALWAYS define outputs first (prevents NameError) ---
            top_df = pd.DataFrame()
            good_df = pd.DataFrame()
            bad_df = pd.DataFrame()
            count_val = None
            avg_val = None

        try:
            with engine.begin() as conn:

            # 1) COUNT
                stmt = (
                    select(func.count())
                    .select_from(Business)
                    .where(
                        Business.c.city == city.value,
                        like_filter(Business.c.categories, category_substr.value),
                    )
                )
                count_val = conn.scalar(stmt)

                # 2) AVG STARS
                stmt = (
                    select(func.avg(Business.c.stars))
                    .select_from(Business)
                    .where(
                        Business.c.city == city.value,
                        like_filter(Business.c.categories, category_substr.value),
                    )
                )
                avg_val = conn.scalar(stmt)

                # 3) TOP BUSINESSES
                stmt = (
                    select(
                        Business.c.name,
                        Business.c.address,
                        Business.c.stars,
                        Business.c.review_count,
                    )
                    .where(
                        Business.c.city == city.value,
                        like_filter(Business.c.categories, category_substr.value),
                    )
                    .order_by(
                        Business.c.stars.desc(),
                        Business.c.review_count.desc(),
                    )
                    .limit(int(limit_top.value))
                )
                top_df = pd.DataFrame(conn.execute(stmt).mappings().all())

                # 4) GOOD REVIEWS
                subq = (
                    select(Business.c.business_id)
                    .where(
                        Business.c.city == city.value,
                        like_filter(Business.c.categories, category_substr.value),
                    )
                    .subquery()
                )
                stmt = (
                    select(Review.c.text, Review.c.stars)
                    .where(
                        Review.c.business_id.in_(select(subq.c.business_id)),
                        Review.c.stars >= int(min_stars.value),
                    )
                    .limit(100)
                )
                good_df = pd.DataFrame(conn.execute(stmt).mappings().all())

                # 5) BAD REVIEWS
                stmt = (
                    select(Review.c.text, Review.c.stars)
                    .where(
                        Review.c.business_id.in_(select(subq.c.business_id)),
                        Review.c.stars <= int(max_stars.value),
                    )
                    .limit(100)
                )
                bad_df = pd.DataFrame(conn.execute(stmt).mappings().all())

        except Exception as e:
        # Optional: print error but keep the variables defined
            print("Query error:", e)

    # ✅ These variables ALWAYS exist now
    return


if __name__ == "__main__":
    app.run()
