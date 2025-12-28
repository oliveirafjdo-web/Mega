# Copilot Instructions for MetriFy ERP

## Project Overview
- **MetriFy ERP** is a Flask-based ERP system for sales, inventory, and financial management, supporting both SQLite (local) and PostgreSQL (Render cloud) backends.
- Main entrypoint: `app.py` (Flask app, DB config, routes, migrations, user/session management).
- Data models and key queries: `models.py` (SQLAlchemy, business logic for sales, products, configs).
- Templates: `templates/` (Jinja2 HTML views for all UI screens).
- Static assets: `static/` (JS, CSS for UI).

## Key Workflows
- **Local run:** `python app.py` (development, SQLite)
- **Production run:** `gunicorn -c gunicorn_config.py app:app` (see `Procfile`/`Dockerfile`)
- **Backup/migration:**
  - Export: Use Admin UI or `python backup_banco.py`
  - Import: `python import_render_backup.py <zipfile>` (for Render/Postgres)
- **Database auto-migration:** On startup, `app.py` runs `migrate_ml_columns()` to add/patch columns as needed.

## Deployment
- **Render.com** auto-deploys on `git push` to main branch (see `COMO_ATUALIZAR.md`).
- No persistent uploads: all files in `uploads/` are ephemeral on Render Free.
- Environment variables: `DATABASE_URL`, `UPLOAD_FOLDER`, `SECRET_KEY` (see `app.py`).

## Conventions & Patterns
- **Database access:** Use SQLAlchemy Core (not ORM) for all queries and migrations.
- **Business logic:** Centralized in `models.py` and route handlers in `app.py`.
- **User auth:** Flask-Login, user table defined in `app.py`.
- **Error handling:** Use helper `db_retry()` for transient DB/SSL errors.
- **Data import/export:** Use pandas for CSV/Excel, see `import_data.py`, `export_data.py`.
- **Testing:** No formal test suite; test by running scripts or using the UI.

## Examples
- To add a new sales report: create a route in `app.py`, query with SQLAlchemy, render with a new template in `templates/`.
- To add a DB field: update `migrate_ml_columns()` in `app.py` for auto-migration.

## References
- See `README.md` for backup/migration details.
- See `COMO_ATUALIZAR.md` for deployment workflow.
- See `gunicorn_config.py` for production server tuning.

---
**When in doubt, check `app.py` for the latest conventions and patterns.**
