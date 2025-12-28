
# ============================================================================
# NOVAS ROTAS - ADICIONAR AO FINAL DO app.py (ANTES DE app.run())
# ============================================================================


import flask
from flask import render_template, request, redirect, url_for, flash, send_file
from flask_login import login_required, current_user
from sqlalchemy import select, insert
import os
from app import app, engine, usuarios, bcrypt


# ---- GERENCIAR USUÁRIOS ----
@app.route("/admin/usuarios")
@login_required
def admin_usuarios():
    """Gerenciar usuários do sistema"""
    user = current_user
    with engine.connect() as conn:
        usuario_atual = conn.execute(
            select(usuarios).where(usuarios.c.id == user.id)
        ).mappings().first()
        
        if not usuario_atual or usuario_atual.get("papel") != "admin":
            flash("❌ Acesso negado.", "danger")
            return redirect(url_for("dashboard"))
        
        # Listar todos os usuários
        todos_usuarios = conn.execute(
            select(usuarios.c.id, usuarios.c.username, usuarios.c.papel, usuarios.c.ativo)
        ).fetchall()
    
    return render_template("gerenciar_usuarios.html", usuarios=todos_usuarios)


# Backup and restore routes are handled by `app.admin_backup`.
# This file previously defined the backup endpoints; to avoid circular imports
# we keep the implementation in `app.py`. If you need to modify backup behavior,
# update the handler in `app.py` or convert these routes to a Blueprint.


# ============================================================================
# FIM DAS NOVAS ROTAS
# ============================================================================
