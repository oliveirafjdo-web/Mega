# Configuração do Gunicorn otimizada para Render Free (512MB RAM)
import multiprocessing
import os

# Bind
bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"

# Workers: apenas 1 worker para economizar memória
workers = 1

# Tipo de worker: sync (usa menos memória que async)
worker_class = "sync"

# Threads por worker
threads = 1

# Timeout MUITO alto para sincronização lenta da API (até 30 minutos)
timeout = 1800

# Keepalive: evita fechar conexões rapidamente
keepalive = 5

# Disable worker timeout checks (use own timeout)
max_requests = 1000
max_requests_jitter = 50

# Graceful timeout
graceful_timeout = 30
max_requests = 100
max_requests_jitter = 20

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"

# Preload app (economiza memória compartilhando código)
preload_app = True
