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
threads = 2

# Timeout mais alto para importação de dados
timeout = 300

# Memória: limitar e reiniciar worker se usar muita memória
max_requests = 100
max_requests_jitter = 20

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"

# Preload app (economiza memória compartilhando código)
preload_app = True
